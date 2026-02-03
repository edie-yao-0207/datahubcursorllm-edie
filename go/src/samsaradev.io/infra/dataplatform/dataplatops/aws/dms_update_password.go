package aws

import (
	"context"
	"strconv"
	"strings"
	"time"

	awsV1 "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/databasemigrationservice"
	"github.com/aws/aws-sdk-go/service/databasemigrationservice/databasemigrationserviceiface"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/dataplatform/rdsdeltalake"
	"samsaradev.io/infra/samsaraaws/awshelpers/awssessions"
)

// =============================================================================
// DMS Update Password
// =============================================================================

type secretReader interface {
	Read(ctx context.Context, key string) (string, error)
}

type shardResolver interface {
	ShardsForDB(ctx context.Context, registryName, region string) ([]string, error)
}

type registryShardResolver struct{}

func (r *registryShardResolver) ShardsForDB(ctx context.Context, registryName, region string) ([]string, error) {
	_ = ctx
	db, err := rdsdeltalake.GetDatabaseByName(registryName)
	if err != nil {
		return nil, err
	}
	shards := db.RegionToShards[region]
	if len(shards) == 0 {
		return nil, oops.Errorf("no shards found for db=%s in region=%s", registryName, region)
	}
	return shards, nil
}

type dmsTaskPlan struct {
	dbShardName            string
	taskType               string
	taskID                 string
	taskArn                string
	status                 string
	replicationInstanceArn string
	sourceEndpointArn      string
	willSkipUpdate         bool
}

type dmsEndpointPlan struct {
	endpointArn        string
	endpointIdentifier string
	endpointType       string
	engineName         string
	serverName         string
	port               int64
	username           string
	databaseName       string
	sslMode            string
}

type dmsConnectionKey struct {
	replicationInstanceArn string
	endpointArn            string
}

type DMSUpdatePasswordResult struct {
	Region           string   `json:"region"`
	Database         string   `json:"database"`
	TaskType         string   `json:"taskType"`
	Shards           []string `json:"shards"`
	TaskIDs          []string `json:"taskIds"`
	SourceEndpoints  []string `json:"sourceEndpointArns"`
	SecretKey        string   `json:"secretKey"`
	ResumeTasks      bool     `json:"resumeTasks"`
	UpdatedEndpoints int      `json:"updatedEndpoints"`
	FailedEndpoints  int      `json:"failedEndpoints"`
	FailedTasks      int      `json:"failedTasks"`
}

// DMSUpdatePasswordOp updates the password on the *source* endpoint(s) used by one or more DMS replication tasks,
// tests connections, and optionally resumes the replication tasks.
//
// This automates the manual runbook workflow after DB password rotation.
type DMSUpdatePasswordOp struct {
	// Inputs
	Region      string
	Database    string
	Shards      string
	TaskType    string
	ResumeTasks bool

	PollInterval time.Duration
	ConnTimeout  time.Duration

	// Clients
	dmsClient     databasemigrationserviceiface.DatabaseMigrationServiceAPI
	secretReader  secretReader
	shardResolver shardResolver

	// Planned state
	registryName       string
	secretKey          string
	selectedShards     []string
	normalizedTaskType string
	taskPlans          []dmsTaskPlan
	endpointPlansByArn map[string]dmsEndpointPlan
	connectionKeys     []dmsConnectionKey
}

func NewDMSUpdatePasswordOp(region, database, shards, taskType string, resumeTasks bool) *DMSUpdatePasswordOp {
	return &DMSUpdatePasswordOp{
		Region:      region,
		Database:    database,
		Shards:      shards,
		TaskType:    taskType,
		ResumeTasks: resumeTasks,

		PollInterval: 10 * time.Second,
		ConnTimeout:  3 * time.Minute,
	}
}

func (o *DMSUpdatePasswordOp) Name() string { return "dms-update-password" }

func (o *DMSUpdatePasswordOp) Description() string {
	return "Update DMS source endpoint password from secretshelper, test connection, and optionally resume task(s)"
}

func (o *DMSUpdatePasswordOp) Validate(ctx context.Context) error {
	if strings.TrimSpace(o.Region) == "" {
		return oops.Errorf("--region is required")
	}
	if strings.TrimSpace(o.Database) == "" {
		return oops.Errorf("--database is required")
	}

	secretsEnv := "prod"
	secretsProfile := defaultSecretsReadProfileForRegion(o.Region)

	if o.PollInterval <= 0 {
		return oops.Errorf("--poll-interval must be > 0")
	}
	if o.ConnTimeout <= 0 {
		return oops.Errorf("--conn-timeout must be > 0")
	}

	o.registryName = normalizeRegistryName(o.Database)
	if o.registryName == "" {
		return oops.Errorf("invalid --database %q", o.Database)
	}
	o.secretKey = deriveSecretKey(o.registryName)

	o.normalizedTaskType = normalizeTaskType(o.TaskType)
	if o.normalizedTaskType == "" {
		return oops.Errorf("--task-type must be one of kinesis, parquet, all (got %q)", o.TaskType)
	}

	// Create clients.
	if o.dmsClient == nil {
		sess := awssessions.NewInstrumentedAWSSessionWithConfigs(&awsV1.Config{
			Region: awsV1.String(o.Region),
		})
		o.dmsClient = databasemigrationservice.New(sess)
	}

	if o.secretReader == nil {
		r, err := newS3SecretsReader(o.Region, secretsEnv, secretsProfile)
		if err != nil {
			return oops.Wrapf(err, "create secrets reader")
		}
		o.secretReader = r
	}

	if o.shardResolver == nil {
		o.shardResolver = &registryShardResolver{}
	}

	return nil
}

func (o *DMSUpdatePasswordOp) Plan(ctx context.Context) error {
	if o.dmsClient == nil || o.secretReader == nil || o.shardResolver == nil {
		return oops.Errorf("Validate() must be called before Plan()")
	}

	// Resolve shards for this DB+region, then derive tasks.
	allShards, err := o.shardResolver.ShardsForDB(ctx, o.registryName, o.Region)
	if err != nil {
		return oops.Wrapf(err, "resolve shards: db=%s region=%s", o.registryName, o.Region)
	}
	selected, err := selectShards(o.Shards, allShards, o.registryName, o.Region)
	if err != nil {
		return err
	}
	o.selectedShards = selected

	taskIDs := deriveCandidateTaskIDs(o.normalizedTaskType, selected)

	// Resolve each task and collect source endpoint + replication instance info.
	var taskPlans []dmsTaskPlan
	endpointArns := make(map[string]struct{})
	connKeys := make(map[dmsConnectionKey]struct{})

	// Only treat as shard-targeted if the selector is a strict subset of the region's shard set.
	// If user explicitly lists all shards (e.g. prod,1,2,3,4,5), behavior should match --shards=all.
	// Always skip password updates if the task is already running/starting (regardless of shard selector).
	for _, candidate := range taskIDs {
		taskID := candidate.taskID
		taskID = strings.TrimSpace(taskID)
		if taskID == "" {
			continue
		}
		out, err := o.dmsClient.DescribeReplicationTasksWithContext(ctx, &databasemigrationservice.DescribeReplicationTasksInput{
			Filters: []*databasemigrationservice.Filter{
				{
					Name:   awsV1.String("replication-task-id"),
					Values: []*string{awsV1.String(taskID)},
				},
			},
			WithoutSettings: awsV1.Bool(true),
		})
		if err != nil {
			return oops.Wrapf(err, "describe replication task: id=%s", taskID)
		}
		if len(out.ReplicationTasks) == 0 {
			continue
		}
		if len(out.ReplicationTasks) != 1 {
			return oops.Errorf("expected 1 replication task for id=%s, got %d", taskID, len(out.ReplicationTasks))
		}

		t := out.ReplicationTasks[0]
		status := awsV1.StringValue(t.Status)
		willSkip := isRunningOrStarting(status)
		plan := dmsTaskPlan{
			dbShardName:            candidate.dbShardName,
			taskType:               candidate.taskType,
			taskID:                 taskID,
			taskArn:                awsV1.StringValue(t.ReplicationTaskArn),
			status:                 status,
			replicationInstanceArn: awsV1.StringValue(t.ReplicationInstanceArn),
			sourceEndpointArn:      awsV1.StringValue(t.SourceEndpointArn),
			willSkipUpdate:         willSkip,
		}
		if plan.taskArn == "" || plan.sourceEndpointArn == "" || plan.replicationInstanceArn == "" {
			return oops.Errorf("task %s missing required fields: taskArn=%q sourceEndpointArn=%q replicationInstanceArn=%q", taskID, plan.taskArn, plan.sourceEndpointArn, plan.replicationInstanceArn)
		}
		taskPlans = append(taskPlans, plan)
		if !willSkip {
			endpointArns[plan.sourceEndpointArn] = struct{}{}
			connKeys[dmsConnectionKey{replicationInstanceArn: plan.replicationInstanceArn, endpointArn: plan.sourceEndpointArn}] = struct{}{}
		}
	}
	if len(taskPlans) == 0 {
		return oops.Errorf("no matching DMS tasks found for db=%s region=%s task_type=%s shards=%s", o.registryName, o.Region, o.normalizedTaskType, o.Shards)
	}

	// Fetch endpoint details for plan output.
	endpointPlans := make(map[string]dmsEndpointPlan, len(endpointArns))
	for endpointArn := range endpointArns {
		out, err := o.dmsClient.DescribeEndpointsWithContext(ctx, &databasemigrationservice.DescribeEndpointsInput{
			Filters: []*databasemigrationservice.Filter{
				{
					Name:   awsV1.String("endpoint-arn"),
					Values: []*string{awsV1.String(endpointArn)},
				},
			},
		})
		if err != nil {
			return oops.Wrapf(err, "describe endpoint: arn=%s", endpointArn)
		}
		if len(out.Endpoints) != 1 {
			return oops.Errorf("expected 1 endpoint for arn=%s, got %d", endpointArn, len(out.Endpoints))
		}
		e := out.Endpoints[0]
		endpointPlans[endpointArn] = dmsEndpointPlan{
			endpointArn:        endpointArn,
			endpointIdentifier: awsV1.StringValue(e.EndpointIdentifier),
			endpointType:       awsV1.StringValue(e.EndpointType),
			engineName:         awsV1.StringValue(e.EngineName),
			serverName:         awsV1.StringValue(e.ServerName),
			port:               awsV1.Int64Value(e.Port),
			username:           awsV1.StringValue(e.Username),
			databaseName:       awsV1.StringValue(e.DatabaseName),
			sslMode:            awsV1.StringValue(e.SslMode),
		}
	}

	var connectionKeys []dmsConnectionKey
	for k := range connKeys {
		connectionKeys = append(connectionKeys, k)
	}

	o.taskPlans = taskPlans
	o.endpointPlansByArn = endpointPlans
	o.connectionKeys = connectionKeys

	// Print a high-signal plan. Never print the secret value.
	slog.Infow(ctx, "📋 Plan: update DMS source endpoint password",
		"region", o.Region,
		"database", o.Database,
		"registryName", o.registryName,
		"taskType", o.normalizedTaskType,
		"shards", o.selectedShards,
		"tasksFound", len(o.taskPlans),
		"tasksToUpdate", len(o.endpointPlansByArn),
		"sourceEndpoints", len(o.endpointPlansByArn),
		"secretKey", o.secretKey,
		"secretsEnv", "prod",
		"secretsProfile", defaultSecretsReadProfileForRegion(o.Region),
		"testConnection", true,
		"resumeTasks", o.ResumeTasks,
		"pollInterval", o.PollInterval.String(),
		"connectionTimeout", o.ConnTimeout.String(),
	)

	for _, tp := range o.taskPlans {
		if tp.willSkipUpdate {
			slog.Infow(ctx, "📌 Task (skipping update: task already running)",
				"replicationTaskId", tp.taskID,
				"dbShard", tp.dbShardName,
				"taskType", tp.taskType,
				"status", tp.status,
				"willSkipUpdate", tp.willSkipUpdate,
				"replicationTaskArn", tp.taskArn,
				"replicationInstanceArn", tp.replicationInstanceArn,
				"sourceEndpointArn", tp.sourceEndpointArn,
			)
			continue
		}

		ep := o.endpointPlansByArn[tp.sourceEndpointArn]
		slog.Infow(ctx, "📌 Task",
			"replicationTaskId", tp.taskID,
			"dbShard", tp.dbShardName,
			"taskType", tp.taskType,
			"status", tp.status,
			"willSkipUpdate", tp.willSkipUpdate,
			"replicationTaskArn", tp.taskArn,
			"replicationInstanceArn", tp.replicationInstanceArn,
			"sourceEndpointArn", tp.sourceEndpointArn,
			"sourceEndpointIdentifier", ep.endpointIdentifier,
			"sourceEngine", ep.engineName,
			"sourceServer", ep.serverName,
			"sourcePort", ep.port,
			"sourceUser", ep.username,
			"sourceDatabase", ep.databaseName,
			"sourceSSLMode", ep.sslMode,
		)
	}

	return nil
}

type candidateTaskID struct {
	taskID      string
	taskType    string
	dbShardName string
}

func deriveCandidateTaskIDs(taskType string, shards []string) []candidateTaskID {
	var out []candidateTaskID
	for _, shard := range shards {
		switch taskType {
		case "kinesis":
			out = append(out, candidateTaskID{taskID: "kinesis-task-cdc-" + shard, taskType: "kinesis", dbShardName: shard})
		case "parquet":
			out = append(out, candidateTaskID{taskID: "parquet-task-cdc-" + shard, taskType: "parquet", dbShardName: shard})
		case "all":
			out = append(out,
				candidateTaskID{taskID: "kinesis-task-cdc-" + shard, taskType: "kinesis", dbShardName: shard},
				candidateTaskID{taskID: "parquet-task-cdc-" + shard, taskType: "parquet", dbShardName: shard},
			)
		}
	}
	return out
}

func isAllShardsSelector(s string) bool {
	ss := strings.ToLower(strings.TrimSpace(s))
	return ss == "" || ss == "all"
}

func selectShards(selector string, allShards []string, db string, region string) ([]string, error) {
	if isAllShardsSelector(selector) {
		return allShards, nil
	}
	toks := strings.Split(selector, ",")
	var selected []string
	seen := make(map[string]struct{})
	for _, tok := range toks {
		tok = strings.ToLower(strings.TrimSpace(tok))
		if tok == "" {
			continue
		}
		switch tok {
		case "prod":
			prodShard := ""
			for _, shard := range allShards {
				if isProdShard(shard) {
					prodShard = shard
					break
				}
			}
			if prodShard == "" {
				return nil, oops.Errorf("no such shard prod for db=%s in region=%s", db, region)
			}
			if _, ok := seen[prodShard]; !ok {
				seen[prodShard] = struct{}{}
				selected = append(selected, prodShard)
			}
		default:
			n, err := strconv.Atoi(tok)
			if err != nil || n <= 0 {
				return nil, oops.Errorf("invalid --shards token %q (expected 'prod' or positive integer or 'all')", tok)
			}
			want := "-shard-" + tok + "db"
			found := ""
			for _, shard := range allShards {
				if strings.Contains(shard, want) {
					found = shard
					break
				}
			}
			if found == "" {
				return nil, oops.Errorf("no such shard %d for db=%s in region=%s", n, db, region)
			}
			if _, ok := seen[found]; !ok {
				seen[found] = struct{}{}
				selected = append(selected, found)
			}
		}
	}
	if len(selected) == 0 {
		return nil, oops.Errorf("no shards selected for db=%s in region=%s (selector=%q)", db, region, selector)
	}
	return selected, nil
}

func isProdShard(shard string) bool {
	// Registry shard names look like: prod-workflowsdb OR prod-workflows-shard-1db.
	// The non-sharded entry ("prod") does not contain "-shard-".
	return strings.HasPrefix(shard, "prod-") && !strings.Contains(shard, "-shard-")
}

func isRunningOrStarting(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "running", "starting":
		return true
	default:
		return false
	}
}

func normalizeTaskType(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "all":
		return "all"
	case "kinesis":
		return "kinesis"
	case "parquet":
		return "parquet"
	default:
		return ""
	}
}

func normalizeRegistryName(db string) string {
	s := strings.ToLower(strings.TrimSpace(db))
	if s == "" {
		return ""
	}
	// Special-case: clouddb should not be trimmed to "cloud".
	if s == "clouddb" {
		return s
	}
	return strings.TrimSuffix(s, "db")
}

func deriveSecretKey(registryName string) string {
	if registryName == "clouddb" {
		return "database_password"
	}
	return registryName + "_database_password"
}

func (o *DMSUpdatePasswordOp) Execute(ctx context.Context) (any, error) {
	if o.dmsClient == nil || o.secretReader == nil || len(o.taskPlans) == 0 {
		return nil, oops.Errorf("Plan() must be called before Execute()")
	}

	// If all matched tasks were already running/starting (for shard-targeted runs),
	// then there is nothing to update/test/resume.
	if len(o.endpointPlansByArn) == 0 {
		var taskIDs []string
		for _, tp := range o.taskPlans {
			taskIDs = append(taskIDs, tp.taskID)
		}
		slog.Infow(ctx, "no DMS tasks require password update; skipping",
			"region", o.Region,
			"database", o.Database,
			"registryName", o.registryName,
			"taskType", o.normalizedTaskType,
			"shards", o.selectedShards,
			"tasksFound", len(o.taskPlans),
			"tasksToUpdate", 0,
		)
		return &DMSUpdatePasswordResult{
			Region:           o.Region,
			Database:         o.Database,
			TaskType:         o.normalizedTaskType,
			Shards:           o.selectedShards,
			TaskIDs:          taskIDs,
			SourceEndpoints:  nil,
			SecretKey:        o.secretKey,
			ResumeTasks:      o.ResumeTasks,
			UpdatedEndpoints: 0,
			FailedEndpoints:  0,
			FailedTasks:      0,
		}, nil
	}

	// Read secret value only during execution. Do not log it.
	password, err := o.secretReader.Read(ctx, o.secretKey)
	if err != nil {
		return nil, oops.Wrapf(err, "read secret: key=%s", o.secretKey)
	}
	if strings.TrimSpace(password) == "" {
		return nil, oops.Errorf("secret %q is empty", o.secretKey)
	}

	// Build endpoint -> tasks mapping so we can produce a clear summary.
	endpointToTasks := make(map[string][]dmsTaskPlan)
	for _, tp := range o.taskPlans {
		if tp.willSkipUpdate {
			continue
		}
		endpointToTasks[tp.sourceEndpointArn] = append(endpointToTasks[tp.sourceEndpointArn], tp)
	}

	type endpointResult struct {
		endpointArn string
		okModify    bool
		okConn      bool
		errModify   error
		errConn     error
	}
	endpointResults := make(map[string]*endpointResult, len(o.endpointPlansByArn))

	updated := 0
	failedEndpoints := 0
	for endpointArn := range o.endpointPlansByArn {
		res := &endpointResult{endpointArn: endpointArn}
		endpointResults[endpointArn] = res

		_, err := o.dmsClient.ModifyEndpointWithContext(ctx, &databasemigrationservice.ModifyEndpointInput{
			EndpointArn: awsV1.String(endpointArn),
			Password:    awsV1.String(password),
		})
		if err != nil {
			res.errModify = oops.Wrapf(err, "modify endpoint password: arn=%s", endpointArn)
			failedEndpoints++
			slog.Errorw(ctx, res.errModify, slog.Tag{
				"endpointArn":        endpointArn,
				"endpointIdentifier": o.endpointPlansByArn[endpointArn].endpointIdentifier,
			})
			continue
		}

		res.okModify = true
		updated++
		slog.Infow(ctx, "✅ Updated DMS endpoint password",
			"endpointArn", endpointArn,
			"endpointIdentifier", o.endpointPlansByArn[endpointArn].endpointIdentifier,
		)
	}

	// Only run connection tests for endpoints we successfully updated.
	for _, key := range o.connectionKeys {
		res := endpointResults[key.endpointArn]
		if res == nil || !res.okModify {
			continue
		}
		if err := o.testConnectionAndWait(ctx, key.replicationInstanceArn, key.endpointArn); err != nil {
			res.errConn = err
			slog.Errorw(ctx, err, slog.Tag{
				"endpointArn":            key.endpointArn,
				"endpointIdentifier":     o.endpointPlansByArn[key.endpointArn].endpointIdentifier,
				"replicationInstanceArn": key.replicationInstanceArn,
			})
			continue
		}
		res.okConn = true
	}

	failedResumeTasks := 0
	if o.ResumeTasks {
		for _, tp := range o.taskPlans {
			if tp.willSkipUpdate {
				continue
			}
			res := endpointResults[tp.sourceEndpointArn]
			// Only resume tasks whose endpoint update + connection check succeeded.
			if res == nil || !res.okModify || !res.okConn {
				continue
			}
			if err := o.resumeTaskIfNeeded(ctx, tp.taskID, tp.taskArn); err != nil {
				// Continue; collect as task failure.
				failedResumeTasks++
				slog.Errorw(ctx, err, slog.Tag{
					"replicationTaskId":  tp.taskID,
					"replicationTaskArn": tp.taskArn,
				})
			}
		}
	}

	var endpointArns []string
	for arn := range o.endpointPlansByArn {
		endpointArns = append(endpointArns, arn)
	}

	var taskIDs []string
	for _, tp := range o.taskPlans {
		taskIDs = append(taskIDs, tp.taskID)
	}

	// Summarize failures per endpoint (which maps to one or more tasks).
	// Note: resume failures are counted separately because resume is per-task, not per-endpoint.
	failedTasksFromEndpoint := 0
	for endpointArn, tasks := range endpointToTasks {
		res := endpointResults[endpointArn]
		if res == nil {
			continue
		}
		if res.errModify != nil || res.errConn != nil {
			failedTasksFromEndpoint += len(tasks)
		}
	}
	failedTasks := failedTasksFromEndpoint + failedResumeTasks

	result := &DMSUpdatePasswordResult{
		Region:           o.Region,
		Database:         o.Database,
		TaskType:         o.normalizedTaskType,
		Shards:           o.selectedShards,
		TaskIDs:          taskIDs,
		SourceEndpoints:  endpointArns,
		SecretKey:        o.secretKey,
		ResumeTasks:      o.ResumeTasks,
		UpdatedEndpoints: updated,
		FailedEndpoints:  failedEndpoints,
		FailedTasks:      failedTasks,
	}

	// Print a final summary and return non-nil error if any failures occurred.
	if failedEndpoints > 0 || failedTasks > 0 {
		slog.Errorw(ctx, oops.Errorf("one or more failures occurred during DMS password update"),
			slog.Tag{
				"region":            o.Region,
				"database":          o.Database,
				"taskType":          o.normalizedTaskType,
				"shards":            o.selectedShards,
				"updatedEndpoints":  updated,
				"failedEndpoints":   failedEndpoints,
				"failedTasks":       failedTasks,
				"failedResumeTasks": failedResumeTasks,
			},
		)
		return result, oops.Errorf(
			"DMS password update completed with failures (failedEndpoints=%d failedTasks=%d failedResumeTasks=%d)",
			failedEndpoints,
			failedTasks,
			failedResumeTasks,
		)
	}

	return result, nil
}

func (o *DMSUpdatePasswordOp) testConnectionAndWait(ctx context.Context, replicationInstanceArn, endpointArn string) error {
	_, err := o.dmsClient.TestConnectionWithContext(ctx, &databasemigrationservice.TestConnectionInput{
		ReplicationInstanceArn: awsV1.String(replicationInstanceArn),
		EndpointArn:            awsV1.String(endpointArn),
	})
	if err != nil {
		if isDMSConnectionAlreadyBeingTested(err) {
			slog.Infow(ctx, "connection already being tested; will wait for result",
				"endpointArn", endpointArn,
				"endpointIdentifier", o.endpointPlansByArn[endpointArn].endpointIdentifier,
				"replicationInstanceArn", replicationInstanceArn,
			)
		} else {
			return oops.Wrapf(err, "test connection request: replicationInstanceArn=%s endpointArn=%s", replicationInstanceArn, endpointArn)
		}
	}

	ctxWait, cancel := context.WithTimeout(ctx, o.ConnTimeout)
	defer cancel()

	ticker := time.NewTicker(o.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctxWait.Done():
			return oops.Wrapf(ctxWait.Err(), "timed out waiting for connection test: replicationInstanceArn=%s endpointArn=%s", replicationInstanceArn, endpointArn)
		case <-ticker.C:
			out, err := o.dmsClient.DescribeConnectionsWithContext(ctxWait, &databasemigrationservice.DescribeConnectionsInput{
				Filters: []*databasemigrationservice.Filter{
					{
						Name:   awsV1.String("endpoint-arn"),
						Values: []*string{awsV1.String(endpointArn)},
					},
					{
						Name:   awsV1.String("replication-instance-arn"),
						Values: []*string{awsV1.String(replicationInstanceArn)},
					},
				},
			})
			if err != nil {
				if request.IsErrorThrottle(oops.Cause(err)) {
					slog.Warnw(ctxWait, "encountered throttling while describing connections; retrying",
						"endpointArn", endpointArn,
						"replicationInstanceArn", replicationInstanceArn,
					)
					continue
				}
				return oops.Wrapf(err, "describe connections: replicationInstanceArn=%s endpointArn=%s", replicationInstanceArn, endpointArn)
			}

			if len(out.Connections) == 0 {
				slog.Infow(ctxWait, "waiting for DMS connection test result",
					"endpointArn", endpointArn,
					"replicationInstanceArn", replicationInstanceArn,
					"status", "unknown",
				)
				continue
			}

			// There should typically be 1 connection for a given pair. Use the most recent by returning the first.
			c := out.Connections[0]
			status := strings.ToLower(awsV1.StringValue(c.Status))
			switch status {
			case "successful":
				slog.Infow(ctxWait, "✅ DMS connection test successful",
					"endpointArn", endpointArn,
					"endpointIdentifier", o.endpointPlansByArn[endpointArn].endpointIdentifier,
					"replicationInstanceArn", replicationInstanceArn,
				)
				return nil
			case "failed":
				return oops.Errorf("DMS connection test failed: endpointArn=%s replicationInstanceArn=%s", endpointArn, replicationInstanceArn)
			default:
				slog.Infow(ctxWait, "waiting for DMS connection test result",
					"endpointArn", endpointArn,
					"replicationInstanceArn", replicationInstanceArn,
					"status", status,
				)
			}
		}
	}
}

func isDMSConnectionAlreadyBeingTested(err error) bool {
	cause := oops.Cause(err)
	aerr, ok := cause.(awserr.Error)
	if !ok {
		return false
	}
	if aerr.Code() != databasemigrationservice.ErrCodeInvalidResourceStateFault {
		return false
	}
	return strings.Contains(strings.ToLower(aerr.Message()), "already being tested")
}

func (o *DMSUpdatePasswordOp) resumeTaskIfNeeded(ctx context.Context, taskID, taskArn string) error {
	// Refresh current status before deciding.
	out, err := o.dmsClient.DescribeReplicationTasksWithContext(ctx, &databasemigrationservice.DescribeReplicationTasksInput{
		Filters: []*databasemigrationservice.Filter{
			{
				Name:   awsV1.String("replication-task-arn"),
				Values: []*string{awsV1.String(taskArn)},
			},
		},
		WithoutSettings: awsV1.Bool(true),
	})
	if err != nil {
		return oops.Wrapf(err, "describe replication task: arn=%s", taskArn)
	}
	if len(out.ReplicationTasks) != 1 {
		return oops.Errorf("expected 1 replication task for arn=%s, got %d", taskArn, len(out.ReplicationTasks))
	}

	status := strings.ToLower(awsV1.StringValue(out.ReplicationTasks[0].Status))
	switch status {
	case "running", "starting":
		slog.Infow(ctx, "task already running; skipping resume",
			"replicationTaskId", taskID,
			"replicationTaskArn", taskArn,
			"status", status,
		)
		return nil
	}

	_, err = o.dmsClient.StartReplicationTaskWithContext(ctx, &databasemigrationservice.StartReplicationTaskInput{
		ReplicationTaskArn:       awsV1.String(taskArn),
		StartReplicationTaskType: awsV1.String(databasemigrationservice.StartReplicationTaskTypeValueResumeProcessing),
	})
	if err != nil {
		return oops.Wrapf(err, "start replication task (resume-processing): arn=%s", taskArn)
	}

	slog.Infow(ctx, "✅ Requested DMS resume-processing",
		"replicationTaskId", taskID,
		"replicationTaskArn", taskArn,
	)
	return nil
}

func defaultSecretsReadProfileForRegion(region string) string {
	switch strings.TrimSpace(strings.ToLower(region)) {
	case "eu-west-1":
		return "eu-readadmin"
	case "ca-central-1":
		return "ca-readadmin"
	default:
		return "readadmin"
	}
}
