package emrworker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	featureconfigs "samsaradev.io/infra/dataplatform/emrworker/featureconfigs"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/samsarahq/go/oops"
	"go.uber.org/fx"

	"samsaradev.io/helpers/appenv"
	"samsaradev.io/helpers/monitoring"
	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/config"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/releasemanagement/featureconfig"
	"samsaradev.io/infra/samsaraaws/awshelpers/sqsworker"
	"samsaradev.io/infra/samsaraaws/sqsiface"
	"samsaradev.io/libs/ni/pointer"
	"samsaradev.io/platform/entity/entitydataaccessor"
	"samsaradev.io/platform/entity/entitymetadataaccessor"
	"samsaradev.io/platform/entity/entityproto"
	"samsaradev.io/platform/entity/types/value"
)

// EmrSqsWorker handles processing of EMR SQS messages and forwards them to Kinesis
type EmrSqsWorker struct {
	sqsClient              sqsiface.SQSAPI
	worker                 *sqsworker.SQSWorker
	config                 EmrSqsWorkerConfig
	entityDataAccessor     entitydataaccessor.Accessor
	entityMetadataAccessor entitymetadataaccessor.Accessor
	featureConfigEvaluator featureconfig.Evaluator
	kinesisClient          kinesisiface.KinesisAPI
	s3Client               s3iface.S3API
}

// EmrSqsWorkerConfig holds the configuration for the EMR processor
type EmrSqsWorkerConfig struct {
	ProcessMessageTimeout    time.Duration
	VisibilityTimeoutSeconds int64
	EntityName               string // Added for testing support
	StreamName               string // Kinesis stream name
	Concurrency              uint   // Number of concurrent workers
	S3Bucket                 string // S3 bucket for large record offloading
}

// bucketTimestampWithinXMinuteInterval creates a time-bucketed string from a timestamp
// minuteInterval specifies the bucket size in minutes (e.g., 5 for 5-minute buckets)
// Examples with minuteInterval=5:
// "2025-10-23T12:26:33Z" -> "2025-10-23T12:25-12:30"
// "2025-10-23T12:20:00Z" -> "2025-10-23T12:20-12:25"
// "2025-10-23T23:57:00Z" -> "2025-10-23T23:55-24:00"
func bucketTimestampWithinXMinuteInterval(timestamp string, minuteInterval int) string {
	// Parse the timestamp
	t, err := time.Parse(time.RFC3339, timestamp)
	if err != nil {
		// If parsing fails, return the original timestamp truncated at the first colon
		if colonIndex := strings.Index(timestamp, ":"); colonIndex != -1 {
			return timestamp[:colonIndex]
		}
		return timestamp
	}

	// Get the minute and round down to the nearest interval
	minute := t.Minute()
	bucketStartMinute := (minute / minuteInterval) * minuteInterval
	bucketEndMinute := bucketStartMinute + minuteInterval

	// Handle hour rollover (e.g., 23:55-24:00 becomes 23:55-00:00)
	startHour := t.Hour()
	endHour := startHour
	if bucketEndMinute >= 60 {
		bucketEndMinute = 0
		endHour = (startHour + 1) % 24
	}

	// Format the bucket string
	dateStr := t.Format("2006-01-02")
	startTime := fmt.Sprintf("%02d:%02d", startHour, bucketStartMinute)
	endTime := fmt.Sprintf("%02d:%02d", endHour, bucketEndMinute)

	return fmt.Sprintf("%sT%s-%s", dateStr, startTime, endTime)
}

// get queue URL from entity name
func getQueueUrl(appConfig *config.AppConfig, emrSqsWorkerConfig EmrSqsWorkerConfig) (string, error) {
	var entityNameToQueueUrl = map[string]string{
		"asset":                   appConfig.EmrEntityReplicationAssetSqsQueueUrl,
		"speedingintervalsbytrip": appConfig.EmrEntityReplicationSpeedingIntervalsByTripSqsQueueUrl,
		"trip":                    appConfig.EmrEntityReplicationTripSqsQueueUrl,
		"hosviolation":            appConfig.EmrEntityReplicationHosViolationSqsQueueUrl,
		"driver":                  appConfig.EmrEntityReplicationDriverSqsQueueUrl,
		"tag":                     appConfig.EmrEntityReplicationTagSqsQueueUrl,
	}
	queueUrl, ok := entityNameToQueueUrl[strings.ToLower(emrSqsWorkerConfig.EntityName)]
	if !ok {
		return "", oops.Errorf("entity name not found in map: %s", emrSqsWorkerConfig.EntityName)
	}
	return queueUrl, nil
}

// New creates a new EMR processor instance
func New(
	lc fx.Lifecycle,
	appConfig *config.AppConfig,
	sqsClient sqsiface.SQSAPI,
	newSqsWorker sqsworker.Factory,
	entityDataAccessor entitydataaccessor.Accessor,
	entityMetadataAccessor entitymetadataaccessor.Accessor,
	emrSqsWorkerConfig EmrSqsWorkerConfig,
	session *session.Session,
	featureConfigEvaluator featureconfig.Evaluator,
) (*EmrSqsWorker, error) {
	emrSqsWorker := &EmrSqsWorker{
		sqsClient:              sqsClient,
		config:                 emrSqsWorkerConfig,
		entityDataAccessor:     entityDataAccessor,
		entityMetadataAccessor: entityMetadataAccessor,
		kinesisClient:          kinesis.New(session),
		s3Client:               s3.New(session),
		featureConfigEvaluator: featureConfigEvaluator,
	}

	queueUrl, err := getQueueUrl(appConfig, emrSqsWorkerConfig)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to get queue URL")
	}

	workerConfig := sqsworker.SQSWorkerConfig{
		QueueUrl:                 queueUrl,
		ProcessMessageTimeout:    emrSqsWorkerConfig.ProcessMessageTimeout,
		VisibilityTimeoutSeconds: emrSqsWorkerConfig.VisibilityTimeoutSeconds,
		Concurrency:              emrSqsWorkerConfig.Concurrency,
	}

	worker, err := newSqsWorker(workerConfig, emrSqsWorker)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to create SQSworker")
	}
	emrSqsWorker.worker = worker

	// Add metrics reporting
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				if err := emrSqsWorker.reportMetrics(context.Background()); err != nil {
					slog.Errorw(context.Background(), err, slog.Tag{"message": "Error reporting metrics"})
				}
			}()
			return nil
		},
		OnStop: func(context.Context) error {
			return nil
		},
	})

	return emrSqsWorker, nil
}

type emrChangelogMsg struct {
	EventId    string `json:"eventId"`
	EntityName string `json:"entityName"`
	EventType  string `json:"eventType"`
	Timestamp  string `json:"timestamp"`
	OrgId      string `json:"orgId"`
	RecordId   string `json:"recordId"`
}

// unmarshalMessage takes the message body received from SQS and unmarshals it into a ChangeEvent.
func (p *EmrSqsWorker) unmarshalMessage(ctx context.Context, msg *sqs.Message) (*emrChangelogMsg, error) {
	if msg.Body == nil {
		return nil, oops.Errorf("message body is nil")
	}

	slog.Infow(ctx, "EMR Replication SQS Worker processing message", "body", *msg.Body)

	entityName := p.config.EntityName

	var changelogMsg emrChangelogMsg
	if err := json.Unmarshal([]byte(*msg.Body), &changelogMsg); err != nil {
		monitoring.AggregatedDatadog.Incr(
			"entity.emrreplicationsqsworker.process_message.error",
			"entity_name:"+entityName,
			"error_type:unable_to_unmarshal",
		)
		return nil, oops.Wrapf(err, "failed to unmarshal EMR message")
	}

	// log the entity id, event type, timestamp and org id
	slog.Infow(ctx, "EMR Replication SQS Worker processing ChangeEvent",
		"event_id", changelogMsg.EventId,
		"entity_name", changelogMsg.EntityName,
		"event_type", changelogMsg.EventType,
		"timestamp", changelogMsg.Timestamp,
		"org_id", changelogMsg.OrgId,
		"record_id", changelogMsg.RecordId,
	)

	decodedRecordId, err := base64.StdEncoding.DecodeString(changelogMsg.RecordId)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to decode record id")
	}
	changelogMsg.RecordId = string(decodedRecordId)

	// Check if the entity name is the same as the one in the environment variable
	envEntityName := os.Getenv("EMR_REPLICATION_ENTITY_NAME")
	if envEntityName != "" && strings.EqualFold(envEntityName, p.config.EntityName) {
		monitoring.AggregatedDatadog.Incr(
			"entity.emrreplicationsqsworker.env_entity_name_check.count",
			"entity_name:"+changelogMsg.EntityName,
		)
	}

	if entityName != changelogMsg.EntityName {
		monitoring.AggregatedDatadog.Incr(
			"entity.emrreplicationsqsworker.process_message.error",
			"entity_name:"+entityName,
			"error_type:entity_name_mismatch",
		)
		return nil, oops.Errorf("entity name mismatch, wrong entity name received: %s", changelogMsg.EntityName)
	}

	return &changelogMsg, nil
}

func (p *EmrSqsWorker) queryEMRData(ctx context.Context, changeEvent *emrChangelogMsg) (*value.Value, error) {
	// convert changeEvent.OrgId to int64
	orgId, err := strconv.ParseInt(changeEvent.OrgId, 10, 64)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to convert orgId to int64")
	}

	entityMap, err := p.entityMetadataAccessor.GetFullEntityMap(ctx, orgId)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to get entity map")
	}

	entity := entityMap.Get(p.config.EntityName)
	if entity == nil {
		return nil, oops.Errorf("entity not found for name: %s", p.config.EntityName)
	}

	authzPrincipalId := &entityproto.AuthzPrincipalId{
		InternalServiceName: pointer.New(appenv.AppName()),
	}

	// For CDC, we specify the recordID as the primary key and expect a single item in the response.
	data, err := p.entityDataAccessor.GetUnjoinedEntityById(ctx, orgId, authzPrincipalId, entity, changeEvent.RecordId)
	if err != nil {
		monitoring.AggregatedDatadog.Incr(
			"entity.emrreplicationsqsworker.process_message.error",
			"entity_name:"+p.config.EntityName,
			"error_type:get_unjoined_entity_by_id",
		)
		return nil, oops.Wrapf(err, "failed to get unjoined entity by id")
	}

	return &data, nil
}

// ChangeOperation represents the type of change operation being performed.

type ChangeOperation string

const (
	ChangeOperationCreateOrUpdate ChangeOperation = "createOrUpdate"
	ChangeOperationDelete         ChangeOperation = "delete"
)

// EmrKinesisRecord represents a record sent to Kinesis for EMR replication
//
// Large Record Handling:
// When EMR data exceeds Kinesis size limits (1MB), the data is automatically
// offloaded to S3 and only a reference is sent via Kinesis. This ensures
// reliable delivery while maintaining the single-message consumer experience.
//
// Consumer Usage:
// Downstream consumers should read from the export S3 bucket to retrieve
// the offloaded data.
type EmrKinesisRecord struct {
	OrgId           string          `json:"orgId"`
	RecordId        string          `json:"recordId"`
	EmrResponse     *value.Value    `json:"emrResponse,omitempty"` // Inline data (for small records)
	S3Path          string          `json:"s3Path,omitempty"`      // S3 path (for large records)
	Timestamp       string          `json:"timestamp"`
	ChangeOperation ChangeOperation `json:"changeOperation"`
	EntityName      string          `json:"entityName"`
}

// constructKinesisRecord constructs the record to be written to kinesis based on the emr data received from the entity data accessor.
// If the record is too large, it will offload the EmrResponse data to S3.
func (p *EmrSqsWorker) constructKinesisRecord(ctx context.Context, orgId int64, changeEvent *emrChangelogMsg, emrData *value.Value) (*EmrKinesisRecord, error) {
	record := &EmrKinesisRecord{
		OrgId:           fmt.Sprintf("%d", orgId),
		RecordId:        changeEvent.RecordId,
		Timestamp:       changeEvent.Timestamp,
		ChangeOperation: ChangeOperationCreateOrUpdate,
		EntityName:      p.config.EntityName,
	}

	// If the EMR Response is nil, it means that the record pertaining to the
	// change event has been deleted so we need to send the signal to the
	// downstream consumer to delete the record from the replicated table or
	// act accordingly.
	if emrData.IsNil() {
		record.ChangeOperation = ChangeOperationDelete
		record.EmrResponse = emrData // Set the nil value so tests can check it
		return record, nil
	}

	// First try with entire EmrResponse data
	record.EmrResponse = emrData

	// Check if the record exceeds size limit when marshaled
	recordBytes, err := json.Marshal(record)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to marshal record for size check")
	}

	const kinesisMaxRecordSize = 1048576
	if len(recordBytes) > kinesisMaxRecordSize {
		// Record is too large, offload to S3
		s3Path, err := p.uploadToS3(ctx, emrData, orgId, changeEvent.RecordId, changeEvent.Timestamp)
		if err != nil {
			monitoring.AggregatedDatadog.Incr(
				"entity.emrreplicationsqsworker.s3_upload.error",
				"entity_name:"+p.config.EntityName,
			)
			return nil, oops.Wrapf(err, "failed to upload large record to S3")
		}

		// Create S3 reference record
		record.EmrResponse = nil // Remove the large data
		record.S3Path = s3Path

		// S3 offloading successful
		monitoring.AggregatedDatadog.Incr(
			"entity.emrreplicationsqsworker.s3_offload.success",
			"entity_name:"+p.config.EntityName,
		)
		slog.Infow(ctx, "Record offloaded to S3 successfully",
			"original_size", len(recordBytes),
			"s3_path", s3Path,
			"entity_name", p.config.EntityName,
			"org_id", record.OrgId,
			"record_id", record.RecordId,
		)
	}

	return record, nil
}

func (p *EmrSqsWorker) writeToKinesisDataStream(record *EmrKinesisRecord, timestampBucket string, receiveCount int) error {
	// Marshal the record to JSON
	recordBytes, err := json.Marshal(record)
	if err != nil {
		monitoring.AggregatedDatadog.Incr(
			"entity.emrreplicationsqsworker.process_message.error",
			"entity_name:"+p.config.EntityName,
			"error_type:json_marshal",
		)
		return oops.Wrapf(err, "failed to marshal record to JSON")
	}

	// Log if we're sending an S3 path record
	if record.S3Path != "" {
		slog.Infow(context.Background(), "Sending a record with S3 Path to Kinesis",
			"record_size", len(recordBytes),
			"s3_path", record.S3Path,
			"entity_name", p.config.EntityName,
			"org_id", record.OrgId,
			"record_id", record.RecordId,
		)
		monitoring.AggregatedDatadog.Incr(
			"entity.emrreplicationsqsworker.process_message.s3_path_sent",
			"entity_name:"+p.config.EntityName,
		)
	}
	partitionKey := aws.String(fmt.Sprintf("%s-%s-%s-%d", p.config.EntityName, record.OrgId, timestampBucket, receiveCount))

	// Use the `entityName-orgId-receiveCount` as the partition key to ensure records for the same org are processed in order
	input := &kinesis.PutRecordInput{
		Data:         recordBytes,
		StreamName:   aws.String(p.config.StreamName),
		PartitionKey: partitionKey,
	}

	// Send the record to Kinesis
	backoffs := []time.Duration{0, 5 * time.Second, 10 * time.Second, 20 * time.Second}

	var lastErr error
	for i, backoff := range backoffs {
		time.Sleep(backoff)
		_, lastErr = p.kinesisClient.PutRecord(input)
		if lastErr == nil {
			break
		}
		monitoring.AggregatedDatadog.Incr(
			"entity.emrreplicationsqsworker.process_message.retry",
			"entity_name:"+p.config.EntityName,
			"retry_count:"+strconv.Itoa(i+1),
		)
		slog.Warnw(context.Background(), "failed to put record to Kinesis stream:", "Attempt number", i+1, "error:", lastErr)
	}

	if lastErr != nil {
		monitoring.AggregatedDatadog.Incr(
			"entity.emrreplicationsqsworker.process_message.error",
			"entity_name:"+p.config.EntityName,
			"error_type:kinesis_put",
		)
		return oops.Wrapf(lastErr, "failed to put record to Kinesis stream after retries")
	}

	monitoring.AggregatedDatadog.Incr(
		"entity.emrreplicationsqsworker.process_message.success",
		"entity_name:"+p.config.EntityName,
		"event_type:write_to_kinesis_data_stream_success",
	)

	return nil
}

// ProcessMessage processes a single SQS message
func (p *EmrSqsWorker) ProcessMessage(ctx context.Context, msg *sqs.Message) error {
	monitoring.AggregatedDatadog.Incr(
		"entity.emrreplicationsqsworker.process_message.count",
		"entity_name:"+p.config.EntityName,
		"event_type:message_received",
	)

	var count int

	// Get and log the receive count
	if countStr, ok := msg.Attributes["ApproximateReceiveCount"]; ok {
		count, _ = strconv.Atoi(aws.StringValue(countStr))
		slog.Infow(ctx, "Processing SQS message",
			"receive_count", count,
			"entity_name", p.config.EntityName,
		)
		monitoring.AggregatedDatadog.Incr(
			"entity.emrreplicationsqsworker.message.receive_count",
			"entity_name:"+p.config.EntityName,
			"receive_count:"+strconv.Itoa(count),
		)
	}

	changeEvent, err := p.unmarshalMessage(ctx, msg)
	if err != nil {
		return oops.Wrapf(err, "failed to unmarshal EMR message")
	}

	monitoring.AggregatedDatadog.Incr(
		"entity.emrreplicationsqsworker.process_message.count",
		"entity_name:"+p.config.EntityName,
		"event_type:unmarshal_message_success",
	)

	// convert changeEvent.OrgId to int64
	orgId, err := strconv.ParseInt(changeEvent.OrgId, 10, 64)
	if err != nil {
		return oops.Wrapf(err, "failed to convert orgId to int64")
	}

	// Check feature flag with extracted orgId
	enableQueryEMRData, err := featureconfigs.EnableEmrSqsWorkerQueryEmrData(ctx, p.featureConfigEvaluator, orgId)
	if err != nil {
		return oops.Wrapf(err, "failed to get enableQueryEMRData feature flag")
	}
	if !enableQueryEMRData {
		emrSqsWorkerSkipQueryEMRDataCount := "entity.emrreplicationsqsworker.skip_query_emr_data.count"
		monitoring.AggregatedDatadog.Incr(emrSqsWorkerSkipQueryEMRDataCount, "reason:feature_disabled")
		return nil
	}

	emrData, err := p.queryEMRData(ctx, changeEvent)
	if err != nil {
		return oops.Wrapf(err, "failed to query EMR data")
	}

	monitoring.AggregatedDatadog.Incr(
		"entity.emrreplicationsqsworker.process_message.count",
		"entity_name:"+p.config.EntityName,
		"event_type:query_emr_data_success",
	)

	kinesisRecord, err := p.constructKinesisRecord(ctx, orgId, changeEvent, emrData)
	if err != nil {
		return oops.Wrapf(err, "failed to construct kinesis record")
	}

	monitoring.AggregatedDatadog.Incr(
		"entity.emrreplicationsqsworker.process_message.count",
		"entity_name:"+p.config.EntityName,
		"event_type:construct_kinesis_record_success",
	)

	timestampBucket := bucketTimestampWithinXMinuteInterval(changeEvent.Timestamp, 1)

	err = p.writeToKinesisDataStream(kinesisRecord, timestampBucket, count)
	if err != nil {
		return oops.Wrapf(err, "failed to write to Kinesis data stream")
	}

	monitoring.AggregatedDatadog.Incr(
		"entity.emrreplicationsqsworker.process_message.success",
		"entity_name:"+p.config.EntityName,
	)

	return nil
}

// reportMetrics periodically reports metrics about the worker
func (p *EmrSqsWorker) reportMetrics(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			slog.Infow(ctx, "EMR Replication SQS Worker sleeping")
		}
	}
}

// uploadToS3 uploads large EMR data to S3 and returns the S3 key
func (p *EmrSqsWorker) uploadToS3(ctx context.Context, emrData *value.Value, orgId int64, recordId string, timestamp string) (string, error) {
	// Marshal the EMR data to JSON
	emrBytes, err := json.Marshal(emrData)
	if err != nil {
		return "", oops.Wrapf(err, "failed to marshal EMR data for S3 upload")
	}

	// Generate S3 key orgId/recordId/timestamp.json as file name
	// Sanitize timestamp for use in S3 key (remove problematic characters)
	// This is to avoid issues with S3 key names
	// It changes the filename from 2025-06-19T12:00:00Z.json to 20250619120000.json
	replacer := strings.NewReplacer(":", "", "-", "", "T", "", "Z", "")
	sanitizedTimestamp := replacer.Replace(timestamp)
	s3Key := fmt.Sprintf("cdc/large-data/%s/%d/%s/%s.json", p.config.EntityName, orgId, recordId, sanitizedTimestamp)

	// Upload to S3
	_, err = p.s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(p.config.S3Bucket),
		Key:         aws.String(s3Key),
		Body:        strings.NewReader(string(emrBytes)),
		ContentType: aws.String("application/json"),
		Metadata: map[string]*string{
			"entity-name": aws.String(p.config.EntityName),
			"org-id":      aws.String(fmt.Sprintf("%d", orgId)),
			"record-id":   aws.String(recordId),
		},
	})
	if err != nil {
		return "", oops.Wrapf(err, "failed to upload EMR Response data to S3")
	}

	// Increment the metric for successful S3 upload
	monitoring.AggregatedDatadog.Incr(
		"entity.emrreplicationsqsworker.s3_upload.success",
		"entity_name:"+p.config.EntityName,
	)

	slog.Infow(ctx, "Successfully uploaded large EMR data to S3",
		"s3_bucket", p.config.S3Bucket,
		"s3_key", s3Key,
		"data_size", len(emrBytes),
		"entity_name", p.config.EntityName,
		"org_id", orgId,
		"record_id", recordId,
	)

	// Return the full S3 URL for the offloaded data to be sent as part
	// of the kinesis record.
	fullS3URL := fmt.Sprintf("s3://%s/%s", p.config.S3Bucket, s3Key)
	return fullS3URL, nil
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(New)
}
