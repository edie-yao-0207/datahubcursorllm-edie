package difftool

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"path/filepath"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/gqlschema/gqlobjstat"
	"samsaradev.io/helpers/appenv"
	"samsaradev.io/helpers/monitoring"
	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/helpers/samtime"
	"samsaradev.io/helpers/slog"
	"samsaradev.io/hubproto/objectstatproto"
	"samsaradev.io/infra/dataplatform/databricks"
	_ "samsaradev.io/infra/dataplatform/databricksoauthfx"
	"samsaradev.io/infra/dataplatform/ksdeltalake/difftool/allserieslister"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/deploy/helpers"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/samsaraaws/awshelpers/awssessions"
	"samsaradev.io/infra/thunder/batch"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/pointer"
	"samsaradev.io/models"
	"samsaradev.io/stats/kinesisstats/jsonexporter/cf2jsonlschema"
	"samsaradev.io/stats/kinesisstats/objectstatsaccessor"
	"samsaradev.io/stats/kinesisstats/serieslisterpaginatedclient"
	"samsaradev.io/stats/s3stats"
)

type DiffTool struct {
	region string

	appModels    models.ReplicaAppModelsInput
	accessor     objectstatsaccessor.Accessor
	s3Client     s3iface.S3API
	dbxClient    databricks.API
	serieslister allserieslister.AllSeriesListerClient
}

func New(appModels models.ReplicaAppModelsInput, accessor objectstatsaccessor.Accessor, s3client s3iface.S3API, dbxClient databricks.API, seriesLister allserieslister.AllSeriesListerClient) (*DiffTool, error) {
	sess := awssessions.NewInstrumentedAWSSession()
	region := aws.StringValue(sess.Config.Region)

	return &DiffTool{region: region, appModels: appModels, accessor: accessor, s3Client: s3client, dbxClient: dbxClient, serieslister: seriesLister}, nil
}

type DiffToolParams struct {
	FullOrgIds              []int64
	DeviceRandomSampleCount int64
	StartDate               time.Time
	EndDate                 time.Time
	Stats                   []objectstatproto.ObjectStatEnum
	ExportPrefix            string
	Local                   bool
}

type diffToolManifest struct {
	RunTimestamp   int64                                          `json:"run_timestamp"`
	StartDate      string                                         `json:"start_date"`
	EndDate        string                                         `json:"end_date"`
	StatEnums      []objectstatproto.ObjectStatEnum               `json:"stat_enums"`
	Stats          []string                                       `json:"stats"`
	StatsToDevices map[objectstatproto.ObjectStatEnum][]orgObject `json:"stats_to_devices"`
}

// Specifies how many devices' ks data we'll fetch at a time.
var DevicesChunkSize = 500

// TODO: Support specifying non-device stats
// TODO: When running locally we can only use orgs that are allowed in the local dev env. Find a way to fetch basic
//
//	device/org data for any org. Maybe gqlindexservice or some kind of rpc call to some prod service?
func (d *DiffTool) Run(args DiffToolParams) error {
	bucket := helpers.GetS3PrefixByRegion(d.region) + "databricks-kinesisstats-diffs"
	prefix := "ks_exports"
	if args.Local {
		bucket = "samsara-databricks-workspace"
		prefix = "dataplatform/ksdifftool/ks_exports"
	}

	startDate := args.StartDate.UTC().Truncate(24 * time.Hour)
	endDate := args.EndDate.UTC().Truncate(24 * time.Hour)
	runStart := time.Now().UTC()

	endDateString := endDate.Format("2006-01-02")
	exportId := fmt.Sprintf("%s-%s-%d", args.ExportPrefix, endDateString, samtime.TimeToMs(runStart))
	ctx := slog.With(context.Background(), "exportId", exportId)

	// Set up some datadog metrics
	startTime := time.Now()
	status := "failed"

	defer func() {
		appName := appenv.AppName()
		runtime := time.Now().Sub(startTime).Milliseconds()

		slog.Infow(ctx, "Inside deferred func", "app", appName, "runtime", runtime)

		tags := []string{
			fmt.Sprintf("status:%s", status),
			fmt.Sprintf("app:%s", appName),
			fmt.Sprintf("prefix:%s", args.ExportPrefix),
		}
		monitoring.AggregatedDatadog.Incr("ksdifftool.executions", tags...)

		// This only _really_ makes sense to graph for the cronjob, but logging it for the manual
		// jobs as well since it may still provide some information if you are sure the metric
		// logged corresponds to your run.
		monitoring.AggregatedDatadog.Gauge(float64(runtime), "ksdifftool.execution_duration", tags...)

		slog.Infow(ctx, "Finished Deferred func!", "app", appName, "runtime", runtime)
	}()

	slog.Infow(ctx, fmt.Sprintf("Exporting json dumps for exportId %s to folder s3://%s/%s/%s", exportId, bucket, prefix, exportId))

	allDeviceMap, err := d.getCustomerAndSpecifiedDevices(ctx, args.FullOrgIds)
	if err != nil {
		return oops.Wrapf(err, "couldn't fetch customer devices")
	}

	statsToDevices := make(map[objectstatproto.ObjectStatEnum][]orgObject)
	for _, stat := range args.Stats {
		fetchedDevices, err := d.fetchAndWriteStat(ctx, runStart, stat, allDeviceMap, bucket, prefix, exportId, startDate, endDate, args.DeviceRandomSampleCount, args.FullOrgIds)

		// In case a particular stat errors, don't fail the whole task; just log it and don't include that
		// stat in the manifest.
		if err != nil {
			slog.Errorw(ctx, oops.Wrapf(err, "failed to fetch and write data for stat %d", stat), nil)
			monitoring.AggregatedDatadog.Incr("ksdifftool.failed_stat", []string{
				fmt.Sprintf("app:%s", appenv.AppName()),
				fmt.Sprintf("stat:%s", stat.String()),
				fmt.Sprintf("prefix:%s", args.ExportPrefix),
			}...)
		} else {
			statsToDevices[stat] = fetchedDevices
		}
	}

	// Write out a manifest file, including only stats that succeeded.
	var stats []objectstatproto.ObjectStatEnum
	var statStrings []string
	for stat := range statsToDevices {
		stats = append(stats, stat)
		statStrings = append(statStrings, objectstatproto.ObjectStatEnum_name[int32(stat)])
	}

	manifest := diffToolManifest{
		RunTimestamp:   samtime.TimeToMs(runStart),
		StatsToDevices: statsToDevices,
		StartDate:      startDate.Format("2006-01-02"),
		EndDate:        endDate.Format("2006-01-02"),
		StatEnums:      stats,
		Stats:          statStrings,
	}
	manifestPath := fmt.Sprintf("%s/%s/manifest.json", prefix, exportId)
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return oops.Wrapf(err, "couldn't jsonify manifest file")
	}

	slog.Infow(ctx, fmt.Sprintf("Writing manifest file to s3 %s/%s", bucket, manifestPath))
	_, err = d.s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(manifestPath),
		Body:   bytes.NewReader(manifestBytes),
	})
	if err != nil {
		return oops.Wrapf(err, "couldn't write manifest file to s3")
	}

	// Hit the DBX api to start the job
	runInput := &databricks.SubmitRunInput{
		NewCluster: &databricks.NewCluster{
			AutoScale: &databricks.ClusterAutoScale{
				MinWorkers: 1,
				MaxWorkers: 16,
			},
			SparkVersion: sparkversion.KsDiffToolDbrVersion,
			SparkEnvVars: map[string]string{
				"AWS_DEFAULT_REGION": d.region,
				"AWS_REGION":         d.region,
				"DATABRICKS_DEFAULT_SERVICE_CREDENTIAL_NAME": "ks-diff-tool",
			},
			SparkConf: dataplatformresource.SparkConf{
				DisableQueryWatchdog: true,
			}.ToMap(),
			AwsAttributes: &databricks.ClusterAwsAttributes{
				InstanceProfileArn: fmt.Sprintf("arn:aws:iam::%d:instance-profile/dataplatform-cluster", infraconsts.GetDatabricksAccountIdForRegion(d.region)),
			},
			InitScripts:      nil,
			NodeTypeId:       "rd-fleet.2xlarge",
			DriverNodeTypeId: "rd-fleet.xlarge",
			ClusterLogConf:   nil,
			CustomTags: map[string]string{
				"samsara:service":        "databricks-kinesisstats-diff-tool",
				"samsara:team":           "dataplatform",
				"samsara:product-group":  "Platform",
				"samsara:rnd-allocation": "1",
			},
		},
		SparkPythonTask: &databricks.SparkPythonTask{
			PythonFile: fmt.Sprintf("s3://%sdataplatform-deployed-artifacts/%s/job/datacollector/ksdifftool/infra/dataplatform/tools/ksdifftool.py/ksdifftool.py",
				helpers.GetS3PrefixByRegion(d.region), infraconsts.GetProdCloudByRegion(d.region).DatabricksResourcePrefix),
			Parameters: []string{
				"--export-id", exportId,
			},
		},
		Libraries: []*databricks.Library{
			{
				Maven: &databricks.MavenLibrary{
					Coordinates: "uk.co.gresearch.spark:spark-extension_2.12:1.3.2-3.0",
				},
			},
			{
				Pypi: &databricks.PypiLibrary{
					Package: string(dataplatformresource.SparkPyPIDatadog),
				},
			},
		},
	}

	if args.Local {
		// Write script from local to the databricks-workspace bucket to enable
		// easier development locally.
		python_file := filepath.Join(
			filepathhelpers.BackendRoot,
			"python3/samsaradev/infra/dataplatform/tools/ksdifftool.py")
		filebytes, err := ioutil.ReadFile(python_file)
		if err != nil {
			return oops.Wrapf(err, "couldn't read the local script to upload")
		}

		_, err = d.s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
			Bucket: aws.String("samsara-databricks-workspace"),
			Key:    aws.String("dataplatform/ksdifftool/ksdifftool.py"),
			Body:   bytes.NewReader(filebytes),
		})
		if err != nil {
			return oops.Wrapf(err, "couldnt write script to s3")
		}

		// Reference the script and use the dataplatform team cluster instead of
		// creating a new one; this will be much faster while iterating.
		runInput = &databricks.SubmitRunInput{
			ExistingClusterId: pointer.StringPtr("0701-003853-revel825"),
			SparkPythonTask: &databricks.SparkPythonTask{
				PythonFile: "s3://samsara-databricks-workspace/dataplatform/ksdifftool/ksdifftool.py",
				Parameters: []string{
					"--export-id", exportId, "--dev",
				},
			},
			RunName: "local_testing_run",
		}
	}

	submitOutput, err := d.dbxClient.SubmitRun(ctx, runInput)
	if err != nil {
		return oops.Wrapf(err, "could not submit run to dbx")
	}

	isDone := false
	isFirst := true

	for !isDone {
		runOutput, err := d.dbxClient.GetRun(ctx, &databricks.GetRunInput{RunId: submitOutput.RunId})
		if err != nil {
			return oops.Wrapf(err, "could not get run data")
		}

		if isFirst {
			isFirst = false
			slog.Infow(ctx, fmt.Sprintf("here's the run page url %s", runOutput.RunPageUrl))
		}

		if runOutput.State.ResultState != nil {
			isDone = true
			slog.Infow(ctx, fmt.Sprintf("Job completed with state %s", *runOutput.State.ResultState), "runPageUrl", runOutput.RunPageUrl)
		} else {
			slog.Infow(ctx, fmt.Sprintf("Job is in state %s", runOutput.State.LifeCycleState), "runPageUrl", runOutput.RunPageUrl)
		}
		time.Sleep(time.Second * 10)

	}

	// Set job status to success before returning so that
	// it gets picked up by our deferred datadog metric
	status = "success"
	return nil
}

type orgObject struct {
	OrgId    int64 `json:"org_id"`
	ObjectId int64 `json:"object_id"`
}

func (d *DiffTool) fetchAndWriteStat(
	ctx context.Context,
	runStart time.Time,
	statType objectstatproto.ObjectStatEnum,
	deviceMap orgDeviceMap,
	bucket string,
	prefix string,
	exportId string,
	startDate time.Time,
	endDate time.Time,
	randomSampleCount int64,
	orgIds []int64) ([]orgObject, error) {
	sixHoursAgo := samtime.TimeToMs(runStart.Add(-1 * 6 * time.Hour))

	// Figure out what we're going to sample over
	devices, err := d.getDevicesForStat(ctx, deviceMap, statType, startDate, randomSampleCount, orgIds)
	if err != nil {
		return nil, oops.Wrapf(err, "couldnt get devices")
	}

	orgDevices := make([]orgObject, 0, len(devices))
	for _, device := range devices {
		orgDevices = append(orgDevices, orgObject{OrgId: device.OrgId, ObjectId: device.Id})
	}

	for chunkId, chunk := range models.ChunkDevices(devices, DevicesChunkSize) {
		for t := startDate; t.Before(endDate); t = t.Add(24 * time.Hour) {
			ksEndMs := samtime.TimeToMs(t.Add(24 * time.Hour))
			currentDateStr := t.Format("2006-01-02")
			slog.Infow(ctx, fmt.Sprintf("Fetching stats for chunkId %d with statType %d and endMs %d for date %s", chunkId, statType, ksEndMs, currentDateStr))

			deviceBatch := models.DeviceBatchFromSlice(chunk)
			statsBatch, err := d.fetchStatsWithRetry(ctx, deviceBatch, statType, ksEndMs)
			if err != nil {
				return nil, oops.Wrapf(err, "couldnt get stats")
			}

			// Collect + filter all the points and produce a jsonlines file.
			// TODO: consider streaming this to s3 instead of writing it out to a temporary buffer here.
			fileBuffer := bytes.NewBuffer([]byte{})
			jsonEncoder := json.NewEncoder(fileBuffer)
			for batchIdx, stats := range statsBatch {
				device := deviceBatch[batchIdx]
				for _, stat := range stats {
					date := samtime.MsToTime(stat.ChangedAtMs).Format("2006-01-02")

					// Ignore changes that were ingested less than 6 hours ago. This gives them
					// time to make it through the merge queue and not erroneously give us diffs.
					if date != currentDateStr || stat.GetIngestedTimeMs() > sixHoursAgo {
						continue
					}
					msg := cf2jsonlschema.ObjectStatOutput{
						Date:       date,
						StatType:   statType,
						OrgId:      device.OrgId,
						ObjectType: objectstatproto.ObjectTypeEnum_otDevice,
						ObjectId:   device.Id,
						Time:       stat.ChangedAtMs,
						Value: &cf2jsonlschema.ObjectStatValue{
							Date:                 "", // This field is not filled in in production, so I don't fill it in here.
							Time:                 stat.ChangedAtMs,
							ReceivedDeltaSeconds: stat.ReceivedDeltaSec,
							IsStart:              stat.IsStart,
							IsEnd:                stat.IsEnd,
							IsDataBreak:          stat.IsDataBreak,
							IntValue:             stat.IntValue,
							DoubleValue:          stat.DoubleValue,
						},
					}
					if stat.ProtoValue != nil {
						msg.Value.ProtoValue = stat.ProtoValue
					}
					if err = jsonEncoder.Encode(msg); err != nil {
						// This represents a case where we cannot jsonify the stat value. This basically should never happen
						// unless there is a Nan or Inf value in the proto; in this case, our normal ingestion path will also
						// fail to parse them and they won't exist in databricks. (Our frontend does
						// partially support these stat values so they still do show up in graphiql and other places).
						// Skipping this row would be bad since it would replicate the skipping we're already doing on the
						// datalake ingestion path and give the false idea that theres nothing missing. So instead we will
						// replace the `value` field with a stringified version of the proto
						// field, and jsonify the rest and output it to ks exports so that we at least see some difference
						// in dbx. We'll also log in the manifest that we couldn't parse the protos for some fields.
						newMsg, err := replaceValueWithString(msg)
						if err != nil {
							return nil, oops.Wrapf(err, "failed to rewrap unparsable message")
						}
						if err = jsonEncoder.Encode(newMsg); err != nil {
							slog.Warnw(ctx, "Even after removing the value field we couldn't write out the value. Skipping...", "stat", fmt.Sprintf("%v", msg))
						}
					}
				}
			}

			folderPath := fmt.Sprintf("%s/%s/%s/date=%s", prefix, exportId, objectstatproto.ObjectStatEnum_name[int32(statType)], currentDateStr)
			filePath := fmt.Sprintf("%s/%d.jsonl", folderPath, chunkId)

			// Write files to s3
			slog.Infow(ctx, fmt.Sprintf("Writing out file to s3://%s/%s", bucket, filePath))
			_, err = d.s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(filePath),
				Body:   bytes.NewReader(fileBuffer.Bytes()),
			})
			if err != nil {
				return nil, oops.Wrapf(err, "couldn't write file to s3")
			}
		}
	}

	return orgDevices, nil
}

type orgDeviceMap map[int64]map[int64]*models.Device

func (o orgDeviceMap) getDevice(orgId, deviceId int64) *models.Device {
	if orgmap, ok := o[orgId]; ok {
		if device, ok := orgmap[deviceId]; ok {
			return device
		}
	}
	return nil
}

// Given a stat and the total map of valid devices, return the correct set of devices to sample
func (d *DiffTool) getDevicesForStat(
	ctx context.Context,
	deviceMap orgDeviceMap,
	statType objectstatproto.ObjectStatEnum,
	startDate time.Time,
	randomSampleCount int64,
	orgIds []int64) ([]*models.Device, error) {
	// Step 1: list all series for this stat.
	allSeries, err := d.listAndParseSeries(ctx, statType, startDate)
	if err != nil {
		return nil, oops.Wrapf(err, "could not get relevant stats")
	}

	orgIdsSet := make(map[int64]struct{})
	for _, orgId := range orgIds {
		orgIdsSet[orgId] = struct{}{}
	}

	// Step 2: For orgs that we are fully sampling, grab everything. Put everything else into a new list we will sample from.
	var fullOrgDevices []*models.Device
	var samplableDevices []*models.Device

	for _, series := range allSeries {
		device := deviceMap.getDevice(series.orgId, series.objectId)
		if device == nil {
			continue
		}
		if _, ok := orgIdsSet[series.orgId]; ok {
			fullOrgDevices = append(fullOrgDevices, device)
		} else {
			samplableDevices = append(samplableDevices, device)
		}
	}

	// Step 3: Sample as many series as we need.
	sampledDevices := make([]*models.Device, 0, randomSampleCount)
	rand.Shuffle(len(samplableDevices), func(i, j int) {
		samplableDevices[i], samplableDevices[j] = samplableDevices[j], samplableDevices[i]
	})
	for i := int64(0); i < randomSampleCount && i < int64(len(samplableDevices)); i++ {
		sampledDevices = append(sampledDevices, samplableDevices[i])
	}

	// Step 4: Combine and sort. Under the hood, the ks accessor batch device endpoint
	// will end up making calls per org, so sorting it helps make sure we make as
	// few total calls as possible.
	toReturn := append(fullOrgDevices, sampledDevices...)
	sort.Slice(toReturn, func(i, j int) bool {
		if toReturn[i].OrgId == toReturn[j].OrgId {
			return toReturn[i].Id < toReturn[j].Id
		}
		return toReturn[i].OrgId < toReturn[j].OrgId
	})

	return toReturn, nil

}

// Return a map from orgid -> deviceid -> models.Device
func (d *DiffTool) getCustomerAndSpecifiedDevices(ctx context.Context, orgIds []int64) (orgDeviceMap, error) {
	allOrgs, err := d.appModels.Models.Organization.All(ctx)
	if err != nil {
		return nil, oops.Wrapf(err, "couldn't fetch all orgs")
	}

	fullOrgIdsSet := make(map[int64]struct{})
	var customerOrgIds []int64
	for _, id := range orgIds {
		fullOrgIdsSet[id] = struct{}{}
		customerOrgIds = append(customerOrgIds, id)
	}

	for _, org := range allOrgs {
		if _, ok := fullOrgIdsSet[org.Id]; !ok && org.InternalType == models.OrgTypeCustomer {
			customerOrgIds = append(customerOrgIds, org.Id)
		}
	}

	// Get all devices for all customer orgs
	allDevices, err := d.appModels.Models.Device.ByOrgIdMany(ctx, customerOrgIds)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to fetch all devices")
	}

	orgdevicemap := make(map[int64]map[int64]*models.Device)
	for _, device := range allDevices {
		if _, ok := orgdevicemap[device.OrgId]; !ok {
			orgdevicemap[device.OrgId] = make(map[int64]*models.Device)
		}
		orgdevicemap[device.OrgId][device.Id] = device
	}

	return orgdevicemap, nil
}

type parsedSeries struct {
	orgId    int64
	objectId int64
}

func (d *DiffTool) listAndParseSeries(ctx context.Context, statType objectstatproto.ObjectStatEnum, startDate time.Time) ([]parsedSeries, error) {
	start := time.Now()
	req, err := serieslisterpaginatedclient.CreateObjectStatsStattypesRequest([]int32{int32(statType)})
	if err != nil {
		return nil, oops.Wrapf(err, "error creating serieslister stattypes request")
	}
	series, err := d.serieslister.GetSeriesWithTimeMs(ctx, req)
	if err != nil {
		return nil, oops.Wrapf(err, "error listing series for stat %d", statType)
	}
	if series.SeriesList == nil {
		return nil, oops.Errorf("GetSeriesWithTimeMs returned unexpected nil SeriesList")
	}
	slog.Infow(ctx, fmt.Sprintf("took %f seconds to list stats for %d, got %d results", time.Now().Sub(start).Seconds(), statType, len(series.SeriesList.List)))

	parsed := make([]parsedSeries, 0, len(series.SeriesList.List))
	for _, series := range series.SeriesList.List {
		if samtime.MsToTime(series.TimeMs).Before(startDate) {
			continue
		}

		objectId, err := s3stats.ParseSeriesObjectId(series.Series)
		if err != nil {
			return nil, oops.Wrapf(err, "couldn't parse object from series %s", series)
		}

		orgId, err := s3stats.ParseSeriesOrgId(series.Series)
		if err != nil {
			return nil, oops.Wrapf(err, "couldn't parse org from series %s", series)
		}

		parsed = append(parsed, parsedSeries{objectId: objectId, orgId: orgId})
	}
	return parsed, nil
}

func replaceValueWithString(msg cf2jsonlschema.ObjectStatOutput) (map[string]interface{}, error) {
	stringifiedProto := msg.Value.ProtoValue.String()

	// Make sure to remove this before we start since it can't be jsonified.
	msg.Value = nil
	bytes, err := json.Marshal(msg)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to marshal the message even with a nil value, this should never happen. original message %v", msg)
	}
	var newMessage map[string]interface{}
	err = json.Unmarshal(bytes, &newMessage)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to unmarshal the message into a map[string]interface, this should never happen. original message %v", msg)
	}

	// Intentionally replacing the entire `value` field and not just value.proto_value.
	// When our script can't parse this entire field it considers the row unparsable. (it's hard to figure out
	// if just parsing the proto value section failed or not).
	newMessage["value"] = stringifiedProto
	return newMessage, nil
}

func (d *DiffTool) fetchStatsWithRetry(ctx context.Context, deviceBatch models.DeviceBatch, statType objectstatproto.ObjectStatEnum, ksEndMs int64) (map[batch.Index][]*gqlobjstat.ObjStatDatapoint, error) {
	dayDurationMs := int64(24 * 60 * 60 * 1000)
	var statsBatch map[batch.Index][]*gqlobjstat.ObjStatDatapoint
	var err error
	for retryCount := 1; retryCount <= 5; retryCount++ {
		statsBatch, err = d.accessor.GetDeviceBatchObjectStats(ctx, deviceBatch, objectstatsaccessor.ObjectStatArgs{
			StatTypeEnum: &statType,
			EndTime:      &ksEndMs,
			Duration:     &dayDurationMs,
		})

		// If successful, return early.
		if err == nil {
			return statsBatch, nil
		}

		// Otherwise, exponentially back off.
		time.Sleep(time.Duration(math.Pow(2, float64(retryCount))) * time.Second)
	}

	return nil, err
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(New)
}
