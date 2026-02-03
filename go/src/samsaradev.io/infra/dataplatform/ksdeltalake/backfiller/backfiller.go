package backfiller

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/boltdb/bolt"
	"github.com/cznic/mathutil"
	"github.com/samsarahq/go/oops"
	"github.com/schollz/progressbar"
	"golang.org/x/time/rate"

	"samsaradev.io/helpers/errgroup"
	"samsaradev.io/helpers/monitoring"
	"samsaradev.io/helpers/parallelism"
	"samsaradev.io/helpers/slog"
	"samsaradev.io/hubproto/objectstatproto"
	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dataplatform/ksdeltalake/ni/exportversions"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/grpc/grpchelpers/grpcclient"
	"samsaradev.io/infra/releasemanagement/launchdarkly"
	"samsaradev.io/infra/samsaraaws/awshelpers"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/samsaraaws/s3iface"
	"samsaradev.io/infra/samsaraaws/sqsiface"
	"samsaradev.io/infra/serviceorchestration/leaderelection"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/kinesisstatsshards"
	"samsaradev.io/libs/ni/pointer"
	sqscomponents "samsaradev.io/service/components/ni/sqs"
	"samsaradev.io/stats/kinesisstats/archivefilecache"
	"samsaradev.io/stats/kinesisstats/archivefiletypes"
	"samsaradev.io/stats/kinesisstats/jsonexporter"
	"samsaradev.io/stats/kinesisstats/kinesisstatsproto"
	"samsaradev.io/stats/kinesisstats/serieslisterpaginatedclient"
)

type Flags struct {
	table     string
	bucket    string
	stateFile string
}

type KSBackfiller struct {
	appConfig              *config.AppConfig
	grpcDial               grpcclient.DialFunc
	s3Client               s3iface.S3API
	sqsClient              sqsiface.SQSAPI
	waitForLeaderHelper    leaderelection.WaitForLeaderLifecycleHookHelper
	TriggeredBackfillCount int
	PartialBackfill        bool
	launchDarklyBridge     *launchdarkly.SDKBridge
}

func newKSBackfiller(appConfig *config.AppConfig, grpcDial grpcclient.DialFunc, s3Client s3iface.S3API, sqsClient sqsiface.SQSAPI, waitForLeaderHelper leaderelection.WaitForLeaderLifecycleHookHelper, launchDarklyBridge *launchdarkly.SDKBridge) (*KSBackfiller, error) {
	return &KSBackfiller{
		appConfig:              appConfig,
		grpcDial:               grpcDial,
		s3Client:               s3Client,
		sqsClient:              sqsClient,
		waitForLeaderHelper:    waitForLeaderHelper,
		TriggeredBackfillCount: 0,
		launchDarklyBridge:     launchDarklyBridge,
	}, nil
}

func (ksb *KSBackfiller) Run(table string, stateFile string) error {
	ctx := context.Background()

	// If partial backfills, set appropriate variables
	key := "kinesisstats-backfill"
	backfilledMetric := "kinesisstatsdeltalakebackfiller.table.backfilled"
	errorMetric := "kinesisstatsdeltalakebackfiller.table.error"
	if ksb.PartialBackfill {
		key = "kinesisstats-backfill-partial"
		backfilledMetric = "kinesisstatsdeltalakebackfiller.table.partial.backfilled"
		errorMetric = "kinesisstatsdeltalakebackfiller.table.partial.error"
	}

	ksb.waitForLeaderHelper.BlockUntilLeader(ctx)
	s3Client := ksb.s3Client
	sqsClient := ksb.sqsClient
	datadogTags := []string{
		fmt.Sprintf("table:%s", table),
	}
	sess := session.Must(session.NewSession())
	if sess.Config.Region == nil {
		return oops.Errorf("Cannot determine AWS region")
	}
	region := *sess.Config.Region
	prefix := awsregionconsts.RegionPrefix[region]
	dynamodbClient := dynamodb.New(sess)

	// Check if backfill has already been run
	slog.With(ctx, "object-stat", table)
	needsBackfill := 0
	_, err := s3Client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(prefix + "dataplatform-metadata"),
		Key:    aws.String(fmt.Sprintf("%s/%s/completed", key, table)),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchKey {
			needsBackfill = 1
		} else {
			return oops.Wrapf(err, "")
		}
	}

	// Log whether or not the stat is backfilled.
	// We do it once here so that if this script fails we at least track that it needs backfilling.
	monitoring.AggregatedDatadog.Gauge(
		float64((needsBackfill+1)%2),
		backfilledMetric,
		datadogTags...)

	// Check if necessary sqs queues exist
	destQueueName := fmt.Sprintf("samsara_delta_lake_merge_ks_%s_queue", strings.ToLower(table))
	sqsName := sqscomponents.ShortenQueueName(destQueueName)
	accId := strconv.Itoa(infraconsts.GetAccountIdForRegion(region))
	_, err = sqsClient.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: &sqsName, QueueOwnerAWSAccountId: &accId})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == sqs.ErrCodeQueueDoesNotExist {
			return oops.Wrapf(err, "Destination queues do not exist. Please apply terraform to create the necessary infrastructure to backfill this object stat")
		}
		return oops.Wrapf(err, "")
	}

	if needsBackfill == 0 {
		slog.Infow(ctx, "This object stat has already been backfilled")
		return nil
	}
	slog.Infow(ctx, "Object stat has not been backfilled, now backfilling")

	// Check if backfill has attempts
	slog.Infow(ctx, "Checking if attempts file exists...")
	attemptsCount := 0
	maxAttempts := 1
	var attemptsFile *s3.GetObjectOutput
	attemptsFile, err = s3Client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(prefix + "dataplatform-metadata"),
		Key:    aws.String(fmt.Sprintf("%s/%s/attempts", key, table)),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() != s3.ErrCodeNoSuchKey {
			return oops.Wrapf(err, "")
		}
	} else {
		defer attemptsFile.Body.Close()
		// If attempts file exists, read body of file, convert to int and set current attempts count
		body, err := ioutil.ReadAll(attemptsFile.Body)
		if err != nil {
			return oops.Wrapf(err, "Error reading contents of attempts file")
		}
		value, err := strconv.ParseInt(string(body), 10, 64)
		if err != nil {
			return oops.Wrapf(err, "Could not parse contents of attempts file into integer: %s", string(body))
		}
		attemptsCount = int(value)
		slog.Infow(ctx, "Attempts file exists", "retry count", value)
	}

	if attemptsCount > maxAttempts {
		// If retry has happened too many times, abort and send metric
		monitoring.AggregatedDatadog.Gauge(
			1,
			errorMetric,
			datadogTags...)
		return oops.Errorf("Number of attempts has exceeded limit of %d", maxAttempts)
	}
	attemptsCount++

	if _, err = s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(prefix + "dataplatform-metadata"),
		Key:    aws.String(fmt.Sprintf("%s/%s/attempts", key, table)),
		Body:   strings.NewReader(strconv.FormatInt(int64(attemptsCount), 10)),
	}); err != nil {
		return oops.Wrapf(err, "Putting file %sdataplatform-metadata/%s/%s/attempts in S3 failed", prefix, key, table)
	}
	slog.Infow(ctx, "Attempts file has been created/updated and stored in S3")

	db, err := bolt.Open(stateFile, 0600, nil)
	if err != nil {
		return oops.Wrapf(err, "")
	}
	defer db.Close()

	// Initialize state file.
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("files"))
		return err
	}); err != nil {
		return oops.Wrapf(err, "")
	}

	clusters, err := infraconsts.RegionKinesisStatsClusters(region)
	if err != nil {
		return oops.Wrapf(err, "")
	}

	var partialBackfill *jsonexporter.PartialBackfillArgs
	if ksb.PartialBackfill {
		partialBackfill, err = findPartialBackfillParameters(table, prefix+"dataplatform-metadata", s3Client)
		if err != nil {
			return oops.Wrapf(err, "Could not find partial backfill parameters")
		}
	}

	if err := BackfillKsClusters(ctx, ksb.appConfig, ksb.grpcDial, clusters, sqsClient, dynamodbClient, s3Client, db, BackfillInput{
		Region:                  region,
		Table:                   table,
		ArchiveListingRateLimit: rate.Limit(500),
		PartialBackfill:         partialBackfill,
	}, ksb.launchDarklyBridge); err != nil {
		ksb.TriggeredBackfillCount++
		return oops.Wrapf(err, "")
	}
	ksb.TriggeredBackfillCount++

	// Mark backfill completed in S3 with completed file
	if _, err = s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(prefix + "dataplatform-metadata"),
		Key:    aws.String(fmt.Sprintf("%s/%s/completed", key, table)),
	}); err != nil {
		return oops.Wrapf(err, "Putting file %sdataplatform-metadata/%s/%s/completed in S3 failed", prefix, key, table)
	}
	slog.Infow(ctx, "Completed file has been stored in S3")

	// Delete attempts file
	if _, err = s3Client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(prefix + "dataplatform-metadata"),
		Key:    aws.String(fmt.Sprintf("%s/%s/attempts", key, table)),
	}); err != nil {
		return oops.Wrapf(err, "Deleting file %sdataplatform-metadata/%s/%s/attempts in S3 failed", prefix, key, table)
	}
	slog.Infow(ctx, "Attempts file has been deleted")

	// Upon success, we'll want to set backfilled to 1 and errors to 0, just to clean up the gauge value.
	monitoring.AggregatedDatadog.Gauge(1, backfilledMetric, datadogTags...)
	monitoring.AggregatedDatadog.Gauge(0, errorMetric, datadogTags...)

	return nil
}

type chunkFile struct {
	bucket    string
	key       string
	versionId string
	size      int64
}

func (f chunkFile) String() string {
	s := fmt.Sprintf("s3://%s/%s", f.bucket, f.key)
	if f.versionId != "" {
		s += "@" + f.versionId
	}
	return s
}

type BackfillInput struct {
	Region                  string
	Table                   string
	ProgressBar             bool
	StreamFilesOnly         bool
	DryRun                  bool
	ArchiveListingRateLimit rate.Limit
	PartialBackfill         *jsonexporter.PartialBackfillArgs
}

func BackfillKsClusters(
	ctx context.Context,
	appConfig *config.AppConfig,
	dial grpcclient.DialFunc,
	clusters []string,
	sqsClient sqsiface.SQSAPI,
	dynamoClient *dynamodb.DynamoDB,
	s3Client s3iface.S3API,
	db *bolt.DB,
	input BackfillInput,
	launchDarklyBridge *launchdarkly.SDKBridge) error {

	// Before starting anything, let's bump the dynamo version
	// It's not actually a problem if anything in the backfill fails after we do this.
	currentVersion, err := allocateBackfillVersion(ctx, dynamoClient, input.Table)
	if err != nil {
		return oops.Wrapf(err, "couldn't bump dynamo version")
	}

	for _, cluster := range clusters {
		archiveFileCache := archivefilecache.NewDynamoDBArchiveFileCacheForCluster(dynamoClient, s3Client, cluster)
		err := backfillKsCluster(ctx, appConfig, dial, archiveFileCache, sqsClient, db, cluster, currentVersion, input, launchDarklyBridge)
		if err != nil {
			return err
		}
	}

	return nil
}

func backfillKsCluster(
	ctx context.Context,
	appConfig *config.AppConfig,
	dial grpcclient.DialFunc,
	archiveFileCache *archivefilecache.DynamoDBArchiveFileCache,
	sqsClient sqsiface.SQSAPI,
	db *bolt.DB,
	cluster string,
	exportVersion int,
	input BackfillInput,
	launchDarklyBridge *launchdarkly.SDKBridge,
) error {

	if input.ArchiveListingRateLimit == 0 {
		return oops.Errorf("ArchiveListingRateLimit must be set")
	}
	if input.ArchiveListingRateLimit > rate.Limit(500) {
		// As of 2021-09-14, we have ~2000 rps capacity remaining. Make sure to use
		// less than half of that. This ensures we can at least run two concurrent
		// backfills. The archive file DynamoDB table dashboard includes usage from
		// the backfiller cron so we will be able to account for this backfiller's
		// usage when maintaining the capacity allocation.
		return oops.Errorf("ArchiveListingRateLimit must be less than 500")
	}
	bucket := fmt.Sprintf("%s%s-kinesisstats", awsregionconsts.RegionPrefix[input.Region], cluster)

	var bar *progressbar.ProgressBar
	if input.ProgressBar {
		bar = progressbar.NewOptions(0, progressbar.OptionShowIts(), progressbar.OptionShowCount(), progressbar.OptionThrottle(time.Second))
		defer bar.Finish()
	}

	var streamChunkFiles []chunkFile
	var archiveChunkFiles []chunkFile

	// 1. List all stream files.
	fmt.Printf("Listing all stream files for %s in %s\n", input.Table, cluster)
	streamDirectoryListerUrl := appConfig.KinesisStatsServiceConfigByCluster[cluster].StreamDirectoryListerUrlBase
	streamDirectoryListerConn, err := dial(streamDirectoryListerUrl, grpcclient.DialFuncOptions{})
	if err != nil {
		return oops.Wrapf(err, "dial: %s", streamDirectoryListerUrl)
	}
	streamDirectoryLister := kinesisstatsproto.NewStreamDirectoryListerServiceClient(streamDirectoryListerConn)

	for shard := range kinesisstatsshards.AllKinesisStatsShardRanges() {
		listStreamOutput, err := streamDirectoryLister.ListChunkfiles(ctx, &kinesisstatsproto.ListChunkfilesRequest{
			Shard: shard,
		})
		if err != nil {
			return oops.Wrapf(err, "list stream files for shard %s from %s", shard, streamDirectoryListerUrl)
		}
		for _, file := range listStreamOutput.Files {
			streamChunkFiles = append(streamChunkFiles, chunkFile{
				bucket:    bucket,
				key:       file.Key,
				versionId: file.VersionId,
				size:      file.NumBytes,
			})
		}
	}

	if !input.StreamFilesOnly {
		fmt.Printf("Listing all series for %s in %s\n", input.Table, cluster)
		// 2. List all series for the given stat. We need the series list to
		// enumerate archive files.
		statType, ok := objectstatproto.ObjectStatEnum_value[input.Table]
		if !ok && input.Table != "location" {
			return oops.Errorf("%s is not an objectstat stat type", input.Table)
		}
		seriesListerUrl := appConfig.KinesisStatsServiceConfigByCluster[cluster].SerieslisterUrlBase
		seriesListerConn, err := dial(seriesListerUrl, grpcclient.DialFuncOptions{})
		if err != nil {
			return oops.Wrapf(err, "list stream files for shard %s from %s", cluster, streamDirectoryListerUrl)
		}
		seriesLister := kinesisstatsproto.NewSeriesListerServiceClient(seriesListerConn)
		seriesListerClient := serieslisterpaginatedclient.NewSeriesListerPaginatedClient(seriesLister, launchDarklyBridge)

		var getSeriesRequest *kinesisstatsproto.GetSeriesRequest
		if input.Table == "location" {
			getSeriesRequest, err = serieslisterpaginatedclient.CreateLocationStatRequest()
		} else {
			getSeriesRequest, err = serieslisterpaginatedclient.CreateObjectStatsStattypesRequest([]int32{statType})
		}
		if err != nil {
			return oops.Wrapf(err, "failed to create serieslister get request")
		}
		getSeriesOutput, err := seriesListerClient.GetSeries(ctx, getSeriesRequest)
		if err != nil {
			return oops.Wrapf(err, "get series for stat %d from %s", statType, seriesListerUrl)
		}

		// 3. List all archive files for the given stat, by going through every
		// series. Wrap the list call with a rate limiter to avoid overloading the
		// file cache. Do this in parallel because DynamoDB roundtrips can take a
		// while.
		if bar != nil {
			bar.Describe(fmt.Sprintf("list archive files in %s", cluster))
			bar.ChangeMax(len(getSeriesOutput.SeriesList))
			bar.Reset()
		}
		limiter := rate.NewLimiter(input.ArchiveListingRateLimit, 1)

		group, groupCtx := errgroup.WithContext(ctx)
		seriesQueue := make(chan string, len(getSeriesOutput.SeriesList))
		for _, series := range getSeriesOutput.SeriesList {
			seriesQueue <- series
		}
		close(seriesQueue)

		var archiveFilesMu sync.Mutex
		var archiveFiles []*archivefiletypes.ArchiveFileWithSizeVersionId
		for i := 0; i < 64; i++ {
			group.Go(func() error {
				for series := range seriesQueue {
					if err := limiter.Wait(groupCtx); err != nil {
						return oops.Wrapf(err, "wait")
					}
					list, err := archiveFileCache.ReadFileList(groupCtx, series)
					if err != nil {
						return oops.Wrapf(err, "read archive file list for series: %s", series)
					}
					archiveFilesMu.Lock()
					archiveFiles = append(archiveFiles, list...)
					archiveFilesMu.Unlock()
					if bar != nil {
						bar.Add(1)
					}
				}
				return nil
			})
		}
		if err := group.Wait(); err != nil {
			return oops.Wrapf(err, "")
		}
		for _, file := range archiveFiles {
			archiveChunkFiles = append(archiveChunkFiles, chunkFile{
				bucket:    bucket,
				key:       file.String(),
				size:      file.Size,
				versionId: file.VersionId,
			})
		}
		// 4. If startMs and endMs are set, use to do a partial backfill by filtering out archive chunkfiles by ms
		if input.PartialBackfill != nil {
			archiveChunkFiles, err = filterChunkFilesByMs(archiveChunkFiles, input.PartialBackfill.StartMs, input.PartialBackfill.EndMs)
			if err != nil {
				return oops.Wrapf(err, "Cannot filter chunk files by Ms")
			}
		}
	}

	// 5. Filter files not yet backfilled.
	files := append(streamChunkFiles, archiveChunkFiles...)
	pending := make([]chunkFile, 0, len(files))
	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("files"))
		for _, file := range files {
			if b.Get([]byte(file.String())) == nil {
				pending = append(pending, file)
			}
		}
		return nil
	}); err != nil {
		return oops.Wrapf(err, "")
	}

	if input.DryRun {
		for _, file := range pending {
			fmt.Printf("s3://%s/%s\n", file.bucket, file.key)
		}
		return nil
	}

	if bar != nil {
		bar.Describe(fmt.Sprintf("enqueue files for %s", cluster))
		bar.ChangeMax(len(pending))
		bar.Reset()
	}

	// 6. Backfill in batches.
	fmt.Printf("beginning to send backfill messages for %s in %s\n", input.Table, cluster)
	const batchSize = 10
	var batches [][]chunkFile
	for i := 0; i*batchSize < len(pending); i++ {
		batchStart := i * batchSize
		batchEnd := mathutil.Min(len(pending), (i+1)*batchSize)
		batch := pending[batchStart:batchEnd]
		batches = append(batches, batch)
	}

	parallelism.For(ctx, batches, func(ctx context.Context, batch []chunkFile) (e error) {
		now := time.Now().In(time.UTC).Format(time.RFC3339)

		queueUrl := fmt.Sprintf("https://sqs.%s.amazonaws.com/%d/samsara_kinesisstats_json_backfill_export_queue", input.Region, infraconsts.GetAccountIdForRegion(input.Region))
		sendMessageInput := sqs.SendMessageBatchInput{
			QueueUrl: aws.String(queueUrl),
		}
		idToEntry := make(map[string]chunkFile, len(batch))

		// If partial backfill, set firstTime and lastTime variables so correct values can be packaged into the backfill request
		var partialBackfill *jsonexporter.PartialBackfillArgs
		if input.PartialBackfill != nil {
			partialBackfill = &jsonexporter.PartialBackfillArgs{
				StartMs: input.PartialBackfill.StartMs,
				EndMs:   input.PartialBackfill.EndMs,
			}
		}

		for i, entry := range batch {
			msg := jsonexporter.SQSMessage{
				SqsS3NotificationMessage: awshelpers.SqsS3NotificationMessage{
					Records: []events.S3EventRecord{
						{
							EventTime: time.Now(),
							S3: events.S3Entity{
								Bucket: events.S3Bucket{
									Name: entry.bucket,
								},
								Object: events.S3Object{
									Key:       entry.key,
									Size:      entry.size,
									VersionID: entry.versionId,
								},
							},
						},
					},
				},

				// Signal to jsonexportworker that this is for backfill. This makes sure
				// hourly stream files are exported.
				BackfillRequest: &jsonexporter.BackfillRequest{
					TableName:       input.Table,
					PartialBackfill: partialBackfill,
					ExportVersion:   pointer.IntPtr(exportVersion),
				},
			}

			b, err := json.Marshal(msg)
			if err != nil {
				return oops.Wrapf(err, "")
			}
			id := strconv.Itoa(i)
			idToEntry[id] = entry
			sendMessageInput.Entries = append(sendMessageInput.Entries, &sqs.SendMessageBatchRequestEntry{
				Id:          aws.String(id),
				MessageBody: aws.String(string(b)),
			})
		}

		batchResponse, err := sqsClient.SendMessageBatchWithContext(ctx, &sendMessageInput)
		if err != nil {
			return oops.Wrapf(err, "")
		}

		if len(batchResponse.Successful) > 0 {
			// record successfully sent messages in state db
			if err := db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte("files"))
				for _, success := range batchResponse.Successful {
					entry := idToEntry[*success.Id]
					if err := b.Put([]byte(entry.String()), []byte(now)); err != nil {
						return oops.Wrapf(err, "put: %s", entry.String())
					}
				}
				return nil
			}); err != nil {
				return oops.Wrapf(err, "")
			}
		}

		if len(batchResponse.Failed) > 0 {
			failures := make([]string, len(batchResponse.Failed))
			for i, fail := range batchResponse.Failed {
				entry := idToEntry[*fail.Id]
				failures[i] = fmt.Sprintf("Entry: %s\nFailed with response: %s", entry.String(), fail.String())
			}
			return oops.Errorf("Send message encountered failures in response:\n%s", strings.Join(failures, "\n\n"))
		}

		if bar != nil {
			bar.Add(len(batch))
		}
		return nil
		// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html
		// For normal SQS queues, they support "nearly unlimited" number of message sends. We had set this at
		// 20, but it was taking quite a while to get through all the messages. I'm bumping to 128 as it worked
		// running locally. We can even consider higher in the future.
	}, parallelism.WithMaxConcurrency(128), parallelism.WithoutCancelOnError())
	fmt.Printf("done sending backfill messages for %s in %s\n", input.Table, cluster)
	return nil
}

func filterChunkFilesByMs(files []chunkFile, startMs int64, endMs int64) ([]chunkFile, error) {
	ctx := context.Background()
	filteredFiles := []chunkFile{}

	slog.Infow(ctx, "Now filtering chunk files", "startms", startMs, "endMs", endMs)
	for _, file := range files {
		name, err := archivefiletypes.ParseArchiveFile(file.key)
		if err != nil {
			slog.Errorw(ctx, oops.Wrapf(err, "Could not parse archive file key %s", file.key), nil)
		}

		if name.FirstTime > endMs || startMs > name.LastTime {
			continue
		}
		filteredFiles = append(filteredFiles, file)
	}
	return filteredFiles, nil
}

func findPartialBackfillParameters(table string, bucket string, s3Client s3iface.S3API) (*jsonexporter.PartialBackfillArgs, error) {
	var completedAt *time.Time
	// Get completed file
	out, err := s3Client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fmt.Sprintf("kinesisstats-backfill/%s/completed", table)),
	})
	if err != nil {
		return nil, oops.Wrapf(err, "Unable to get completed file")
	} else {
		completedAt = out.LastModified
	}

	// Subtract 1 month from completedAt to get start time and convert to ms
	startTime := completedAt.AddDate(0, -1, 0)
	startMs := startTime.UTC().Unix() * 1000

	// Convert last modified to ms to get end time
	endMs := completedAt.UTC().Unix() * 1000

	return &jsonexporter.PartialBackfillArgs{
		StartMs: startMs,
		EndMs:   endMs,
	}, nil
}

// Bumps the export version for the table and returns the export version for the backfill.
// If a table starts at version N, we bump the version to N+2 and have the backfiller use version
// N+1, so that ongoing replication is prioritized over this backfill, and this backfill
// is prioritized over all previous data.
func allocateBackfillVersion(ctx context.Context, client *dynamodb.DynamoDB, table string) (int, error) {
	// Try to find the current version for the table. A nil value indicates there is no value for the
	// table yet (i.e. effectively the current version is 0).
	out, err := client.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(true),
		Key: map[string]*dynamodb.AttributeValue{
			"table": {
				S: aws.String(table),
			},
		},
		TableName: aws.String(exportversions.ExportVersionsTableName),
	})
	if err != nil {
		return 0, oops.Wrapf(err, "error fetching table version from dynamo")
	}

	currentVersion := 0

	if out.Item != nil {
		var elem exportversions.TableVersionEntry
		err = dynamodbattribute.UnmarshalMap(out.Item, &elem)
		if err != nil {
			return 0, oops.Wrapf(err, "failed to unmarshal element %v", out.Item)
		}
		currentVersion = elem.CurrentVersion
	}

	// Bump the table version to currentVersion + 2 so that ongoing replication
	// is prioritized higher than the backfill, whose version will be currentVersion+1.
	newElem := exportversions.TableVersionEntry{
		Table:          table,
		CurrentVersion: currentVersion + 2,
	}
	marshaledMap, err := dynamodbattribute.MarshalMap(newElem)
	if err != nil {
		return 0, oops.Wrapf(err, "failed to marshal new element %v", newElem)
	}

	_, err = client.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		Item:      marshaledMap,
		TableName: aws.String(exportversions.ExportVersionsTableName),
	})

	if err != nil {
		return 0, oops.Wrapf(err, "failed to bump version number")
	}

	return currentVersion + 1, nil
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(newKSBackfiller)
}
