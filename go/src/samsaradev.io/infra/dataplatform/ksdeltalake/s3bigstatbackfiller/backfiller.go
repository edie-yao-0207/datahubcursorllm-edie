package s3bigstatbackfiller

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/samsarahq/go/oops"
	"github.com/zorkian/go-datadog-api"

	"samsaradev.io/helpers/monitoring"
	"samsaradev.io/helpers/samtime"
	"samsaradev.io/helpers/slog"
	"samsaradev.io/hubproto/objectstatproto"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/monitoring/datadogdsl"
	"samsaradev.io/infra/monitoring/datadogutils"
	"samsaradev.io/infra/samsaraaws/awshelpers"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/samsaraaws/dynamodbiface"
	"samsaradev.io/infra/samsaraaws/s3iface"
	"samsaradev.io/infra/samsaraaws/sqsiface"
	"samsaradev.io/infra/serviceorchestration/leaderelection"
	"samsaradev.io/service/components/sqsregistry/sqsdefinitions"
)

const S3BigstatsBackfillStateTableName = "s3bigstats-backfill-state"

var batchSize int64 = 1000
var maxSqsBatchSize int = 10

type BackfillStatus string

const (
	FinishedStatus   = "finished"
	InProgressStatus = "in-progress"
	ErrorStatus      = "error"
)

type Flags struct {
	table     string
	bucket    string
	stateFile string
}

type S3BigStatBackfiller struct {
	sqsClient           sqsiface.SQSAPI
	waitForLeaderHelper leaderelection.WaitForLeaderLifecycleHookHelper
	datadogClient       *datadog.Client
	dynamoClient        dynamodbiface.DynamoDBAPI
	athenaClient        *athena.Athena
	s3Client            s3iface.S3API
}

type DynamoBackfillStatus struct {
	BigstatName    string `dynamodbav:"bigstat_name"`
	BackfillStatus string `dynamodbav:"backfill_status"`
	ExecutionId    string `dynamodbav:"execution_id"`
	NextToken      string `dynamodbav:"next_token"`
	ExpectedCount  string `dynamodbav:"expected_count"`
	ProcessedCount string `dynamodbav:"processed_count"`
}

func newS3BigStatBackfiller(s3Client s3iface.S3API, athenaClient *athena.Athena, sqsClient sqsiface.SQSAPI, dynamoClient dynamodbiface.DynamoDBAPI, datadogClient *datadog.Client, waitForLeaderHelper leaderelection.WaitForLeaderLifecycleHookHelper) (*S3BigStatBackfiller, error) {
	return &S3BigStatBackfiller{
		athenaClient:        athenaClient,
		sqsClient:           sqsClient,
		s3Client:            s3Client,
		waitForLeaderHelper: waitForLeaderHelper,
		datadogClient:       datadogClient,
		dynamoClient:        dynamoClient,
	}, nil
}

// SetBackfillStatusToError sets the backfill status to error so that no other cron job can pick it up until the error is resolved.
// Once fixed the dynamo entry has to be deleted to redo the backfill or the status must be set to InProgress to continue the backfilll
func (b *S3BigStatBackfiller) SetBackfillStatusToError(ctx context.Context, table string, region string) error {
	ddTags := []string{fmt.Sprintf("table:%s", table), fmt.Sprintf("region:%s", region)}
	// Update the dynamo status to started
	_, dynamoError := b.dynamoClient.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(S3BigstatsBackfillStateTableName),
		Key: map[string]*dynamodb.AttributeValue{
			"bigstat_name": {S: aws.String(table)},
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":s": {
				S: aws.String(string(ErrorStatus)),
			},
		},
		UpdateExpression: aws.String("set backfill_status = :s"),
	})
	if dynamoError != nil {
		// If this error happens, it will stay in the "inprogress" state, so no backfill cron job can pick it up
		// But it will require manually changing the state to resume
		err := oops.Wrapf(dynamoError, "Dynamo in a bad state, saw an error in a backfill and couldn't update dynamo status from inprogress to started")
		datadogutils.IncrInvocation(&err, "s3bigstats.backfill.dynamostate.error", ddTags...)
		return err
	}
	return nil
}

func (b *S3BigStatBackfiller) GetBackfillStatus(ctx context.Context, table string, region string) (string, error) {
	// Get table status from dynamo
	ddTags := []string{fmt.Sprintf("table:%s", table), fmt.Sprintf("region:%s", region)}
	tableStatus, err := b.dynamoClient.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(S3BigstatsBackfillStateTableName),
		Key: map[string]*dynamodb.AttributeValue{
			"bigstat_name": {S: aws.String(table)},
		},
	})
	if err != nil {
		ddTagsErr := append(ddTags, "errorType:dynamoGetStatusBeforeRunningBackfill")
		datadogutils.IncrInvocation(&err, "s3bigstats.backfill.error", ddTagsErr...)
		return "", oops.Wrapf(err, "error getting item from dynamo for object %v table %v", table, S3BigstatsBackfillStateTableName)
	}

	var dynamoBackfillStatus DynamoBackfillStatus
	if err := dynamodbattribute.UnmarshalMap(tableStatus.Item, &dynamoBackfillStatus); err != nil {
		ddTagsErr := append(ddTags, "errorType:UnmarshalDynamoStatusBeforeRunningBackfill")
		datadogutils.IncrInvocation(&err, "s3bigstats.backfill.error", ddTagsErr...)
		return "", oops.Wrapf(err, "error unmarshal map: %v", tableStatus.Item)
	}
	return dynamoBackfillStatus.BackfillStatus, nil
}

func (b *S3BigStatBackfiller) Run(ctx context.Context, table string, stateFile string, region string) error {
	ddTags := []string{fmt.Sprintf("table:%s", table), fmt.Sprintf("region:%s", region)}
	// Get table status from dynamo
	tableStatus, err := b.dynamoClient.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(S3BigstatsBackfillStateTableName),
		Key: map[string]*dynamodb.AttributeValue{
			"bigstat_name": {S: aws.String(table)},
		},
	})
	if err != nil {
		ddTagsErr := append(ddTags, "errorType:dynamoGetStatus")
		datadogutils.IncrInvocation(&err, "s3bigstats.backfill.error", ddTagsErr...)
		return oops.Wrapf(err, "error getting item from dynamo for object %v table %v", table, S3BigstatsBackfillStateTableName)
	}

	// If there is no item, we have not started the backfill.
	if tableStatus.Item == nil {
		// Query athena for the s3 keys for all the big stats for that table.
		// The athena query results will be outputted in a s3 file.
		execId, err := b.queryAthenaForBigStatKeys(ctx, region, table)
		if err != nil {
			ddTagsErr := append(ddTags, "errorType:athenaQueryBigStatsKeys")
			datadogutils.IncrInvocation(&err, "s3bigstats.backfill.error", ddTagsErr...)
			return oops.Wrapf(err, "error querying athena for object %v", table)
		}

		count, err := b.queryAthenaForBigStatKeysCount(ctx, region, table)
		if err != nil {
			ddTagsErr := append(ddTags, "errorType:athenaQueryBigStatsCount")
			datadogutils.IncrInvocation(&err, "s3bigstats.backfill.error", ddTagsErr...)
			return oops.Wrapf(err, "error querying athena for object count %v", table)
		}
		// Update the status of the backfill to dynamo where the status is "started" and the
		// execution id is populated from the athena query.
		item := map[string]*dynamodb.AttributeValue{
			"bigstat_name":    {S: aws.String(table)},
			"backfill_status": {S: aws.String(string(InProgressStatus))},
			"execution_id":    {S: aws.String(execId)},
			"expected_count":  {S: aws.String(string(strconv.Itoa(count)))},
			"processed_count": {S: aws.String(string(strconv.Itoa(0)))},
		}
		_, err = b.dynamoClient.PutItemWithContext(ctx, &dynamodb.PutItemInput{
			Item:                item,
			TableName:           aws.String(S3BigstatsBackfillStateTableName),
			ConditionExpression: aws.String("attribute_not_exists(big_stat_name)"),
		})
		if err != nil {
			err = oops.Wrapf(err, "error putting item from dynamo for object %v table %v exec id %v", table, S3BigstatsBackfillStateTableName, execId)
			ddTagsErr := append(ddTags, "errorType:dynamoPutItem")
			datadogutils.IncrInvocation(&err, "s3bigstats.backfill.error", ddTagsErr...)
			return err
		}

		// Query and update the table status.
		tableStatus = &dynamodb.GetItemOutput{
			Item: item,
		}
	}

	var dynamoBackfillStatus DynamoBackfillStatus
	if err := dynamodbattribute.UnmarshalMap(tableStatus.Item, &dynamoBackfillStatus); err != nil {
		err = oops.Wrapf(err, "error unmarshal map: %v", tableStatus.Item)
		ddTagsErr := append(ddTags, "errorType:dynamoUnmarshallStatus")
		datadogutils.IncrInvocation(&err, "s3bigstats.backfill.error", ddTagsErr...)
		return err
	}

	// If the status is finished, the backfill is complete.
	if dynamoBackfillStatus.BackfillStatus == FinishedStatus {
		return nil
	}

	// If the status is an error, the backfill shouldnt run until
	// a dev as looked into the error and changed the status manually back to inprogress
	// or restarted the backfill by deleting the dynamo entry
	if dynamoBackfillStatus.BackfillStatus == ErrorStatus {
		return nil
	}

	// If the status is started the backfill is in progress. Use the athena client to read a
	// page at a time of the s3 keys.
	if dynamoBackfillStatus.BackfillStatus == string(InProgressStatus) {
		execId := dynamoBackfillStatus.ExecutionId

		processedCount, err := strconv.Atoi(dynamoBackfillStatus.ProcessedCount)
		if err != nil {
			err = oops.Wrapf(err, "Couldn't parse proccessed count from dynamo status")
			ddTagsErr := append(ddTags, "errorType:dynamoParseStatusProccesdCount")
			datadogutils.IncrInvocation(&err, "s3bigstats.backfill.error", ddTagsErr...)
			return err
		}
		// In the first request nextToken is an empty string, and then gets populated for every future request
		err = b.fetchS3KeysAndPushToSqs(ctx, region, b.athenaClient, execId, dynamoBackfillStatus.NextToken, table, ddTags, int64(processedCount))
		if err != nil {
			return oops.Wrapf(err, "error in fetchS3KeysAndPushToSqs ")
		}

		// Since we just ran a backfill, we want to return true
		return nil
	}
	return nil
}

func (b *S3BigStatBackfiller) queryAthenaForBigStatKeys(ctx context.Context, region string, table string) (string, error) {
	slog.Infow(ctx, "Querying athena for s3bigstat", "table", table)
	// yesterday := time.Now().AddDate(0, -1, 0) // s3inventory files are generated every day, so we want the files from yesterday
	// dateStr := fmt.Sprintf("%s-00-00", yesterday.Format("2006-01-02"))
	athenaQuerySql := fmt.Sprintf(`
	SELECT
	  bucket,
	  url_decode(key) AS key,
	  CAST(size AS bigint) AS size,
	  version_id,
	  dt
	FROM s3inventory.big_object_stats
	WHERE is_latest = true
	AND is_delete_marker = false
	AND key LIKE '%%stat_type=%v%%'
	AND dt = '2021-10-14-01-00'
	`, objectstatproto.ObjectStatEnum_value[table])

	start := samtime.TimeNowInMs()
	outputBucket := fmt.Sprintf("%sdata-platform-aws-athena-query-results/s3bigstats", awsregionconsts.RegionPrefix[region])
	out, err := b.athenaClient.StartQueryExecution(&athena.StartQueryExecutionInput{
		QueryExecutionContext: &athena.QueryExecutionContext{
			Database: aws.String("s3inventory"),
		},
		ResultConfiguration: &athena.ResultConfiguration{
			OutputLocation: aws.String(fmt.Sprintf("s3://%s", outputBucket)),
		},
		QueryString: aws.String(athenaQuerySql),
	})
	if err != nil {
		return "", oops.Wrapf(err, "failed execution of athena sql query: %s", athenaQuerySql)
	}

	execId := aws.StringValue(out.QueryExecutionId)
	if err = awshelpers.PollAthenaExecutionState(b.athenaClient, execId); err != nil {
		return "", oops.Wrapf(err, "")
	}
	timeElapsed := samtime.TimeNowInMs() - start
	monitoring.AggregatedDatadog.HistogramValue(float64(timeElapsed), "s3bigstats.backfill.athena.query.duration", datadogdsl.Tag("region", region), datadogdsl.Tag("queryType", "FetchKeys"), datadogdsl.Tag("table", table), datadogdsl.Tag("succcess", fmt.Sprintf("%t", err == nil)))

	slog.Infow(ctx, "Finished querying athena for s3bigstat", "table", table, "execuctionId", execId)
	return execId, nil
}

func (b *S3BigStatBackfiller) queryAthenaForBigStatKeysCount(ctx context.Context, region string, table string) (int, error) {
	slog.Infow(ctx, "Querying athena for s3bigstat count", "table", table)
	// yesterday := time.Now().AddDate(0, -1, 0) // s3inventory files are generated every day, so we want the files from yesterday
	// dateStr := fmt.Sprintf("%s-00-00", yesterday.Format("2006-01-02"))
	athenaQuerySql := fmt.Sprintf(`
	SELECT
	  COUNT(*)
	FROM s3inventory.big_object_stats
	WHERE is_latest = true
	AND is_delete_marker = false
	AND  url_decode(key) LIKE '%%stat_type=%v%%'
	AND dt = '2021-10-14-01-00'
	`, objectstatproto.ObjectStatEnum_value[table])

	start := samtime.TimeNowInMs()
	outputBucket := fmt.Sprintf("%sdata-platform-aws-athena-query-results/s3bigstatscountqueries", awsregionconsts.RegionPrefix[region])
	out, err := b.athenaClient.StartQueryExecution(&athena.StartQueryExecutionInput{
		QueryExecutionContext: &athena.QueryExecutionContext{
			Database: aws.String("s3inventory"),
		},
		ResultConfiguration: &athena.ResultConfiguration{
			OutputLocation: aws.String(fmt.Sprintf("s3://%s", outputBucket)),
		},
		QueryString: aws.String(athenaQuerySql),
	})

	if err != nil {
		return 0, oops.Wrapf(err, "failed execution of athena sql query: %s", athenaQuerySql)
	}
	execId := aws.StringValue(out.QueryExecutionId)
	if err = awshelpers.PollAthenaExecutionState(b.athenaClient, execId); err != nil {
		return 0, oops.Wrapf(err, "")
	}
	timeElapsed := samtime.TimeNowInMs() - start
	monitoring.AggregatedDatadog.HistogramValue(float64(timeElapsed), "s3bigstats.backfill.athena.query.duration", datadogdsl.Tag("table", table), datadogdsl.Tag("region", region), datadogdsl.Tag("queryType", "Count"), datadogdsl.Tag("succcess", fmt.Sprintf("%t", err == nil)))

	slog.Infow(ctx, "Finished querying athena for s3bigstat count", "table", table, "execuctionId", execId)

	slog.Infow(ctx, "Reading file for s3bigstat count", "table", table, "execuctionId", execId)

	exec, err := b.athenaClient.GetQueryExecution(&athena.GetQueryExecutionInput{
		QueryExecutionId: aws.String(execId),
	})
	if err != nil {
		return 0, oops.Wrapf(err, "get exec: %s", execId)
	}

	s3Location := aws.StringValue(exec.QueryExecution.ResultConfiguration.OutputLocation)
	u, err := url.Parse(s3Location)
	if err != nil {
		return 0, oops.Wrapf(err, "parse s3 url: %s", s3Location)
	}

	bucket, key := u.Host, strings.TrimPrefix(u.Path, "/")
	s3Out, err := b.s3Client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, oops.Wrapf(err, "s3 get: s3://%s/%s", bucket, key)
	}
	defer s3Out.Body.Close()
	slog.Infow(ctx, "Finished reading file for s3bigstat count", "table", table, "execuctionId", execId)

	csvReader := csv.NewReader(s3Out.Body)
	records, err := csvReader.ReadAll()
	if err != nil {
		return 0, oops.Wrapf(err, "read csv: s3://%s/%s", bucket, key)
	}
	if len(records) != 2 || len(records[1]) != 1 {
		return 0, oops.Errorf("malformed results from count query table %s, execution id %s", table, execId)
	}

	// The return response looks like `[[_col0] [123]]`` where 123 is the nuber of entires. We want to extract that value
	count, err := strconv.ParseInt(records[1][0], 10, 64)
	if err != nil {
		return 0, oops.Wrapf(err, "Could not parse count for table %s execution id %s", table, execId)
	}
	return int(count), nil
}

// fetchS3KeysAndPushToSqs makes requests to athena to process each page of the sqs queue (up to maxResults size)
// and creates s3 bucket notifications to the sqs queue.
func (b *S3BigStatBackfiller) fetchS3KeysAndPushToSqs(ctx context.Context, region string, athenaClient *athena.Athena, execId string, nextToken string, table string, ddTags []string, processedCount int64) error {
	// These indices match the selection fields in the athena query in `queryAthenaForBigStatKeys`
	bucketIndex := 0
	keyIndex := 1
	sizeIndex := 2
	versionIdIndex := 3

	succesfullyProcessedAllPages := false
	var errorProcessingPage error = nil

	athenaQueryInput := &athena.GetQueryResultsInput{MaxResults: &batchSize, QueryExecutionId: &execId}

	// The token lets us start reading from where we left off from the last request
	if nextToken != "" {
		athenaQueryInput.NextToken = &nextToken
	}

	seenRows := processedCount
	processedRows := processedCount
	publishedRows := processedCount

	err := athenaClient.GetQueryResultsPagesWithContext(ctx, athenaQueryInput,
		func(page *athena.GetQueryResultsOutput, lastPage bool) bool {
			// If we have seen an error, we want to pause the backfill
			if errorProcessingPage != nil {
				return false
			}
			if page != nil {
				if page.ResultSet != nil {
					resultSet := *page.ResultSet
					if resultSet.Rows != nil {
						inputEntries := make([]*sqs.SendMessageBatchRequestEntry, 0, len(resultSet.Rows))
						monitoring.AggregatedDatadog.Count(int64(len(resultSet.Rows)), "s3bigstats.backfill.rows.seen", ddTags...)
						for i, r := range resultSet.Rows {
							seenRows++
							if len(r.Data) != 5 || r.Data[bucketIndex] == nil || r.Data[keyIndex] == nil || r.Data[sizeIndex] == nil || r.Data[versionIdIndex] == nil {
								monitoring.AggregatedDatadog.Incr("s3bigstats.backfill.malformed.row", ddTags...)
								slog.Errorw(ctx, fmt.Errorf("Malformed row seen table %s region %s length %d row %s", table, region, len(r.Data), r), slog.Tag{})
								return false
							}
							s3Bucket := *(r.Data[bucketIndex].VarCharValue)
							key := *(r.Data[keyIndex].VarCharValue)
							sizeString := *(r.Data[sizeIndex].VarCharValue)
							versionId := *(r.Data[versionIdIndex].VarCharValue)

							// This is the first line that corresponds to the columns and not data, we want to ignore it.
							// This only happens for the first entry on the first page of a backfill.
							if s3Bucket == "bucket" && key == "key" && sizeString == "size" && versionId == "version_id" {
								slog.Infow(ctx, "Skipping the header for table region", "table", table, "region", region)
								continue
							}

							size, err := strconv.ParseInt(sizeString, 10, 64)
							if err != nil {
								errorProcessingPage = err
								return false
							}

							sqsnotification, err := json.Marshal(awshelpers.SqsS3NotificationMessage{
								Records: []events.S3EventRecord{
									{
										EventTime: time.Now(),
										S3: events.S3Entity{
											Bucket: events.S3Bucket{
												Name: s3Bucket,
											},
											Object: events.S3Object{
												Key:       key,
												Size:      size,
												VersionID: versionId,
											},
										},
									},
								},
							})
							if err != nil {
								slog.Errorw(ctx, oops.Wrapf(err, "Error marshalling s3 keys into a sqs notification "), slog.Tag{})
								errorProcessingPage = err
								return false
							}
							id := strconv.Itoa(i)
							inputEntries = append(inputEntries, &sqs.SendMessageBatchRequestEntry{
								Id:          aws.String(id),
								MessageBody: aws.String(string(sqsnotification)),
							})
							processedRows++

							if len(inputEntries) == maxSqsBatchSize {
								monitoring.AggregatedDatadog.Count(int64(len(inputEntries)), "s3bigstats.backfill.rows.marshalled", ddTags...)
								err = b.publishSqsMessages(ctx, region, table, inputEntries, ddTags, publishedRows)
								if err != nil {
									slog.Errorw(ctx, err, slog.Tag{})
									errorProcessingPage = err
									return false
								}
								publishedRows += int64(len(inputEntries))
								inputEntries = make([]*sqs.SendMessageBatchRequestEntry, 0, len(resultSet.Rows))
							}
						}
						if len(inputEntries) != 0 {
							monitoring.AggregatedDatadog.Count(int64(len(inputEntries)), "s3bigstats.backfill.rows.marshalled", ddTags...)
							err := b.publishSqsMessages(ctx, region, table, inputEntries, ddTags, publishedRows)
							if err != nil {
								slog.Errorw(ctx, err, slog.Tag{})
								errorProcessingPage = err
								return false
							}
							publishedRows += int64(len(inputEntries))
						}
					}
				}

				// Update next token and push the next token to the dynamo store
				if page.NextToken != nil {
					athenaQueryInput.NextToken = page.NextToken
					_, err := b.dynamoClient.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
						TableName: aws.String(S3BigstatsBackfillStateTableName),
						Key: map[string]*dynamodb.AttributeValue{
							"bigstat_name": {S: aws.String(table)},
						},
						ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
							":t": {
								S: aws.String(*athenaQueryInput.NextToken),
							},
						},
						UpdateExpression: aws.String("set next_token = :t"),
					})
					if err != nil {
						errorProcessingPage = oops.Wrapf(err, "Dynamo in a bad state, could not update next token")
						return false
					}
				}
			}
			// Update the local variable isLastPage so that we can exit when we're done reading the s3 files
			succesfullyProcessedAllPages = lastPage
			monitoring.AggregatedDatadog.Incr("s3bigstats.backfill.batch.processed", ddTags...)

			return !succesfullyProcessedAllPages
		})

	if err != nil {
		err = oops.Wrapf(err, "error running GetQueryResultsPages on athena client")
		ddTagsErr := append(ddTags, "errorType:athenaGetQueryResultsPage", fmt.Sprintf("token:%s", *athenaQueryInput.NextToken))
		datadogutils.IncrInvocation(&err, "s3bigstats.backfill.error", ddTagsErr...)
		return err
	}
	if errorProcessingPage != nil {
		err = oops.Wrapf(errorProcessingPage, "error in athenaClient.GetQueryResultsPages")
		slog.Errorw(ctx, err, slog.Tag{})
		ddTagsErr := append(ddTags, "errorType:athenaGetQueryResultsProcessPage", fmt.Sprintf("token:%s", *athenaQueryInput.NextToken))
		datadogutils.IncrInvocation(&err, "s3bigstats.backfill.error", ddTagsErr...)
		return err
	}

	slog.Infow(ctx, "Finished processing all pages", "seenRows", seenRows, "processedRows", processedRows, "publishedRows", publishedRows)

	// Update the status of the backfill to finished when we have succesfully processed all teh pages
	if succesfullyProcessedAllPages {
		_, err = b.dynamoClient.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(S3BigstatsBackfillStateTableName),
			Key: map[string]*dynamodb.AttributeValue{
				"bigstat_name": {S: aws.String(table)},
			},
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":s": {
					S: aws.String(string(FinishedStatus)),
				},
			},
			UpdateExpression: aws.String("set backfill_status = :s"),
		})
	}

	if err != nil {
		err = oops.Wrapf(err, "Dynamo in a bad state, could not update the status for inprogress to finished for table %s", table)
		ddTagsErr := append(ddTags, "errorType:dynamoUpdateStatusToInFinished")
		datadogutils.IncrInvocation(&err, "s3bigstats.backfill.error", ddTagsErr...)
		return err
	}
	return nil
}

func (b *S3BigStatBackfiller) publishSqsMessages(ctx context.Context, region string, table string, inputEntries []*sqs.SendMessageBatchRequestEntry, ddTags []string, publishedRows int64) error {
	// Create batch sqs message ouput
	queueUrl := sqsdefinitions.SamsaraS3BigStatsJsonBackfillExportQueue.SqsUrl(region)
	input := sqs.SendMessageBatchInput{
		QueueUrl: aws.String(queueUrl),
		Entries:  inputEntries,
	}

	batchResponse, err := b.sqsClient.SendMessageBatchWithContext(ctx, &input)
	if err != nil {
		return err
	}

	if len(batchResponse.Failed) > 0 {
		failures := make([]string, len(batchResponse.Failed))
		for i, fail := range batchResponse.Failed {
			failures[i] = fmt.Sprintf("Failed with response: %s", fail.String())
		}
		return oops.Errorf("Send message encountered failures in response:\n%s", strings.Join(failures, "\n\n"))
	}
	monitoring.AggregatedDatadog.Count(int64(len(inputEntries)), "s3bigstats.backfill.processed", ddTags...)
	err = b.updateCountTrackerDynamo(ctx, region, table, len(inputEntries), ddTags, publishedRows)
	if err != nil {
		return err
	}
	return nil
}

func (b *S3BigStatBackfiller) updateCountTrackerDynamo(ctx context.Context, region string, table string, inputEntriesLength int, ddTags []string, currentCount int64) error {
	newCount := currentCount + int64(inputEntriesLength)
	// We expect every batch size to be 10 except for the batch with the header and the last one. This checks the size of the batch so we only
	// slog the ones where things might have gone wrong
	if newCount-currentCount != int64(maxSqsBatchSize) {
		slog.Infow(ctx, "Increasing proccess count", "currentCount", currentCount, "newCount", newCount, "table", table, "region", region)
	}
	_, err := b.dynamoClient.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(S3BigstatsBackfillStateTableName),
		Key: map[string]*dynamodb.AttributeValue{
			"bigstat_name": {S: aws.String(table)},
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":s": {
				S: aws.String(string(strconv.Itoa(int(newCount)))),
			},
		},
		UpdateExpression: aws.String("set processed_count = :s"),
	})
	if err != nil {
		err = oops.Wrapf(err, "Couldn't update dynamo status currentCount: %d, newCount: %d", currentCount, newCount)
		ddTagsErr := append(ddTags, "errorType:dynamoStatusCountUpdate")
		datadogutils.IncrInvocation(&err, "s3bigstats.backfill.error", ddTagsErr...)
		return err
	}
	return nil
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(newS3BigStatBackfiller)
}
