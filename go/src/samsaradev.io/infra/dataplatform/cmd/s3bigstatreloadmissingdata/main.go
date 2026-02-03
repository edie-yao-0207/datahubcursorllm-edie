package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/slog"
	"samsaradev.io/hubproto/objectstatproto"
	"samsaradev.io/infra/dataplatform/lambdafunctions/util"
	"samsaradev.io/infra/samsaraaws/awshelpers"
	"samsaradev.io/infra/samsaraaws/awshelpers/awssessions"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/service/components/sqsregistry/sqsdefinitions"
)

var maxSqsBatchSize int = 10

func publishSqsMessages(ctx context.Context, sqsClient *sqs.SQS, region string, inputEntries []*sqs.SendMessageBatchRequestEntry) error {
	// Create batch sqs message ouput
	queueUrl := sqsdefinitions.SamsaraS3BigStatsJsonBackfillExportQueue.SqsUrl(region)
	input := sqs.SendMessageBatchInput{
		QueueUrl: aws.String(queueUrl),
		Entries:  inputEntries,
	}

	batchResponse, err := sqsClient.SendMessageBatchWithContext(ctx, &input)
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
	return nil
}

func parseFile(ctx context.Context, region string, wg *sync.WaitGroup, sqsClient *sqs.SQS, fileName string, lastState map[string]int64, mu *sync.Mutex) {
	defer wg.Done()

	fmt.Println("Starting parsing csv file: ", fileName[0:10])

	filePath := fmt.Sprintf("$BACKEND_ROOT/go/src/samsaradev.io/infra/dataplatform/cmd/s3bigstatreloadmissingdata/%s", fileName)
	csvfile, err := os.Open(filePath)
	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}
	r := csv.NewReader(csvfile)
	filesCount := 0
	inputEntries := make([]*sqs.SendMessageBatchRequestEntry, 0, maxSqsBatchSize)
	for {
		records, readErr := r.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			log.Fatal(readErr)
		}

		// Skip the header
		if records[0] == "filename" && records[1] == "receipt_handle" && records[2] == "added_at" {
			continue
		}
		filesCount++
		// Example input: s3://samsara-s3bigstats-json-export/osDAccelerometer/date=2021-06-07/29794-otDevice-281474977947665-1623088664183.json.gz
		// Example output: #object_id=281474977947665#object_type=otDevice#org=29794#stat_type=184/1623088664183.json.gz.gzipproto
		s3Url := strings.Split(records[0], "/")
		objectStatName := s3Url[3]
		s3UrlKey := strings.Split(s3Url[5], "-")
		orgId := s3UrlKey[0]
		objectType := s3UrlKey[1]
		objectId := s3UrlKey[2]

		timestamp := strings.Split(s3UrlKey[3], ".json.gz")[0]
		objectStatId := fmt.Sprint(objectstatproto.ObjectStatEnum_value[objectStatName])
		key := fmt.Sprintf("#object_id=%s#object_type=%s#org=%s#stat_type=%s/%s.gzipproto", objectId, objectType, orgId, objectStatId, timestamp)

		s3BucketName := "samsara-big-object-stats"
		if region == infraconsts.SamsaraAWSEURegion {
			s3BucketName = "samsara-eu-big-object-stats"
		} else if region == infraconsts.SamsaraAWSCARegion {
			s3BucketName = "samsara-ca-big-object-stats"
		}
		sqsnotification, err := json.Marshal(awshelpers.SqsS3NotificationMessage{
			Records: []events.S3EventRecord{
				{
					EventTime: time.Now(),
					S3: events.S3Entity{
						Bucket: events.S3Bucket{
							Name: s3BucketName,
						},
						Object: events.S3Object{
							Key: key,
							// version id and size not specified
						},
					},
				},
			},
		})
		if err != nil {
			slog.Errorw(ctx, oops.Wrapf(err, "Error marshalling s3 keys into a sqs notification "), slog.Tag{})
			break
		}

		id := strconv.Itoa(filesCount)
		inputEntries = append(inputEntries, &sqs.SendMessageBatchRequestEntry{
			Id:          aws.String(id),
			MessageBody: aws.String(string(sqsnotification)),
		})

		if len(inputEntries) == maxSqsBatchSize {
			if filesCount%10000 == 0 {
				fmt.Println(fmt.Sprintf("Parsed %d files in file %s", filesCount, fileName[:10]))
			}
			mu.Lock()
			shouldPublishToSqs := false
			if lastState[fileName[:10]] < int64(filesCount) {
				lastState[fileName[:10]] = int64(filesCount)
				shouldPublishToSqs = true
			}
			mu.Unlock()

			if shouldPublishToSqs {
				fmt.Println(fileName[:10], " ", filesCount)
				err = publishSqsMessages(ctx, sqsClient, region, inputEntries)
				if err != nil {
					fmt.Println("Error publishing sqs messgaes", err)
					break
				}
			}
			inputEntries = make([]*sqs.SendMessageBatchRequestEntry, 0, maxSqsBatchSize)
		}

	}
	if len(inputEntries) > 0 {
		if filesCount%10000 == 0 {
			fmt.Println(fmt.Sprintf("Parsed %d files in file %s", filesCount, fileName[:10]))
		}
		mu.Lock()
		shouldPublishToSqs := false
		if lastState[fileName[:10]] < int64(filesCount) {
			lastState[fileName[:10]] = int64(filesCount)
			shouldPublishToSqs = true
		}

		mu.Unlock()
		if shouldPublishToSqs {
			fmt.Println(fileName[:10], " ", filesCount)
			err = publishSqsMessages(ctx, sqsClient, region, inputEntries)
			if err != nil {
				fmt.Println("Error publishing sqs messgaes", err)
			}
		}
		if err != nil {
			fmt.Println("Error publishing sqs messages", err)
		}
	}

	fmt.Println(fmt.Sprintf("Finished parsing %d files for fileName %s", int64(filesCount), fileName[:10]))
}

// This script is used to read csv files downloaded for s3files
// and create sqs bucket notificactions for all the files, allowing them
// to be reingested by the s3bigstats pipeline.
// To run download the s3 files locally in this folder and run the script
// To get the s3 files, download the files from `ss3://samsara-s3bigstats-delta-lake/s3files/<sbigstat>`
func main() {
	var sqsClient = sqs.New(session.New(util.RetryerConfig))
	awsSession := awssessions.NewInstrumentedAWSSession()
	region := *awsSession.Config.Region
	ctx := context.Background()

	// If the task times out due to credential issues, it prints out its last state in the logs
	// Copy the results into last state so it can continue where it left off.
	var lastState = map[string]int64{}

	files, err := ioutil.ReadDir(".")
	if err != nil {
		log.Fatal(err)
	}

	fileNames := make([]string, 0, len(files))
	for _, f := range files {
		if strings.Contains(f.Name(), ".csv") {
			name := f.Name()
			fileNames = append(fileNames, name)
		}

	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, f := range fileNames {
		wg.Add(1)
		go parseFile(ctx, region, &wg, sqsClient, f, lastState, &mu)
	}
	wg.Wait()
	fmt.Println("Completed parsing all files, here is the final state")
	// Sometimes the script times out credentials. This prints out the state so we can copy it in
	// into last state and use it to continue where we left off
	for key, val := range lastState {
		fmt.Println(fmt.Sprintf("\"%s\":%d", key, val))
	}

}
