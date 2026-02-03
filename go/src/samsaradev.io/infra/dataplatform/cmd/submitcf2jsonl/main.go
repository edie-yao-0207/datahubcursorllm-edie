package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/batch"
	"github.com/cznic/mathutil"
	"go.uber.org/fx"

	"samsaradev.io/hubproto/objectstatproto"
	"samsaradev.io/infra/config"
	"samsaradev.io/models"
	"samsaradev.io/system"
)

func chunkInt64Slice(slice []int64, chunkSize int) [][]int64 {
	var chunks [][]int64
	for start := 0; start < len(slice); start += chunkSize {
		end := mathutil.Min(start+chunkSize, len(slice))
		chunks = append(chunks, slice[start:end])
	}
	return chunks
}

func main() {
	var orgIdsFlag string
	var productIdsFlag string
	var startStr, endStr string
	var kind string
	var objectTypeStr string
	var statTypeStr string

	flag.StringVar(&orgIdsFlag, "orgs", "17317", "comma separated org id list")
	flag.StringVar(&productIdsFlag, "products", "", "comma separated product id list")
	flag.StringVar(&startStr, "start", "2019-07-01T00:00:00", "")
	flag.StringVar(&endStr, "end", "2019-08-01T00:00:00", "")
	flag.StringVar(&kind, "kind", "objectstat", "location or objectstat")
	flag.StringVar(&objectTypeStr, "objecttype", "otDevice", "objectType to read; used for kind=objectstat")
	flag.StringVar(&statTypeStr, "stattype", "osDSpeedLimitValueDetected", "statType to read; used for kind=objectstat")
	flag.Parse()

	var appModelsInput models.ReplicaAppModelsInput
	app := system.NewFx(&config.ConfigParams{}, fx.Populate(&appModelsInput))
	if err := app.Start(context.Background()); err != nil {
		log.Fatalln(err)
	}
	defer app.Stop(context.Background())

	// List organizations.
	orgRangeRegexp := regexp.MustCompile(`(\d+)\-(\d+)`)
	var orgIds []int64
	for _, part := range strings.Split(orgIdsFlag, ",") {
		matches := orgRangeRegexp.FindAllStringSubmatch(part, -1)
		if len(matches) > 0 {
			start, err := strconv.Atoi(matches[0][1])
			if err != nil {
				log.Fatalln(err)
			}
			end, err := strconv.Atoi(matches[0][2])
			if err != nil {
				log.Fatalln(err)
			}

			for orgId := start; orgId <= end; orgId++ {
				orgIds = append(orgIds, int64(orgId))
			}
			continue
		}

		orgId, err := strconv.Atoi(part)
		if err != nil {
			log.Fatalf("%s is not int64", part)
		}
		if orgId == -1 {
			orgs, err := appModelsInput.Models.Organization.All(context.Background())
			if err != nil {
				log.Fatalln(err)
			}
			for _, org := range orgs {
				orgIds = append(orgIds, org.Id)
			}
			break
		}
		orgIds = append(orgIds, int64(orgId))
	}

	// Parse start and end time.
	const timeFormatWithoutTZ = "2006-01-02T15:04:05"
	startTime, err := time.Parse(timeFormatWithoutTZ, startStr)
	if err != nil {
		log.Fatalln(err)
	}
	endTime, err := time.Parse(timeFormatWithoutTZ, endStr)
	if err != nil {
		log.Fatalln(err)
	}

	// Validate stat kind.
	switch kind {
	case "location":
	case "objectstat":
	default:
		log.Fatalf("unknown kind: %s", kind)
	}

	// Parse object type.
	var objectType objectstatproto.ObjectTypeEnum
	switch objectTypeStr {
	case "otDevice":
		objectType = objectstatproto.ObjectTypeEnum_otDevice
	case "otWidget":
		objectType = objectstatproto.ObjectTypeEnum_otWidget
	default:
		log.Fatalf("unknown object type: %s", objectTypeStr)
	}

	// Parse stat type.
	statTypeNum, ok := objectstatproto.ObjectStatEnum_value[statTypeStr]
	if kind == "objectstat" && !ok {
		log.Fatalf("unknown stat type: %s", statTypeStr)
	}
	statType := objectstatproto.ObjectStatEnum(statTypeNum)

	batchClient := batch.New(session.Must(session.NewSessionWithOptions(session.Options{})))

	jobDefOutput, err := batchClient.DescribeJobDefinitions(&batch.DescribeJobDefinitionsInput{
		JobDefinitionName: aws.String("batch-cf2jsonl"),
		Status:            aws.String("ACTIVE"),
	})
	if err != nil {
		log.Fatalln(err)
	}

	for _, orgIds := range chunkInt64Slice(orgIds, 50) {
		var orgIdStrings []string
		for _, orgId := range orgIds {
			orgIdStrings = append(orgIdStrings, fmt.Sprint(orgId))
		}
		cmd := []string{"/cf2jsonl",
			"-orgs", strings.Join(orgIdStrings, ","),
			"-start", startTime.Format(timeFormatWithoutTZ),
			"-end", endTime.Format(timeFormatWithoutTZ),
			"-kind", kind,
		}
		if productIdsFlag != "" {
			cmd = append(cmd, "-products", productIdsFlag)
		}

		if kind == "objectstat" {
			cmd = append(cmd, "-objecttype", objectType.String(), "-stattype", statType.String())
		}
		// name := regexp.MustCompile(`[^a-zA-Z0-9-_]`).ReplaceAllString(
		// 	strings.Replace(strings.Join(cmd, "_"), ",", "-", -1), "")
		table := kind
		if kind == "objectstat" {
			table = statType.String()
		}
		name := fmt.Sprintf("cf2jsonl_%s_%s_%s_%s", table, startTime.Format("2006-01-02"), endTime.Format("2006-01-02"), strings.Join(orgIdStrings, "-"))
		maxLen := len(name)
		if maxLen > 128 {
			maxLen = 128
		}
		name = name[:maxLen]

		out, err := batchClient.SubmitJob(&batch.SubmitJobInput{
			JobQueue:      aws.String(fmt.Sprintf("cf2jsonl-%s", table)),
			JobName:       aws.String(name),
			JobDefinition: jobDefOutput.JobDefinitions[0].JobDefinitionArn,
			ContainerOverrides: &batch.ContainerOverrides{
				Command: aws.StringSlice(cmd),
				Vcpus:   aws.Int64(4),
				Memory:  aws.Int64(7168),
				Environment: []*batch.KeyValuePair{
					&batch.KeyValuePair{
						Name:  aws.String("ECS_CLUSTERNAME_OVERRIDE"),
						Value: aws.String("prod"),
					},
				},
			},
			RetryStrategy: &batch.RetryStrategy{
				Attempts: aws.Int64(3),
			},
		})
		if err != nil {
			log.Fatalln(err)
		}
		log.Println(aws.StringValue(out.JobId), aws.StringValue(out.JobName))
	}
}
