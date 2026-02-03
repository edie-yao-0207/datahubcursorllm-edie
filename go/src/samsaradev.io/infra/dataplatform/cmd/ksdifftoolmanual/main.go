package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ecs"

	"samsaradev.io/hubproto/objectstatproto"
	"samsaradev.io/infra/dataplatform/dataplatformhelpers"
	"samsaradev.io/libs/ni/infraconsts"
)

/**
 * Use this command to build the ECS command you will need to kick off a manual run of the ksdifftool.
 * Example usage:
 * go run . --start_date="2021-01-01" --end_date="2021-01-02" --random_sample_count="100" --stats="osDDerivedGpsDistance,osDAccelerometer" --prefix="parth_test"
 * This script will NOT run the command for you, it will only print it out so you can run it.
 */
func main() {
	ctx := context.Background()

	var region string
	var startDate string
	var endDate string
	var randomSampleCount int64
	var stats string
	var prefix string
	flag.StringVar(&region, "region", "", "region (us-west-2, eu-west-1, or ca-central-1. currently only works for us-west-2)")
	flag.StringVar(&startDate, "start_date", "", "start date (inclusive)")
	flag.StringVar(&endDate, "end_date", "", "end date (exclusive)")
	flag.Int64Var(&randomSampleCount, "random_sample_count", 0, "random sample count")
	flag.StringVar(&stats, "stats", "", "list of comma separated stats, with correct capitalization, e.g. osDGpsDistance,osDAccelerometer")
	flag.StringVar(&prefix, "prefix", "", "prefix that should be used")
	flag.Parse()

	// Before doing any work, let's make sure the inputs are valid
	if startDate == "" || endDate == "" {
		log.Fatalln("Please provide valid start and end dates.")
	}
	if randomSampleCount <= 0 {
		log.Fatalln("Please provide a positive random sample count")
	}
	if stats == "" {
		log.Fatalln("Please provide a nonempty set of stats")
	}

	var unknownStats []string
	statsList := strings.Split(stats, ",")
	for _, stat := range statsList {
		if _, ok := objectstatproto.ObjectStatEnum_value[stat]; !ok {
			unknownStats = append(unknownStats, stat)
		}
	}
	if len(unknownStats) > 0 {
		log.Fatalf("Please check these stats exist and are capitalized correctly %v\n", unknownStats)
	}

	if prefix == "" {
		log.Fatalln("Please provide a prefix.")
	}

	switch region {
	case infraconsts.SamsaraAWSDefaultRegion:
	case infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion:
		log.Fatalln("The diff tool currently doesn't work in the EU or CA.")
	default:
		log.Fatalln("Please provide a valid region. Only valid region currently is us-west-2.")
	}

	session := dataplatformhelpers.GetAWSSession(region)
	ecsClient := ecs.New(session)
	describeOutput, err := ecsClient.DescribeTaskDefinitionWithContext(ctx, &ecs.DescribeTaskDefinitionInput{
		TaskDefinition: aws.String("kinesisstatsdeltalakedifftoolmanual"),
	})

	if err != nil {
		log.Fatalln(fmt.Sprintf("failed to get latest version with err %v\n", err))
	}

	latestVersion := describeOutput.TaskDefinition.Revision
	if latestVersion == nil {
		log.Fatalln("latest version was nil, try running AWS_DEFAULT_PROFILE=dataplatformadmin aws ecs describe-task-definition --task-definition kinesisstatsdeltalakedifftoolmanual | jq and inspect the output.")
	}

	cmd := fmt.Sprintf(`
AWS_DEFAULT_PROFILE=dataplatformadmin aws ecs run-task --task-definition kinesisstatsdeltalakedifftoolmanual:%d --cluster prod --count 1 --enable-ecs-managed-tags --launch-type EC2 --network-configuration '{"awsvpcConfiguration": {"subnets": ["subnet-0cb53f66b5ed0428e","subnet-057ff9b0e33610aca","subnet-0be8fad3d2006444c"],"securityGroups": ["%s"],"assignPublicIp": "DISABLED"}}'  --overrides '{"containerOverrides": [{"name": "main", "command": ["/kinesisstatsdeltalakedifftoolmanual", "-startDate", "%s", "-endDate", "%s", "-randomSampleCount", "%d", "-stats", "%s", "-prefix", "%s"]}]}'
`,
		*latestVersion,
		infraconsts.USProdECSInstancesSGID,
		startDate,
		endDate,
		randomSampleCount,
		stats,
		prefix)

	fmt.Printf("Here's the command\n\n %s \n\n", cmd)
}
