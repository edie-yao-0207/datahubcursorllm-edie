package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"samsaradev.io/service/dbregistry"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ecs"

	"samsaradev.io/infra/dataplatform/dataplatformhelpers"
	"samsaradev.io/libs/ni/infraconsts"
)

/**
 * Use this command to build the ECS command you will need to kick off a manual run of the flushbinlog tool.
 * Example usage:
 * go run . -region=us-west-2 -dbs="internal,workflows" -dryrun=false -withDummyWrite=true
 * This script can run the command for you, but will also give you the command you can run to invoke it manually.
 */
func main() {
	ctx := context.Background()

	var region, dbs string
	var dryRun, withDummyWrite bool
	flag.StringVar(&region, "region", "us-west-2", "region (us-west-2, eu-west-1 and ca-central-1)")
	flag.StringVar(&dbs, "dbs", "", "comma-separated list of dbs")
	flag.BoolVar(&dryRun, "dryrun", true, "whether this is a dryrun")
	flag.BoolVar(&withDummyWrite, "withDummyWrite", false, "set to true to also write and subsequently delete a row to the schema_version table")
	flag.Parse()

	switch region {
	case infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion:
	default:
		log.Fatalln("Please provide a valid region. Valid regions are us-west-2, eu-west-1 and ca-central-1")
	}

	// Before running the script, let's just check that the dbs provided are all correct.
	var invalidDbs []string
	dbList := strings.Split(strings.TrimSpace(dbs), ",")
	fmt.Printf("%v\n", dbList)
	for _, elem := range dbList {
		if _, ok := dbregistry.DbToTeam[elem]; !ok {
			invalidDbs = append(invalidDbs, elem)
		}
	}

	if len(invalidDbs) > 0 {
		log.Fatalf("DB names must match those in the registry (e.g. 'workflows', not 'workflowsdb'. Heres a list of invalid dbs provided: %s\n", invalidDbs)
	}

	if dryRun && withDummyWrite {
		log.Fatalf("cannot specify both dryrun and withDummyWrite")
	}

	session := dataplatformhelpers.GetAWSSession(region)
	ecsClient := ecs.New(session)
	describeOutput, err := ecsClient.DescribeTaskDefinitionWithContext(ctx, &ecs.DescribeTaskDefinitionInput{
		TaskDefinition: aws.String("flushbinlog"),
	})

	if err != nil {
		log.Fatalln(fmt.Sprintf("failed to get latest version with err %v\n", err))
	}

	regionProfileName := "dataplatformadmin"
	if region == infraconsts.SamsaraAWSEURegion {
		regionProfileName = "eu-dataplatformadmin"
	} else if region == infraconsts.SamsaraAWSCARegion {
		regionProfileName = "ca-dataplatformadmin"
	}

	latestVersion := describeOutput.TaskDefinition.Revision
	if latestVersion == nil {
		log.Fatalln(fmt.Sprintf("latest version was nil, try running AWS_DEFAULT_PROFILE=%s aws ecs describe-task-definition --task-definition flushbinlog | jq and inspect the output.", regionProfileName))
	}

	input := &ecs.RunTaskInput{
		Cluster:              aws.String("prod"),
		Count:                aws.Int64(1),
		EnableECSManagedTags: aws.Bool(true),
		LaunchType:           aws.String("EC2"),
		Overrides: &ecs.TaskOverride{
			ContainerOverrides: []*ecs.ContainerOverride{
				{
					Name: aws.String("main"),
					Command: []*string{
						aws.String("/flushbinlog"),
						aws.String("-dbs"),
						aws.String(dbs),
						aws.String(fmt.Sprintf("-dryrun=%v", dryRun)),
						aws.String(fmt.Sprintf("-withDummyWrite=%v", withDummyWrite)),
					},
				},
			},
		},
		TaskDefinition: describeOutput.TaskDefinition.TaskDefinitionArn,
	}

	manualCommand := fmt.Sprintf(`
AWS_DEFAULT_PROFILE=%s aws ecs run-task --task-definition flushbinlog:%d --cluster prod --count 1 --enable-ecs-managed-tags --launch-type EC2 --overrides '{"containerOverrides": [{"name": "main", "command": ["/flushbinlog", "-dbs", "%s", "-dryrun", "%v", "-withDummyWrite", "%v"]}]}'
`,
		regionProfileName,
		*latestVersion,
		dbs,
		dryRun,
		withDummyWrite)

	fmt.Printf("We're going to run this ecs task with the following input: \n\n %v \n\n", input)
	fmt.Printf("In case you want to run this manually, you can do so with \n%s\n", manualCommand)
	confirm("We will execute this now. Does everything look good?")

	out, err := ecsClient.RunTaskWithContext(ctx, input)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Heres the raw output of the run task command \n%v\n", out)
	if out != nil && len(out.Tasks) > 0 && out.Tasks[0].TaskArn != nil {
		// It'll look like: arn:aws:ecs:us-west-2:781204942244:task/prod/8c2aa9e1c9f1434ab5dd94db15a27d47
		// We'll cut out that last part to link to the details page, which is otherwise very hard to find
		taskArn := *out.Tasks[0].TaskArn
		splitArn := strings.Split(taskArn, "/")
		taskId := splitArn[len(splitArn)-1]
		link := fmt.Sprintf("https://us-west-2.console.aws.amazon.com/ecs/home?region=us-west-2#/clusters/prod/tasks/%s/details", taskId)
		if region == infraconsts.SamsaraAWSEURegion {
			link = fmt.Sprintf("https://eu-west-1.console.aws.amazon.com/ecs/home?region=eu-west-1#/clusters/prod/tasks/%s/details", taskId)
		} else if region == infraconsts.SamsaraAWSCARegion {
			link = fmt.Sprintf("https://ca-central-1.console.aws.amazon.com/ecs/home?region=ca-central-1#/clusters/prod/tasks/%s/details", taskId)
		}
		fmt.Printf("For task arn %s, you can get to the details page here:\n%s\n", taskArn, link)

	} else {
		fmt.Printf("Couldn't figure out the task arn from the output, check cloudwatch or the manual output and figure out how to monitor this\n")
	}

}

func confirm(text string) {
	var response string
	fmt.Print(text + " Confirm by typing 'yes': ")
	_, err := fmt.Scanln(&response)
	if err != nil {
		log.Fatal(err)
	}

	if strings.ToLower(response) != "yes" {
		log.Println("Failed to confirm, exiting.")
		os.Exit(0)
	}
}
