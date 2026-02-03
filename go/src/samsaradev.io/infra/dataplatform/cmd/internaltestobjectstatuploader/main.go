package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ecs"

	"samsaradev.io/infra/dataplatform/dataplatformhelpers"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/slicehelpers"
)

var validRegions = slicehelpers.Map(infraconsts.SamsaraProdCloudsAsSlice, func(cloud infraconsts.SamsaraCloud) string {
	return cloud.AWSRegion
})

/**
 * Use this command to build the ECS command you will need to kick off a manual run of the internaltestobjectstatuploader tool.
 * Example usage:
 * go run . -region=us-west-2 -dryrun=false -num=10
 * This script can run the command for you, but will also give you the command you can run to invoke it manually.
 */
func main() {
	ctx := context.Background()

	var region string
	var dryRun bool
	var num int
	flag.StringVar(&region, "region", "us-west-2", strings.Join(validRegions, " or "))
	flag.BoolVar(&dryRun, "dryrun", true, "whether this is a dryrun")
	flag.IntVar(&num, "num", 0, "number of stats to upload")
	flag.Parse()

	if !slices.Contains(validRegions, region) {
		log.Fatalf("invalid region: %q", region)
	}

	session := dataplatformhelpers.GetAWSSession(region)
	ecsClient := ecs.New(session)
	describeOutput, err := ecsClient.DescribeTaskDefinitionWithContext(ctx, &ecs.DescribeTaskDefinitionInput{
		TaskDefinition: aws.String("internaltestobjectstatuploader"),
	})

	if err != nil {
		log.Fatalln(fmt.Sprintf("failed to get latest version with err %v\n", err))
	}

	regionProfileName := infraconsts.GetProdCloudByRegion(region).SSORolePrefix + "dataplatformadmin"

	latestVersion := describeOutput.TaskDefinition.Revision
	if latestVersion == nil {
		log.Fatalln(fmt.Sprintf("latest version was nil, try running AWS_DEFAULT_PROFILE=%s aws ecs describe-task-definition --task-definition internaltestobjectstatuploader | jq and inspect the output.", regionProfileName))
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
						aws.String("/serviceapp/internaltestobjectstatuploader"),
						aws.String(fmt.Sprintf("-dryrun=%v", dryRun)),
						aws.String(fmt.Sprintf("-num=%v", num)),
					},
				},
			},
		},
		TaskDefinition: describeOutput.TaskDefinition.TaskDefinitionArn,
	}

	manualCommand := fmt.Sprintf(`
AWS_DEFAULT_PROFILE=%s aws ecs run-task --task-definition internaltestobjectstatuploader:%d --cluster prod --count 1 --enable-ecs-managed-tags --launch-type EC2 --overrides '{"containerOverrides": [{"name": "main", "command": ["/serviceapp/internaltestobjectstatuploader" "-dryrun", "%v", "-num", %v]}]}'
`,
		regionProfileName,
		*latestVersion,
		dryRun,
		num)

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
