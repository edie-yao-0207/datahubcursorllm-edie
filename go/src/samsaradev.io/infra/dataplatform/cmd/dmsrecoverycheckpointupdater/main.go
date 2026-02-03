package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ecs"

	"samsaradev.io/infra/dataplatform/dataplatformhelpers"
	"samsaradev.io/libs/ni/infraconsts"
)

/**
 * Use this command to build the ECS command you will need to kick off a manual run of the dmsrecoverycheckpointupdater tool.
 * Example usage:
 * go run . -region=us-west-2
 * This script can run the command for you, but will also give you the command you can run to invoke it manually.
 */
func main() {
	ctx := context.Background()

	var region string
	flag.StringVar(&region, "region", "us-west-2", "region (us-west-2 or eu-west-1)")
	flag.Parse()

	switch region {
	case infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion:
	default:
		log.Fatalln("Please provide a valid region. Valid regions are us-west-2, eu-west-1 and ca-central-1")
	}

	session := dataplatformhelpers.GetAWSSession(region)
	ecsClient := ecs.New(session)
	describeOutput, err := ecsClient.DescribeTaskDefinitionWithContext(ctx, &ecs.DescribeTaskDefinitionInput{
		TaskDefinition: aws.String("dmsrecoverycheckpointupdater"),
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
		log.Fatalln(fmt.Sprintf("latest version was nil, try running AWS_DEFAULT_PROFILE=%s aws ecs describe-task-definition --task-definition dmsrecoverycheckpointupdater | jq and inspect the output.", regionProfileName))
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
						aws.String("/dmsrecoverycheckpointupdater"),
					},
				},
			},
		},
		TaskDefinition: describeOutput.TaskDefinition.TaskDefinitionArn,
	}

	manualCommand := fmt.Sprintf(`
AWS_DEFAULT_PROFILE=%s aws ecs run-task --task-definition dmsrecoverycheckpointupdater:%d --cluster prod --count 1 --enable-ecs-managed-tags --launch-type EC2 --overrides '{"containerOverrides": [{"name": "main", "command": ["/dmsrecoverycheckpointupdater"]}]}'
`,
		regionProfileName,
		*latestVersion)

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
