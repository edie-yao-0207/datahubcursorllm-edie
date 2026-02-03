package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ecs"

	"samsaradev.io/infra/dataplatform/dataplatformhelpers"
	"samsaradev.io/infra/samsaraaws/awshelpers"
	"samsaradev.io/libs/ni/infraconsts"
)

/**
 * Use this command to build the ECS command you will need to kick off a manual run of the dataplatformtestinternaldbuploader tool.
 * Example usage:
 * go run . -region=us-west-2 -num=10 -tableName=json_notes
 * This script can run the command for you, but will also give you the command you can run to invoke it manually.
 */
func main() {
	ctx := context.Background()

	var region string
	var num int
	var tableName string
	flag.StringVar(&region, "region", "us-west-2", "region (us-west-2 or eu-west-1 or ca-central-1)")
	flag.IntVar(&num, "num", 0, "number of rows to insert")
	flag.StringVar(&tableName, "tableName", "notes", "name of table")
	flag.Parse()

	switch region {
	case infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion:
	default:
		log.Fatalln("Please provide a valid region. Valid regions are us-west-2, eu-west-1 and ca-central-1")
	}

	session := dataplatformhelpers.GetAWSSession(region)
	ecsClient := ecs.New(session)
	describeOutput, err := ecsClient.DescribeTaskDefinitionWithContext(ctx, &ecs.DescribeTaskDefinitionInput{
		TaskDefinition: aws.String("dataplatformtestinternaldbuploader"),
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
		log.Fatalln(fmt.Sprintf("latest version was nil, try running AWS_DEFAULT_PROFILE=%s aws ecs describe-task-definition --task-definition dataplatformtestinternaldbuploader | jq and inspect the output.", regionProfileName))
	}

	// Get network configuration for Fargate tasks (awsvpc mode)
	networkConfig, err := awshelpers.GetNetworkConfigurationFromReference(ctx, ecsClient, "prod")
	if err != nil {
		log.Fatalln(fmt.Sprintf("failed to get network configuration: %v", err))
	}

	input := &ecs.RunTaskInput{
		Cluster:              aws.String("prod"),
		Count:                aws.Int64(1),
		EnableECSManagedTags: aws.Bool(true),
		LaunchType:           aws.String("FARGATE"),
		NetworkConfiguration: networkConfig,
		Overrides: &ecs.TaskOverride{
			ContainerOverrides: []*ecs.ContainerOverride{
				{
					Name: aws.String("main"),
					Command: []*string{
						aws.String("/serviceapp/dataplatformtestinternaldbuploader"),
						aws.String(fmt.Sprintf("-num=%v", num)),
						aws.String(fmt.Sprintf("-tableName=%v", tableName)),
					},
				},
			},
		},
		TaskDefinition: describeOutput.TaskDefinition.TaskDefinitionArn,
	}

	manualCommand := fmt.Sprintf(`
AWS_DEFAULT_PROFILE=%s aws ecs run-task --task-definition dataplatformtestinternaldbuploader:%d --cluster prod --count 1 --enable-ecs-managed-tags --launch-type FARGATE --network-configuration awsvpcConfiguration={subnets=[...],securityGroups=[...],assignPublicIp=DISABLED} --overrides '{"containerOverrides": [{"name": "main", "command": ["/dataplatformtestinternaldbuploader", "-num", %v, "-tableName", %v]}]}'
`,
		regionProfileName,
		*latestVersion,
		num, tableName)

	fmt.Printf("We're going to run this ecs task with the following input: \n\n %v \n\n", input)
	fmt.Printf("In case you want to run this manually, you can do so with \n%s\n", manualCommand)
	confirm("We will execute this now. Does everything look good?")

	out, err := ecsClient.RunTaskWithContext(ctx, input)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Heres the raw output of the run task command \n%v\n", out)
	if out == nil || len(out.Tasks) == 0 || out.Tasks[0].TaskArn == nil {
		log.Fatalln("Couldn't get task ARN from the output, check cloudwatch or the manual output")
	}

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

	// Wait for task to stop and check exit code
	fmt.Println("Waiting for task to complete...")
	exitCode, err := waitForTaskAndGetExitCode(ctx, ecsClient, taskArn)
	if err != nil {
		log.Fatalln(fmt.Sprintf("failed to wait for task completion: %v", err))
	}

	if exitCode == 0 {
		fmt.Printf("Task completed successfully with exit code %d\n", exitCode)
	} else {
		fmt.Printf("Task completed with exit code %d (non-zero indicates failure)\n", exitCode)
		os.Exit(int(exitCode))
	}
}

// waitForTaskAndGetExitCode waits for the task to stop and returns the exit code of the main container
func waitForTaskAndGetExitCode(ctx context.Context, ecsClient *ecs.ECS, taskArn string) (int64, error) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	timeout := 5 * time.Minute
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return -1, ctx.Err()
		case <-ticker.C:
			describeInput := &ecs.DescribeTasksInput{
				Cluster: aws.String("prod"),
				Tasks:   []*string{aws.String(taskArn)},
			}

			describeOutput, err := ecsClient.DescribeTasksWithContext(ctx, describeInput)
			if err != nil {
				return -1, fmt.Errorf("failed to describe task: %v", err)
			}

			if len(describeOutput.Tasks) == 0 {
				return -1, fmt.Errorf("task %s not found", taskArn)
			}

			task := describeOutput.Tasks[0]
			lastStatus := aws.StringValue(task.LastStatus)

			if lastStatus == "STOPPED" {
				// Find the main container and get its exit code
				for _, container := range task.Containers {
					if container.Name != nil && *container.Name == "main" {
						if container.ExitCode != nil {
							return aws.Int64Value(container.ExitCode), nil
						}
						return -1, errors.New("main container exit code not available")
					}
				}
				return -1, errors.New("main container not found in task")
			}

			fmt.Printf("Task status: %s (waiting for STOPPED...)\n", lastStatus)
		}
	}

	return -1, fmt.Errorf("timeout waiting for task %s to stop", taskArn)
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
