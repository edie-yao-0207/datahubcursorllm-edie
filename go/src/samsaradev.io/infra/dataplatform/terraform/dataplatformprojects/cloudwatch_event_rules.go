package dataplatformprojects

import (
	"strings"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/team"
)

type AWSEventRule struct {
	Name         string
	Description  string
	EventPattern *policy.AWSEventPattern
	Tags         map[string]string

	CloudWatchLogGroupSuffix string
}

func (r *AWSEventRule) Resources() []tf.Resource {
	var resources []tf.Resource
	eventRule := &awsresource.CloudWatchEventRule{
		Name:         r.Name,
		Description:  r.Description,
		EventPattern: r.EventPattern,
		IsEnabled:    true,
		Tags:         r.Tags,
	}
	resources = append(resources, eventRule)

	if r.CloudWatchLogGroupSuffix != "" {
		logGroup := &awsresource.CloudwatchLogGroup{
			ResourceName:    r.CloudWatchLogGroupSuffix,
			Name:            "/aws/events/" + r.CloudWatchLogGroupSuffix,
			RetentionInDays: 731,
			Tags:            r.Tags,
		}
		resources = append(resources, logGroup)
		eventTarget := &awsresource.CloudWatchEventTarget{
			Rule: eventRule.Name,
			Arn:  logGroup.ResourceId().ReferenceAttr("arn"),
		}
		eventTarget = eventTarget.WithContext(tf.Context{
			Detail: eventRule.Name + "-cloudwatch-" + r.CloudWatchLogGroupSuffix,
		})
		resources = append(resources, eventTarget)
	}
	return resources
}

func CloudwatchEventRules() map[string][]tf.Resource {
	rules := []AWSEventRule{
		{
			// Create a cloudwatch rule that takes EC2 spot interruption warnings and puts
			// them into a cloudwatch log group. From this metric we can sum up and see
			// how frequently spot instances are being interrupted.
			Name:        "log-ec2-spot-interruption-events",
			Description: "Send ec2 spot interruption events to a cloudwatch log group to help us monitor spot interruptions.",
			EventPattern: &policy.AWSEventPattern{
				Source:     []string{"aws.ec2"},
				DetailType: []string{"EC2 Spot Instance Interruption Warning"},
			},
			CloudWatchLogGroupSuffix: "ec2-log-spot-interruption",
			Tags: map[string]string{
				"samsara:service":       "ec2-log-spot-interruption",
				"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
				"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
			},
		},
		{
			// Log EC2 Fleet events, which includes errors when an EC2 Fleet cannot acquire
			// any instance based the specified AZs and instance types. Because Databricks
			// will simply timeout with no helpful errors when it cannot acquire machines,
			// this logging will give us additional insight into why these errors are
			// occurring.
			// See more in https://samsara-rd.slack.com/archives/G010UNRQGFN/p1654657125372979.
			Name:        "log-ec2-fleet-events",
			Description: "Log EC2 Fleet events in CloudWatch",
			EventPattern: &policy.AWSEventPattern{
				Source: []string{"aws.ec2fleet"},
			},
			CloudWatchLogGroupSuffix: "ec2-fleet-events",
			Tags: map[string]string{
				"samsara:service":       "ec2-fleet-events",
				"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
				"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
			},
		},
	}

	var resources []tf.Resource
	for _, rule := range rules {
		resources = append(resources, rule.Resources()...)
	}
	return map[string][]tf.Resource{
		"cloudwatch_event_rules": resources,
	}
}
