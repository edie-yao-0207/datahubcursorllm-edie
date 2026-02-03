// Basic helper functions for common, repeated aws function inputs
package util

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/databasemigrationservice"
)

func DescribeConnectionsInput(endpointArn *string) *databasemigrationservice.DescribeConnectionsInput {
	return &databasemigrationservice.DescribeConnectionsInput{
		Filters: []*databasemigrationservice.Filter{
			&databasemigrationservice.Filter{
				Name:   aws.String("endpoint-arn"),
				Values: []*string{endpointArn},
			},
		},
	}
}

func DescribeEndpointsInput(endpointArn *string) *databasemigrationservice.DescribeEndpointsInput {
	return &databasemigrationservice.DescribeEndpointsInput{
		Filters: []*databasemigrationservice.Filter{
			&databasemigrationservice.Filter{
				Name:   aws.String("endpoint-arn"),
				Values: []*string{endpointArn},
			},
		},
	}
}

func DescribeReplicationTasksInput(taskArn *string) *databasemigrationservice.DescribeReplicationTasksInput {
	return &databasemigrationservice.DescribeReplicationTasksInput{
		Filters: []*databasemigrationservice.Filter{
			&databasemigrationservice.Filter{
				Name:   aws.String("replication-task-arn"),
				Values: []*string{taskArn},
			},
		},
	}
}
