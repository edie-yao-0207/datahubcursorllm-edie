package databricksaccount

import (
	"fmt"
	"slices"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/dataplatform/unitycatalog"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/team"
)

// Creates AWS resources that are related to account-level databricks resources. For example, IAM roles
// related to storage credentials.
func DatabricksAccountAwsProject(providerGroup string) (*project.Project, error) {
	p := &project.Project{
		RootTeam:       dataplatformterraformconsts.DataPlatformOfficialDatabricksProviderTerraformProjectPipeline,
		Provider:       providerGroup,
		Class:          "unitycatalog",
		ResourceGroups: map[string][]tf.Resource{},
	}
	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
		"aws_provider": resource.ProjectAWSProvider(p),
		"tf_backend":   resource.ProjectTerraformBackend(p),
	})

	resources, err := buildStorageLocationIams(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "building storage location iams")
	}

	cloudCredentials, err := buildCloudCredentialIamRoles(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "building cloud credential iams")
	}

	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
		"storagelocations":  resources,
		"cloud_credentials": cloudCredentials,
	})

	return p, nil
}

// Build the necessary IAM roles for storage credentials.
// https://docs.databricks.com/en/connect/unity-catalog/storage-credentials.html
func buildStorageLocationIams(providerGroup string) ([]tf.Resource, error) {
	var resources []tf.Resource

	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	for _, location := range unitycatalog.AllStorageLocations(config) {
		if location.SelfAssumingRole {
			continue
		}

		// Create IAM resources only if this flag is set.
		if location.ExternalIAMRoleArn == "" {
			externalId, err := dataplatformconsts.GetCredentialExternalId(config.Region)
			if err != nil {
				return nil, oops.Wrapf(err, "getting storage credential external id")
			}

			if slices.Contains(location.DisallowedRegions, config.Region) {
				continue
			}
			bucketName := awsregionconsts.RegionPrefix[config.Region] + location.UnprefixedBucketName
			bucketArn := fmt.Sprintf("arn:aws:s3:::%s", bucketName)
			roleName := dataplatformconsts.StorageCredentialIamRoleName(bucketName)
			roleArn, err := dataplatformresource.IAMRoleArn(config.DatabricksProviderGroup, roleName)
			if err != nil {
				return nil, oops.Wrapf(err, "failed to build role arn")
			}

			statements := []policy.AWSPolicyStatement{
				// Allow databricks to assume role, from a specific role on their end and
				// with a specific externalid provided.
				{
					Principal: map[string]string{
						"AWS": dataplatformconsts.DatabricksUCAwsRoleReference,
					},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{
							"sts:ExternalId": externalId,
						},
					},
					Effect: "Allow",
					Action: []string{
						"sts:AssumeRole",
					},
				},
			}

			// Self-assuming roles cannot be made in one-shot with terraform, because the self-assuming policy
			// cannot be created when the role doesn't exist. So, we must first create the role (`NeedsCreation_DONOTUSE` is true),
			// then create a new PR to unset NeedsCreation_DONOTUSE so that the self-assumption can happen.
			if !location.NeedsCreation_DONOTUSE {
				statements = append(statements, policy.AWSPolicyStatement{
					Principal: map[string]string{
						"AWS": roleArn,
					},
					Effect: "Allow",
					Action: []string{
						"sts:AssumeRole",
					},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{
							"sts:ExternalId": externalId,
						},
					},
				})
			}

			storageLocationRole := &awsresource.IAMRole{
				Name:        roleName,
				Description: "Storage Credential Role assumed by databricks to access this bucket.",
				AssumeRolePolicy: policy.AWSPolicy{
					Version:   policy.AWSPolicyVersion,
					Statement: statements,
				},
				Tags: map[string]string{
					"samsara:service":       "databricks-storage-credential-role",
					"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
					"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
				},
			}

			s3Access := &awsresource.IAMRolePolicy{
				Name: "s3-access",
				Role: storageLocationRole.ResourceId(),
				Policy: policy.AWSPolicy{
					Version: policy.AWSPolicyVersion,
					Statement: []policy.AWSPolicyStatement{
						{
							Effect: "Allow",
							Action: []string{
								"s3:GetObject",
								"s3:PutObject",
								"s3:DeleteObject",
								"s3:ListBucket",
								"s3:GetBucketLocation",
							},
							Resource: []string{
								bucketArn,
								fmt.Sprintf("%s/*", bucketArn),
							},
						},
					},
				},
			}

			resources = append(resources, storageLocationRole, s3Access)
		}
	}
	return resources, nil
}

func buildCloudCredentialIamRoles(providerGroup string) ([]tf.Resource, error) {
	var resources []tf.Resource

	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	externalId, err := dataplatformconsts.GetCredentialExternalId(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "getting storage credential external id")
	}

	for _, credential := range unitycatalog.AllCloudCredentials(config) {

		if credential.SelfAssumingRole {
			continue
		}

		roleName := dataplatformconsts.CloudCredentialIamRoleName(credential.Name)
		roleArn, err := dataplatformresource.IAMRoleArn(config.DatabricksProviderGroup, roleName)
		if err != nil {
			return nil, oops.Wrapf(err, "failed to build role arn")
		}

		// Create trust and self-assume statements.
		statements := []policy.AWSPolicyStatement{
			// Allow databricks to assume role, from a specific role on their end and
			// with a specific externalid provided.
			{
				Principal: map[string]string{
					"AWS": dataplatformconsts.DatabricksUCAwsRoleReference,
				},
				Condition: &policy.AWSPolicyCondition{
					StringEquals: map[string]string{
						"sts:ExternalId": externalId,
					},
				},
				Effect: "Allow",
				Action: []string{
					"sts:AssumeRole",
				},
			},
		}

		// Self-assuming roles cannot be made in one-shot with terraform, because the self-assuming policy
		// cannot be created when the role doesn't exist. So, we must first create the role (`NeedsCreation_DONOTUSE` is true),
		// then create a new PR to unset NeedsCreation_DONOTUSE so that the self-assumption can happen.
		if !credential.NeedsCreation_DONOTUSE {
			statements = append(statements, policy.AWSPolicyStatement{
				Principal: map[string]string{
					"AWS": roleArn,
				},
				Effect: "Allow",
				Action: []string{
					"sts:AssumeRole",
				},
				Condition: &policy.AWSPolicyCondition{
					StringEquals: map[string]string{
						"sts:ExternalId": externalId,
					},
				},
			})
		}

		cloudCredentialRole := &awsresource.IAMRole{
			Name:        roleName,
			Description: "Cloud Credential Role assumed by databricks to access these cloud resources.",
			AssumeRolePolicy: policy.AWSPolicy{
				Version:   policy.AWSPolicyVersion,
				Statement: statements,
			},
			Tags: map[string]string{
				"samsara:service":       "databricks-cloud-credential-role",
				"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
				"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
			},
		}

		resources = append(resources, cloudCredentialRole)

		policyResources, err := GetCloudCredentialPolicyResources(credential, *cloudCredentialRole, config.Region)
		if err != nil {
			return nil, oops.Wrapf(err, "getting cloud credential policy resources")
		}
		resources = append(resources, policyResources...)
	}

	return resources, nil
}

func GetCloudCredentialPolicyResources(credential unitycatalog.CloudCredential, cloudCredentialRole awsresource.IAMRole, region string) ([]tf.Resource, error) {
	var s3PolicyStatements []policy.AWSPolicyStatement
	for _, s3Permission := range credential.S3Permissions {
		if slices.Contains(s3Permission.DisallowedRegions, region) {
			continue
		}

		bucketArn := fmt.Sprintf("arn:aws:s3:::%s", awsregionconsts.RegionPrefix[region]+s3Permission.UnprefixedBucketName)
		allowedPerms := s3ReadPermissions
		if s3Permission.ReadWrite {
			allowedPerms = s3ReadWritePermissions
		} else if s3Permission.WriteOnly {
			allowedPerms = s3WritePermissions
		}

		// Make sure to give ListBucket on the bucket itself; necessary for
		// read operations. Skip for write-only permissions.
		if !s3Permission.WriteOnly {
			s3PolicyStatements = append(s3PolicyStatements, policy.AWSPolicyStatement{
				Effect:   "Allow",
				Action:   []string{"s3:ListBucket"},
				Resource: []string{bucketArn},
			})
		}

		var s3Resource []string
		if len(s3Permission.AllowedPrefixes) > 0 {
			for _, prefix := range s3Permission.AllowedPrefixes {
				s3Resource = append(s3Resource, fmt.Sprintf("%s/%s/*", bucketArn, prefix))
			}
		} else {
			s3Resource = append(s3Resource, fmt.Sprintf("%s/*", bucketArn))
		}

		s3PolicyStatements = append(s3PolicyStatements, policy.AWSPolicyStatement{
			Effect:   "Allow",
			Action:   allowedPerms,
			Resource: s3Resource,
		})
	}

	var ssmPolicyStatements []policy.AWSPolicyStatement
	var ssmParameters []string
	for _, ssmParam := range credential.SSMParameters {
		ssmParameters = append(
			ssmParameters,
			fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", region, infraconsts.GetDatabricksAccountIdForRegion(region), ssmParam))
	}
	if len(ssmParameters) > 0 {
		ssmPolicyStatements = append(ssmPolicyStatements, policy.AWSPolicyStatement{
			Effect:   "Allow",
			Action:   []string{"ssm:GetParameter"},
			Resource: ssmParameters,
		})
	}

	var sqsConsumerPolicyStatements []policy.AWSPolicyStatement
	var sqsConsumerQueues []string
	for _, queue := range credential.SqsConsumerQueues {
		sqsConsumerQueues = append(
			sqsConsumerQueues,
			fmt.Sprintf("arn:aws:sqs:%s:%d:%s", region, infraconsts.GetAccountIdForRegion(region), queue))

	}
	if len(sqsConsumerQueues) > 0 {
		sqsConsumerPolicyStatements = append(sqsConsumerPolicyStatements, policy.AWSPolicyStatement{
			Effect:   "Allow",
			Action:   sqsConsumerPermissions,
			Resource: sqsConsumerQueues,
		})
	}

	var sqsProducerPolicyStatements []policy.AWSPolicyStatement
	var sqsProducerQueues []string
	for _, queue := range credential.SqsProducerQueues {
		sqsProducerQueues = append(
			sqsProducerQueues,
			fmt.Sprintf("arn:aws:sqs:%s:%d:%s", region, infraconsts.GetAccountIdForRegion(region), queue))

	}
	if len(sqsProducerQueues) > 0 {
		sqsProducerPolicyStatements = append(sqsProducerPolicyStatements, policy.AWSPolicyStatement{
			Effect:   "Allow",
			Action:   sqsProducerPermissions,
			Resource: sqsProducerQueues,
		})
	}

	var batchQueuePolicyStatements []policy.AWSPolicyStatement
	var batchQueues []string
	for _, queue := range credential.BatchJobQueues {
		batchQueues = append(batchQueues, fmt.Sprintf("arn:aws:batch:%s:%d:job-queue/%s", region, infraconsts.GetAccountIdForRegion(region), queue))
	}
	if len(batchQueues) > 0 {
		batchQueuePolicyStatements = append(batchQueuePolicyStatements, policy.AWSPolicyStatement{
			Effect:   "Allow",
			Action:   batchJobPermissions,
			Resource: batchQueues,
		})
	}

	// Add Kinesis policy statements for enhanced fan-out consumers
	var kinesisPolicyStatements []policy.AWSPolicyStatement
	if len(credential.ReadKinesisStreamArns) > 0 {
		// Grant permissions on the streams themselves
		kinesisPolicyStatements = append(kinesisPolicyStatements, policy.AWSPolicyStatement{
			Effect:   "Allow",
			Action:   kinesisEnhancedFanOutStreamPermissions,
			Resource: credential.ReadKinesisStreamArns,
		})

		// Also need permissions on stream consumers (ARN pattern: stream/*/consumer/*)
		var streamConsumerArns []string
		for _, streamArn := range credential.ReadKinesisStreamArns {
			streamConsumerArns = append(streamConsumerArns, fmt.Sprintf("%s/consumer/*", streamArn))
		}
		kinesisPolicyStatements = append(kinesisPolicyStatements, policy.AWSPolicyStatement{
			Effect:   "Allow",
			Action:   kinesisEnhancedFanOutConsumerPermissions,
			Resource: streamConsumerArns,
		})
	}

	// Add SES policy statements if the credential needs SES access.
	var sesPolicyStatements []policy.AWSPolicyStatement
	if credential.SESAccess {
		sesPolicyStatements = append(sesPolicyStatements, policy.AWSPolicyStatement{
			Effect:   "Allow",
			Action:   sesPermissions,
			Resource: []string{"*"},
			Condition: &policy.AWSPolicyCondition{
				ForAllValuesStringLike: map[string][]string{
					"ses:Recipients": {
						"*@samsara.com",
						"*@samsara-net.slack.com",
						"*@samsara.pagerduty.com",
					},
				},
				StringEquals: map[string]string{
					"ses:FromAddress": dataplatformresource.DatabricksAlertsSender(region),
				},
			},
		})
	}

	// Add SNS policy statements if the credential has SNS topic ARNs.
	var snsPolicyStatements []policy.AWSPolicyStatement
	if len(credential.SnsTopicPublishArns) > 0 {
		snsPolicyStatements = append(snsPolicyStatements, policy.AWSPolicyStatement{
			Effect:   "Allow",
			Action:   snsPublishPermissions,
			Resource: credential.SnsTopicPublishArns,
		})
	}

	// Add CodeArtifact policy statements if the credential needs CodeArtifact access.
	var codeArtifactPolicyStatements []policy.AWSPolicyStatement
	if credential.EnableReadFromCodeArtifact {
		codeArtifactPolicyStatements = append(codeArtifactPolicyStatements,
			policy.AWSPolicyStatement{
				Effect: "Allow",
				Action: []string{
					"codeartifact:Describe*",
					"codeartifact:Get*",
					"codeartifact:List*",
					"codeartifact:ReadFromRepository",
				},
				Resource: []string{"*"},
			},
			policy.AWSPolicyStatement{
				Effect: "Allow",
				Action: []string{
					"sts:GetServiceBearerToken",
				},
				Resource: []string{"*"},
				Condition: &policy.AWSPolicyCondition{
					StringEquals: map[string]string{
						"sts:AWSServiceName": "codeartifact.amazonaws.com",
					},
				},
			},
		)
	}

	var dynamoDBPolicyStatements []policy.AWSPolicyStatement
	for _, dynamoDBPermission := range credential.DynamoDBPermissions {
		if slices.Contains(dynamoDBPermission.DisallowedRegions, region) {
			continue
		}

		dynamoDBPolicyStatements = append(dynamoDBPolicyStatements, policy.AWSPolicyStatement{
			Sid:      "DynamoDBRead",
			Effect:   "Allow",
			Action:   dynamoDBReadPermissions,
			Resource: dynamoDBPermission.Arns,
		})
		if dynamoDBPermission.ReadWrite {
			dynamoDBPolicyStatements = append(dynamoDBPolicyStatements, policy.AWSPolicyStatement{
				Sid:      "DynamoDBWrite",
				Effect:   "Allow",
				Action:   dynamoDBWritePermissions,
				Resource: dynamoDBPermission.Arns,
			})
		}
		if dynamoDBPermission.CanScan {
			dynamoDBPolicyStatements = append(dynamoDBPolicyStatements, policy.AWSPolicyStatement{
				Sid:      "DynamoDBScan",
				Effect:   "Allow",
				Action:   dynamoDBScanPermissions,
				Resource: dynamoDBPermission.Arns,
			})
		}
	}

	resources := []tf.Resource{}

	// Add in the s3 statements as necessary
	if len(s3PolicyStatements) > 0 {
		s3Access := &awsresource.IAMRolePolicy{
			Name: "s3-access",
			Role: cloudCredentialRole.ResourceId(),
			Policy: policy.AWSPolicy{
				Version:   policy.AWSPolicyVersion,
				Statement: s3PolicyStatements,
			},
		}
		resources = append(resources, s3Access)
	}

	// Add in the SSM statements as necessary
	if len(ssmPolicyStatements) > 0 {
		ssmAccess := &awsresource.IAMRolePolicy{
			Name: "ssm-access",
			Role: cloudCredentialRole.ResourceId(),
			Policy: policy.AWSPolicy{
				Version:   policy.AWSPolicyVersion,
				Statement: ssmPolicyStatements,
			},
		}
		resources = append(resources, ssmAccess)
	}

	// Add in the SQS Consumer statements as necessary
	if len(sqsConsumerPolicyStatements) > 0 {
		sqsConsumerAccess := &awsresource.IAMRolePolicy{
			Name: "sqs-consumer-access",
			Role: cloudCredentialRole.ResourceId(),
			Policy: policy.AWSPolicy{
				Version:   policy.AWSPolicyVersion,
				Statement: sqsConsumerPolicyStatements,
			},
		}
		resources = append(resources, sqsConsumerAccess)
	}

	// Add in the SQS Producer statements as necessary
	if len(sqsProducerPolicyStatements) > 0 {
		sqsProducerAccess := &awsresource.IAMRolePolicy{
			Name: "sqs-producer-access",
			Role: cloudCredentialRole.ResourceId(),
			Policy: policy.AWSPolicy{
				Version:   policy.AWSPolicyVersion,
				Statement: sqsProducerPolicyStatements,
			},
		}
		resources = append(resources, sqsProducerAccess)
	}

	if len(batchQueuePolicyStatements) > 0 {
		batchQueueAccess := &awsresource.IAMRolePolicy{
			Name: "batch-queue-access",
			Role: cloudCredentialRole.ResourceId(),
			Policy: policy.AWSPolicy{
				Version:   policy.AWSPolicyVersion,
				Statement: batchQueuePolicyStatements,
			},
		}
		resources = append(resources, batchQueueAccess)
	}

	// Add Kinesis enhanced fan-out access policy
	if len(kinesisPolicyStatements) > 0 {
		kinesisAccess := &awsresource.IAMRolePolicy{
			Name: "kinesis-enhanced-fanout-access",
			Role: cloudCredentialRole.ResourceId(),
			Policy: policy.AWSPolicy{
				Version:   policy.AWSPolicyVersion,
				Statement: kinesisPolicyStatements,
			},
		}
		resources = append(resources, kinesisAccess)
	}

	if len(sesPolicyStatements) > 0 {
		sesAccess := &awsresource.IAMRolePolicy{
			Name: "ses-access",
			Role: cloudCredentialRole.ResourceId(),
			Policy: policy.AWSPolicy{
				Version:   policy.AWSPolicyVersion,
				Statement: sesPolicyStatements,
			},
		}
		resources = append(resources, sesAccess)
	}

	if len(snsPolicyStatements) > 0 {
		snsAccess := &awsresource.IAMRolePolicy{
			Name: "sns-access",
			Role: cloudCredentialRole.ResourceId(),
			Policy: policy.AWSPolicy{
				Version:   policy.AWSPolicyVersion,
				Statement: snsPolicyStatements,
			},
		}
		resources = append(resources, snsAccess)
	}

	if len(codeArtifactPolicyStatements) > 0 {
		codeArtifactAccess := &awsresource.IAMRolePolicy{
			Name: "codeartifact-access",
			Role: cloudCredentialRole.ResourceId(),
			Policy: policy.AWSPolicy{
				Version:   policy.AWSPolicyVersion,
				Statement: codeArtifactPolicyStatements,
			},
		}
		resources = append(resources, codeArtifactAccess)
	}

	if len(dynamoDBPolicyStatements) > 0 {
		dynamoDBAccess := &awsresource.IAMRolePolicy{
			Name: "dynamodb-access",
			Role: cloudCredentialRole.ResourceId(),
			Policy: policy.AWSPolicy{
				Version:   policy.AWSPolicyVersion,
				Statement: dynamoDBPolicyStatements,
			},
		}
		resources = append(resources, dynamoDBAccess)
	}

	return resources, nil
}

var s3ReadPermissions = []string{
	"s3:Get*",
	"s3:List*",
}

var s3WritePermissions = []string{
	"s3:Put*",
	"s3:Delete*",
}

var s3ReadWritePermissions = []string{
	"s3:Get*",
	"s3:List*",
	"s3:Put*",
	"s3:Delete*",
}

var sqsConsumerPermissions = []string{
	"sqs:ReceiveMessage",
	"sqs:DeleteMessage",
	"sqs:GetQueueAttributes",
	"sqs:ChangeMessageVisibility",
}

var sqsProducerPermissions = []string{
	"sqs:SendMessage",
}

var batchJobPermissions = []string{
	"batch:SubmitJob",
}

var sesPermissions = []string{
	"ses:SendEmail",
	"ses:SendRawEmail",
}

// kinesisEnhancedFanOutStreamPermissions defines permissions needed for Kinesis streams
var kinesisEnhancedFanOutStreamPermissions = []string{
	"kinesis:Describe*",
	"kinesis:List*",
	"kinesis:GetRecords",
	"kinesis:GetShardIterator",
	"kinesis:RegisterStreamConsumer",
}

// kinesisEnhancedFanOutConsumerPermissions defines permissions needed for Kinesis EFO consumers
var kinesisEnhancedFanOutConsumerPermissions = []string{
	"kinesis:SubscribeToShard",
	"kinesis:DescribeStreamConsumer",
	"kinesis:DeregisterStreamConsumer",
	"kinesis:List*",
}

// snsPublishPermissions defines permissions needed for publishing to SNS topics
var snsPublishPermissions = []string{
	"sns:Publish",
}

// dynamoDBReadPermissions defines permissions needed for reading from DynamoDB tables
var dynamoDBReadPermissions = []string{
	"dynamodb:Query",
	"dynamodb:GetItem",
	"dynamodb:DescribeTable",
	"dynamodb:BatchGetItem",
}

// dynamoDBWritePermissions defines permissions needed for writing to DynamoDB tables
var dynamoDBWritePermissions = []string{
	"dynamodb:UpdateItem",
	"dynamodb:PutItem",
	"dynamodb:DeleteItem",
	"dynamodb:BatchWriteItem",
}

// dynamoDBScanPermissions defines permissions needed for scanning DynamoDB tables.
// This is generally discouraged as it can be expensive.
var dynamoDBScanPermissions = []string{
	"dynamodb:Scan",
}
