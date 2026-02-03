package dataplatformprojects

import (
	"fmt"
	"sort"
	"strings"

	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/team"
)

func clusterProfileExists(name string, c dataplatformconfig.DatabricksConfig) bool {
	for _, c := range allRawClusterProfiles(c) {
		if c.clusterName == name {
			return true
		}
	}
	if name == "databricks-support" {
		return true
	}

	return false
}

type iamRolePolicyStatement struct {
	Effect  string
	Arns    []string
	Actions []string
}

// ec2GroupNameOverride lists all clusters that were created with EC2 group names that
// are formatted differently than all our other clusters. This group has "-cluster" in the name,
// while all others (and any new ones moving forward) do not.
var ec2GroupNameOverride = map[string]struct{}{
	"kinesisstats-import":    {},
	"s3bigstats-import":      {},
	"readonly":               {},
	"report-aggregation":     {},
	"rate-plan-optimization": {},
	"thor-variant-ingest":    {},
	"vodafone-reports":       {},
	"rds-import":             {},
	"dynamodb-import":        {},
}

func Ec2Clusters(c dataplatformconfig.DatabricksConfig) (map[string][]tf.Resource, error) {
	resourceGroups := make(map[string][]tf.Resource)

	clusterProfiles, err := allUpdatedClusterProfiles(c, c.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "Error building allUpdatedClusterProfiles")
	}

	// Set up an IAM role + instance profile for every cluster.
	for _, config := range clusterProfiles {
		var ec2Resources []tf.Resource
		formattedClusterName := config.clusterName + "-cluster"
		clusterLabel := config.clusterName
		if _, ok := ec2GroupNameOverride[config.clusterName]; ok {
			clusterLabel = formattedClusterName
		}

		resourceBasename := strings.Replace("ec2_"+clusterLabel, "-", "_", -1)

		trustStatements := []policy.AWSPolicyStatement{
			policy.AWSPolicyStatement{
				Effect:    "Allow",
				Action:    []string{"sts:AssumeRole"},
				Principal: map[string]string{"Service": "ec2.amazonaws.com"},
			},
		}

		// Give permissions to use this as a sql endpoint in the biztech POC.
		if config.clusterName == "biztech-dbt-dev" {
			trustStatements = append(trustStatements, policy.AWSPolicyStatement{
				Effect:    "Allow",
				Action:    []string{"sts:AssumeRole"},
				Principal: map[string]string{"AWS": "arn:aws:iam::790110701330:role/serverless-customer-resource-role"},
				Condition: &policy.AWSPolicyCondition{
					StringEquals: map[string]string{"sts:ExternalId": fmt.Sprintf("databricks-serverless-%d", dataplatformconsts.BiztechPocWorkspaceId)},
				},
			})
		}

		role := &awsresource.IAMRole{
			ResourceName: resourceBasename,
			Name:         formattedClusterName,
			Path:         "/ec2-instance/",
			AssumeRolePolicy: policy.AWSPolicy{
				Version:   policy.AWSPolicyVersion,
				Statement: trustStatements,
			},
			Tags: map[string]string{
				"samsara:service":       formattedClusterName,
				"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
				"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
			},
		}
		roleId := role.ResourceId()
		roleRef := roleId.Reference()
		ec2Resources = append(ec2Resources, role)

		instanceProfile := &awsresource.IAMInstanceProfile{
			ResourceName: resourceBasename,
			Name:         formattedClusterName,
			Role:         roleRef,
		}
		instanceProfileArn := &genericresource.StringLocal{
			Name:  "cluster_profile_" + strings.Replace(config.clusterName, "-", "_", -1),
			Value: fmt.Sprintf(`"%s"`, instanceProfile.ResourceId().ReferenceAttr("arn")),
		}
		ec2Resources = append(ec2Resources, instanceProfile, instanceProfileArn)

		defaultPolicies := []string{
			"write_cluster_logs",
			"read_dataplatform_deployed_artifacts",
		}

		// Unity catalog enabled clusters don't need all the permissions
		// we've historically given to all clusters. Reading deployed artifact
		// objects and writing clusters logs is all they need by default.
		if !config.isForUnityCatalog {
			defaultPolicies = append(defaultPolicies,
				"pull-ecr-docker-image",
				"ses_send_email",
				"datamodel_ga_read",
			)
		}

		for _, policy := range append(
			config.policyAttachments,
			defaultPolicies...,
		) {
			attachmentName := fmt.Sprintf("%s_%s", resourceBasename, policy)
			attachment := &awsresource.IAMRolePolicyAttachment{
				Name:      attachmentName,
				Role:      roleRef,
				PolicyARN: awsresource.IAMPolicyResourceId(policy).Reference(),
			}
			ec2Resources = append(ec2Resources, attachment)
		}

		if config.readParameters != nil {
			paramResources := []string{}
			for _, parameter := range config.readParameters {
				ssmResource := fmt.Sprintf(
					"arn:aws:ssm:%s:%s:parameter/%s",
					genericresource.DataResourceId("aws_region", "current").ReferenceAttr("name"),
					genericresource.DataResourceId("aws_caller_identity", "current").ReferenceAttr("account_id"),
					parameter,
				)
				paramResources = append(paramResources, ssmResource)
			}
			ssmPolicy := &awsresource.IAMRolePolicy{
				ResourceName: fmt.Sprintf("%s_ssm", resourceBasename),
				Name:         "ssm",
				Role:         roleId,
				Policy: policy.AWSPolicy{
					Version: policy.AWSPolicyVersion,
					Statement: []policy.AWSPolicyStatement{
						policy.AWSPolicyStatement{
							Effect: "Allow",
							Action: []string{
								"ssm:GetParameter*",
							},
							Resource: paramResources,
						},
					},
				},
			}
			ec2Resources = append(ec2Resources, ssmPolicy)
		}

		if config.sqsProducerQueues != nil {
			sqsQueueArns := make([]string, len(config.sqsProducerQueues))
			for i, sqsQueue := range config.sqsProducerQueues {
				sqsQueueArns[i] = fmt.Sprintf("arn:aws:sqs:%s:%d:%s", c.Region, c.AWSAccountId, sqsQueue)
			}
			sqsPolicy := &awsresource.IAMRolePolicy{
				ResourceName: resourceBasename + "_sqs_producer",
				Name:         "sqs",
				Role:         roleId,
				Policy: policy.AWSPolicy{
					Version: policy.AWSPolicyVersion,
					Statement: []policy.AWSPolicyStatement{
						policy.AWSPolicyStatement{
							Effect: "Allow",
							Action: []string{
								"sqs:SendMessage",
							},
							Resource: sqsQueueArns,
						},
					},
				},
			}
			ec2Resources = append(ec2Resources, sqsPolicy)
		}

		if config.sqsConsumerQueues != nil {
			sqsQueueArns := make([]string, len(config.sqsConsumerQueues))
			for i, sqsQueue := range config.sqsConsumerQueues {
				sqsQueueArns[i] = fmt.Sprintf("arn:aws:sqs:%s:%d:%s", c.Region, c.AWSAccountId, sqsQueue)
			}
			sqsPolicy := &awsresource.IAMRolePolicy{
				ResourceName: resourceBasename + "_sqs",
				Name:         "sqs",
				Role:         roleId,
				Policy: policy.AWSPolicy{
					Version: policy.AWSPolicyVersion,
					Statement: []policy.AWSPolicyStatement{
						policy.AWSPolicyStatement{
							Effect: "Allow",
							Action: []string{
								"sqs:DeleteMessage",
								"sqs:ReceiveMessage",
								"sqs:ChangeMessageVisibility*",
								"sqs:GetQueueAttributes",
							},
							Resource: sqsQueueArns,
						},
					},
				},
			}
			ec2Resources = append(ec2Resources, sqsPolicy)
		}

		if len(config.readwriteBuckets) > 0 {
			var readwriteResources []string
			for _, bucket := range config.readwriteBuckets {
				arn := fmt.Sprintf("arn:aws:s3:::%s", bucket)
				readwriteResources = append(readwriteResources,
					arn,
					arn+"/*",
				)
			}
			sort.Strings(readwriteResources)
			s3WritePolicyStatement := policy.AWSPolicyStatement{
				Effect:   "Allow",
				Action:   []string{"s3:*"},
				Resource: readwriteResources,
			}
			s3Policy := &awsresource.IAMRolePolicy{
				ResourceName: resourceBasename + "_s3_database_write",
				Name:         "s3-database-write",
				Role:         roleId,
				Policy: policy.AWSPolicy{
					Version:   policy.AWSPolicyVersion,
					Statement: []policy.AWSPolicyStatement{s3WritePolicyStatement},
				},
			}
			ec2Resources = append(ec2Resources, s3Policy)
		}

		if len(config.readBuckets) > 0 {
			var readResources []string
			for _, bucket := range config.readBuckets {
				arn := fmt.Sprintf("arn:aws:s3:::%s", bucket)
				readResources = append(readResources,
					arn,
					arn+"/*",
				)
			}
			sort.Strings(readResources)
			s3ReadPolicyStatement := policy.AWSPolicyStatement{
				Effect:   "Allow",
				Action:   []string{"s3:List*", "s3:Get*"},
				Resource: readResources,
			}
			s3Policy := &awsresource.IAMRolePolicy{
				ResourceName: resourceBasename + "_s3_database_read",
				Name:         "s3-database-read",
				Role:         roleId,
				Policy: policy.AWSPolicy{
					Version:   policy.AWSPolicyVersion,
					Statement: []policy.AWSPolicyStatement{s3ReadPolicyStatement},
				},
			}
			ec2Resources = append(ec2Resources, s3Policy)
		}

		// TODO: Deprecate the teamReadWriteDatabases field in a followup PR. Merge with the s3_database_write policy.
		prefix := awsregionconsts.RegionPrefix[c.Region]
		if len(config.teamReadWriteDatabases) != 0 {
			var arns []string
			for _, db := range config.teamReadWriteDatabases {
				arns = append(arns,
					fmt.Sprintf("arn:aws:s3:::%sdatabricks-warehouse/%s.db", prefix, db),
					fmt.Sprintf("arn:aws:s3:::%sdatabricks-warehouse/%s.db/*", prefix, db),
				)
			}

			s3Policy := &awsresource.IAMRolePolicy{
				ResourceName: resourceBasename + "_s3_dbs",
				Name:         "s3-team-database-write",
				Role:         roleId,
				Policy: policy.AWSPolicy{
					Version: policy.AWSPolicyVersion,
					Statement: []policy.AWSPolicyStatement{
						{
							Effect:   "Allow",
							Action:   []string{"s3:*"},
							Resource: arns,
						},
					},
				},
			}

			ec2Resources = append(ec2Resources, s3Policy)
		}

		glueBase := fmt.Sprintf(
			"arn:aws:glue:%s:%s",
			genericresource.DataResourceId("aws_region", "current").ReferenceAttr("name"),
			genericresource.DataResourceId("aws_caller_identity", "current").ReferenceAttr("account_id"),
		)
		glueBaseArn := fmt.Sprintf("%s:catalog", glueBase)

		glueReadActions := []string{
			"glue:GetDatabase",
			"glue:GetTable",
			"glue:BatchGetPartition",
		}

		glueWriteActions := []string{
			"glue:UpdateDatabase",
			"glue:CreateTable",
			"glue:UpdateTable",
			"glue:DeleteTable",
			"glue:BatchCreatePartition",
			"glue:BatchDeletePartition",
			"glue:CreatePartition",
			"glue:DeletePartition",
			"glue:UpdatePartition",
		}

		var glueReadOnlyIAMRolePolicyStatements []iamRolePolicyStatement
		if len(config.readDatabases) != 0 {
			arns := []string{
				glueBaseArn,
			}

			for _, db := range config.readDatabases {
				arns = append(arns,
					fmt.Sprintf("%s:database/%s", glueBase, db),
					fmt.Sprintf("%s:table/%s/*", glueBase, db),
				)
			}

			actions := glueReadActions
			if config.canCreateDatabases {
				actions = append(actions, "glue:CreateDatabase")
			}
			glueReadOnlyIAMRolePolicyStatements = append(glueReadOnlyIAMRolePolicyStatements, iamRolePolicyStatement{Effect: "Allow", Arns: arns, Actions: actions})
		}
		if len(glueReadOnlyIAMRolePolicyStatements) > 0 {
			ec2Resources = append(ec2Resources, generateGlueIAMRolePolicy(resourceBasename, "glue_database_read", roleId, glueReadOnlyIAMRolePolicyStatements))
		}

		var readWriteDatabases []string
		for _, db := range config.readwriteDatabases {
			readWriteDatabases = append(readWriteDatabases, db)
		}

		var glueReadWriteIAMRolePolicyStatements []iamRolePolicyStatement
		if len(readWriteDatabases) > 0 {
			arns := []string{
				glueBaseArn,
			}

			for _, db := range readWriteDatabases {
				arns = append(arns,
					fmt.Sprintf("%s:database/%s", glueBase, db),
					fmt.Sprintf("%s:table/%s/*", glueBase, db),
				)
			}

			actions := append(glueWriteActions, glueReadActions...)
			if config.canCreateDatabases && len(config.readDatabases) == 0 {
				actions = append(actions, "glue:CreateDatabase")
			}

			glueReadWriteIAMRolePolicyStatements = append(glueReadWriteIAMRolePolicyStatements, iamRolePolicyStatement{Effect: "Allow", Arns: arns, Actions: actions})
		}

		if len(glueReadWriteIAMRolePolicyStatements) > 0 {
			ec2Resources = append(ec2Resources, generateGlueIAMRolePolicy(resourceBasename, "glue_database_write", roleId, glueReadWriteIAMRolePolicyStatements))
		}

		ec2GroupName := strings.Replace("cluster_ec2_"+clusterLabel, "-", "_", -1)
		resourceGroups = project.MergeResourceGroups(resourceGroups,
			map[string][]tf.Resource{
				ec2GroupName: ec2Resources,
			})
	}
	return resourceGroups, nil
}

// InstanceProfileForDatabricksSupportCluster creates an IAM role and instance profile
// that we can attach to a cluster that we give Databricks support access to use for
// debugging. It's easier to just than creating manually so that all the cross account
// permissions are set up correctly for the Databricks role to be able to assume this role.
func InstanceProfileForDatabricksSupportCluster() map[string][]tf.Resource {
	resourceGroups := make(map[string][]tf.Resource)

	var ec2Resources []tf.Resource
	clusterName := "databricks-support-cluster"
	clusterLabel := "databricks-support"
	resourceBasename := strings.Replace("ec2_"+clusterLabel, "-", "_", -1)

	role := &awsresource.IAMRole{
		ResourceName: resourceBasename,
		Name:         clusterName,
		Path:         "/ec2-instance/",
		AssumeRolePolicy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect:    "Allow",
					Action:    []string{"sts:AssumeRole"},
					Principal: map[string]string{"Service": "ec2.amazonaws.com"},
				},
			},
		},
		Tags: map[string]string{
			"samsara:service":       clusterName,
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}
	roleId := role.ResourceId()
	roleRef := roleId.Reference()
	ec2Resources = append(ec2Resources, role)

	instanceProfile := &awsresource.IAMInstanceProfile{
		ResourceName: resourceBasename,
		Name:         clusterName,
		Role:         roleRef,
	}
	instanceProfileArn := &genericresource.StringLocal{
		Name:  "cluster_profile_" + strings.Replace("databricks-support-cluster", "-", "_", -1),
		Value: fmt.Sprintf(`"%s"`, instanceProfile.ResourceId().ReferenceAttr("arn")),
	}
	ec2Resources = append(ec2Resources, instanceProfile, instanceProfileArn)

	for _, policy := range []string{"write_cluster_logs", "glue_catalog_readonly"} {
		attachmentName := fmt.Sprintf("%s_%s", resourceBasename, policy)
		attachment := &awsresource.IAMRolePolicyAttachment{
			Name:      attachmentName,
			Role:      roleRef,
			PolicyARN: awsresource.IAMPolicyResourceId(policy).Reference(),
		}
		ec2Resources = append(ec2Resources, attachment)
	}

	listResources := []string{
		"arn:aws:s3:::samsara-databricks-playground",
		"arn:aws:s3:::samsara-databricks-playground/*",
	}
	sort.Strings(listResources)
	readwriteResources := []string{
		"arn:aws:s3:::samsara-databricks-playground/warehouse/playground.db/changping_corrupt_merge_base",
		"arn:aws:s3:::samsara-databricks-playground/warehouse/playground.db/changping_corrupt_merge_base/*",
		"arn:aws:s3:::samsara-databricks-playground/warehouse/playground.db/changping_corrupt_merge_delta",
		"arn:aws:s3:::samsara-databricks-playground/warehouse/playground.db/changping_corrupt_merge_delta/*",
	}
	sort.Strings(readwriteResources)

	var s3PolicyStatements []policy.AWSPolicyStatement
	s3PolicyStatements = append(s3PolicyStatements,
		policy.AWSPolicyStatement{
			Effect:   "Allow",
			Action:   []string{"s3:List*"},
			Resource: listResources,
		})
	s3PolicyStatements = append(s3PolicyStatements,
		policy.AWSPolicyStatement{
			Effect:   "Allow",
			Action:   []string{"s3:*"},
			Resource: readwriteResources,
		})
	s3Policy := &awsresource.IAMRolePolicy{
		ResourceName: resourceBasename + "_s3",
		Name:         "s3",
		Role:         roleId,
		Policy: policy.AWSPolicy{
			Version:   policy.AWSPolicyVersion,
			Statement: s3PolicyStatements,
		},
	}
	ec2Resources = append(ec2Resources, s3Policy)

	ec2GroupName := strings.Replace("cluster_ec2_"+clusterLabel, "-", "_", -1)
	resourceGroups = project.MergeResourceGroups(resourceGroups,
		map[string][]tf.Resource{
			ec2GroupName: ec2Resources,
		})
	return resourceGroups
}

func generateGlueIAMRolePolicy(resourceBasename string, name string, roleId tf.ResourceId, glueIAMRolePolicyStatements []iamRolePolicyStatement) *awsresource.IAMRolePolicy {
	statements := make([]policy.AWSPolicyStatement, 0, len(glueIAMRolePolicyStatements))
	for _, statement := range glueIAMRolePolicyStatements {
		statements = append(statements, policy.AWSPolicyStatement{
			Effect:   statement.Effect,
			Action:   statement.Actions,
			Resource: statement.Arns,
		})
	}

	return &awsresource.IAMRolePolicy{
		ResourceName: fmt.Sprintf("%s_%s", resourceBasename, name),
		Name:         strings.ReplaceAll(name, "_", "-"),
		Role:         roleId,
		Policy: policy.AWSPolicy{
			Version:   policy.AWSPolicyVersion,
			Statement: statements,
		},
	}
}
