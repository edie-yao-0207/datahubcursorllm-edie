package datahubprojects

import (
	"fmt"
	"strings"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/team"
)

const (
	oneHourSeconds  = 60 * 60
	fourHourSeconds = 4 * oneHourSeconds
)

// EKS auto-generates a managed aws security group during cluster creation for enabling traffic within the eks cluster.
// We retrieved this security group post cluster creation to associate it with other resources within the same vpc.
var eksClusterSecurityGroupId = "sg-01e73e63e43549506"

func buildkiteCIDataplatformDatahub(config dataplatformconfig.DatabricksConfig) []tf.Resource {
	var resources []tf.Resource

	roleresource := &awsresource.IAMRole{
		Name: "buildkite-ci-dataplatform-datahub",
		AssumeRolePolicy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Action: []string{"sts:AssumeRole"},
					Principal: map[string]string{
						"AWS": fmt.Sprintf("arn:aws:iam::%d:root", infraconsts.SamsaraAWSBuildkiteAccountID),
					},
					Effect: "Allow",
				},
			},
		},
		MaxSessionDuration: fourHourSeconds,
		Tags: map[string]string{
			"samsara:service":        "buildkite",
			"samsara:team":           strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group":  strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
			"samsara:rnd-allocation": "1",
		},
	}

	resources = append(resources, roleresource)

	resources = append(resources, &awsresource.IAMRolePolicy{
		Name: "ecr-write-policy",
		Role: roleresource.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect:   "Allow",
					Action:   []string{"ecr:GetAuthorizationToken"},
					Resource: []string{"*"},
				},
				{
					Effect: "Allow",
					Action: []string{"ecr:*"},
					Resource: []string{fmt.Sprintf("arn:aws:ecr:%s:%d:*", config.Region, infraconsts.GetDatabricksAccountIdForRegion(config.Region)),
						fmt.Sprintf("arn:aws:ecr:%s:%d:*", config.Region, infraconsts.SamsaraAWSBuildkiteAccountID)},
				},
			},
		},
	})

	resources = append(resources, &awsresource.IAMRolePolicy{
		Name: "eks-read-write-policy",
		Role: roleresource.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Action: []string{"eks:CreateCluster",
						"eks:CreateNodegroup",
						"eks:DescribeCluster",
						"eks:DescribeNodegroup",
						"eks:DescribeUpdate",
						"eks:ListClusters",
						"eks:ListNodegroups",
						"eks:ListTagsForResource",
						"eks:TagResource",
						"eks:UntagResource",
						"eks:UpdateClusterConfig",
						"eks:UpdateClusterVersion",
						"eks:UpdateNodegroupConfig",
						"eks:UpdateNodegroupVersion"},
					Resource: []string{"*"},
				},
			},
		},
	})

	return resources

}

func eksResources(config dataplatformconfig.DatabricksConfig, v vpcIds) (map[string][]tf.Resource, error) {
	accountId := awsregionconsts.RegionDatabricksAccountID[config.Region]
	oidcProvider := DatahubEKSClusterOIDCRegionProvider[config.Region]
	eksResources := []tf.Resource{}

	ssmPolicyStatements := []policy.AWSPolicyStatement{
		{
			Effect: "Allow",
			Action: []string{
				"ssm:GetParameter*",
			},
			Resource: []string{
				fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", config.Region, accountId, dataplatformconsts.ParameterDatadogAppToken),
				fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", config.Region, accountId, dataplatformconsts.ParameterDatadogApiToken),
				fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", config.Region, accountId, "E2_DATABRICKS_*"),
				fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", config.Region, accountId, "DAGSTER_PROD_DB_PASSWORD"),
				fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", config.Region, accountId, "DAGSTER_SLACK_BOT_TOKEN"),
				fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", config.Region, accountId, "DAGSTER_DATABRICKS_EU_TOKEN"),
				fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", config.Region, accountId, "DATAHUB_GMS_TOKEN"),
				fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", config.Region, accountId, "DATAHUB_GMS_SERVER"),
				fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", config.Region, accountId, "DATAHUB_DBT_API_TOKEN"),
				fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", config.Region, accountId, "DATAHUB_TABLEAU_TOKEN"),
			},
		},
	}

	glueStatements := []policy.AWSPolicyStatement{
		{
			Effect: "Allow",
			Action: []string{
				"glue:GetDatabases",
				"glue:GetTables",
			},
			Resource: []string{
				fmt.Sprintf("arn:aws:glue:%s:%d:catalog", config.Region, accountId),
				fmt.Sprintf("arn:aws:glue:%s:%d:database/*", config.Region, accountId),
				fmt.Sprintf("arn:aws:glue:%s:%d:table/*", config.Region, accountId),
			},
		},
	}

	clusterRole := awsresource.IAMRole{
		Name:        fmt.Sprintf("%s-%s", DatahubResourceBaseName, "eks"),
		Description: "Role used by EKS for Data Platform's Datahub deployment",
		AssumeRolePolicy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Principal: map[string]string{
						"Service": "eks.amazonaws.com",
					},
					Action: []string{"sts:AssumeRole"},
				},
				{
					Effect: "Allow",
					Principal: map[string]string{
						"Federated": fmt.Sprintf("arn:aws:iam::%d:oidc-provider/%s", accountId, oidcProvider),
					},
					Action: []string{"sts:AssumeRoleWithWebIdentity"},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{
							fmt.Sprintf("%s:aud", oidcProvider): "sts.amazonaws.com",
						},
					},
				},
			},
		},
		Tags: map[string]string{
			"samsara:service":        "datahub",
			"samsara:team":           strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group":  strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
			"samsara:rnd-allocation": "1",
		},
	}

	clusterRoleSSMGetParametesPolicy := awsresource.IAMRolePolicy{
		Name: fmt.Sprintf("%s-%s", DatahubResourceBaseName, "eks-ssm-get-parameters-policy"),
		Role: clusterRole.ResourceId(),
		Policy: policy.AWSPolicy{
			Version:   policy.AWSPolicyVersion,
			Statement: ssmPolicyStatements,
		},
	}

	publicAccessCidrs := append(infraconsts.OfficeCidrs, infraconsts.DevHomeCidrs...)
	publicAccessCidrs = append(publicAccessCidrs, infraconsts.AwsBuildkiteNatGatewayIPs...)
	publicAccessCidrs = append(publicAccessCidrs, infraconsts.AwsBuildkiteNatGatewayIPsForDagster...) /* intentionally Dagster */

	prefix := awsregionconsts.RegionPrefix[config.Region]

	eksCluster := awsresource.EKSCluster{
		Name:    DatahubResourceBaseName,
		RoleArn: clusterRole.ResourceId().ReferenceAttr("arn"),
		VPCConfig: awsresource.EKSClusterVPCConfig{
			EndpointPublicAccess:  true,
			EndpointPrivateAccess: true,
			PublicAccessCidrs:     publicAccessCidrs,
			SecurityGroupIds:      v.securityGroupIds,
			SubnetIds:             append(v.publicSubnetIds, v.privateSubnetIds...),
		},
		EnabledClusterLogTypes: []string{
			// https://docs.aws.amazon.com/eks/latest/userguide/control-plane-logs.html
			"api",
			"audit",
			"authenticator",
			"controllerManager",
			"scheduler",
		},
		Tags: awsresource.ConvertAWSTagsToTagMap(getDatahubTags("eks-cluster")),
	}

	clusterRolePolicy := awsresource.IAMRolePolicyAttachment{
		Name:      fmt.Sprintf("%s-%s", DatahubResourceBaseName, "eks-policy"),
		Role:      clusterRole.ResourceId().ReferenceAttr("name"),
		PolicyARN: "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy",
	}

	clusterVpcPolicy := awsresource.IAMRolePolicyAttachment{
		Name:      fmt.Sprintf("%s-%s", DatahubResourceBaseName, "eks-vpc-policy"),
		Role:      clusterRole.ResourceId().ReferenceAttr("name"),
		PolicyARN: "arn:aws:iam::aws:policy/AmazonEKSVPCResourceController",
	}

	nodeRole := awsresource.IAMRole{
		Name:        fmt.Sprintf("%s-%s", DatahubResourceBaseName, "eks-nodes"),
		Description: "Role used by EKS Nodes for Data Platform's Datahub deployment",
		AssumeRolePolicy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Principal: map[string]string{
						"Service": "ec2.amazonaws.com",
					},
					Action: []string{"sts:AssumeRole"},
				},
				{
					Effect: "Allow",
					Principal: map[string]string{
						"Service": "eks.amazonaws.com",
					},
					Action: []string{"sts:AssumeRole"},
				},
				{
					Effect: "Allow",
					Principal: map[string]string{
						"AWS": "arn:aws:iam::492164655156:role/dagster-eks-fargate-pods",
					},
					Action: []string{"sts:AssumeRole"},
				},
			},
		},
		Tags: map[string]string{
			"samsara:service":        "datahub",
			"samsara:team":           strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group":  strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
			"samsara:rnd-allocation": "1",
		},
	}

	nodeContainerRegistryPolicy := awsresource.IAMRolePolicyAttachment{
		Name:      fmt.Sprintf("%s-%s", DatahubResourceBaseName, "eks-nodes-ro-container-registry"),
		Role:      nodeRole.ResourceId().ReferenceAttr("name"),
		PolicyARN: "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
	}

	nodeWorkerPolicy := awsresource.IAMRolePolicyAttachment{
		Name:      fmt.Sprintf("%s-%s", DatahubResourceBaseName, "eks-nodes-worker-policy"),
		Role:      nodeRole.ResourceId().ReferenceAttr("name"),
		PolicyARN: "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy",
	}

	eksCNIPolicy := awsresource.IAMRolePolicyAttachment{
		Name:      fmt.Sprintf("%s-%s", DatahubResourceBaseName, "eks-cni-policy"),
		Role:      nodeRole.ResourceId().ReferenceAttr("name"),
		PolicyARN: "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy",
	}

	nodeRoleCloudwatchPolicy := awsresource.IAMRolePolicy{
		Name: fmt.Sprintf("%s-%s", DatahubResourceBaseName, "ec2-node-logging"),
		Role: nodeRole.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Action: []string{
						"logs:CreateLogStream",
						"logs:CreateLogGroup",
						"logs:DescribeLogStreams",
						"logs:PutLogEvents",
					},
					Resource: []string{"*"},
				},
			},
		},
	}

	nodeSsmPolicy := awsresource.IAMRolePolicy{
		Name: fmt.Sprintf("%s-%s", DatahubResourceBaseName, "ssm"),
		Role: nodeRole.ResourceId(),
		Policy: policy.AWSPolicy{
			Version:   policy.AWSPolicyVersion,
			Statement: ssmPolicyStatements,
		},
	}

	gluePolicy := awsresource.IAMRolePolicy{
		Name: fmt.Sprintf("%s-%s", DatahubResourceBaseName, "glue-get-tables-and-databases-policy"),
		Role: nodeRole.ResourceId(),
		Policy: policy.AWSPolicy{
			Version:   policy.AWSPolicyVersion,
			Statement: glueStatements,
		},
	}

	s3WarehouseReadPolicy := awsresource.IAMRolePolicy{
		Name: fmt.Sprintf("%s-%s", DatahubResourceBaseName, "s3-read-tables-policy"),
		Role: nodeRole.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Action: []string{
						"s3:Get*",
						"s3:List*",
					},
					Resource: []string{
						fmt.Sprintf("arn:aws:s3:::%sai-datasets", prefix),
						fmt.Sprintf("arn:aws:s3:::%sai-datasets/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sbiztech-edw-bronze", prefix),
						fmt.Sprintf("arn:aws:s3:::%sbiztech-edw-bronze/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sbiztech-edw-dev", prefix),
						fmt.Sprintf("arn:aws:s3:::%sbiztech-edw-dev/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sbiztech-edw-gold", prefix),
						fmt.Sprintf("arn:aws:s3:::%sbiztech-edw-gold/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sbiztech-edw-peopleops-sensitive", prefix),
						fmt.Sprintf("arn:aws:s3:::%sbiztech-edw-peopleops-sensitive/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sbiztech-edw-sensitive-bronze", prefix),
						fmt.Sprintf("arn:aws:s3:::%sbiztech-edw-sensitive-bronze/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sbiztech-edw-sensitive-ci", prefix),
						fmt.Sprintf("arn:aws:s3:::%sbiztech-edw-sensitive-ci/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sbiztech-edw-silver", prefix),
						fmt.Sprintf("arn:aws:s3:::%sbiztech-edw-silver/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sbiztech-edw-uat", prefix),
						fmt.Sprintf("arn:aws:s3:::%sbiztech-edw-uat/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sbiztech-edw-uat/*/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sbiztech-sda-production", prefix),
						fmt.Sprintf("arn:aws:s3:::%sbiztech-sda-production/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdata-eng-mapping-tables", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdata-eng-mapping-table/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdata-pipelines-delta-lake-from-eu-west-1", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdata-pipelines-delta-lake-from-eu-west-1/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdata-pipelines-delta-lake", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdata-pipelines-delta-lake/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdata-stream-lake", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdata-stream-lake/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdata-streams-delta-lake", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdata-streams-delta-lake/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdatabricks-audit-log", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdatabricks-audit-log/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdatabricks-billing", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdatabricks-billing/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdatabricks-playground", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdatabricks-playground/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdatabricks-sql-query-history", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdatabricks-sql-query-history/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdatabricks-warehouse", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdatabricks-warehouse/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdatabricks-workspace", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdatabricks-workspace/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdatahub-metadata", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdatahub-metadata/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdatamodel-warehouse", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdatamodel-warehouse/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdataplatform-deployed-artifacts", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdataplatform-deployed-artifacts/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdataplatform-fivetran-bronze", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdataplatform-fivetran-bronze/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdynamodb-delta-lake", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdynamodb-delta-lake/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sfivetran-greenhouse-recruiting-delta-tables", prefix),
						fmt.Sprintf("arn:aws:s3:::%sfivetran-greenhouse-recruiting-delta-tables/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sfivetran-netsuite-finance-delta-tables", prefix),
						fmt.Sprintf("arn:aws:s3:::%sfivetran-netsuite-finance-delta-tables/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%skinesisstats-delta-lake", prefix),
						fmt.Sprintf("arn:aws:s3:::%skinesisstats-delta-lake/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%slabelbox", prefix),
						fmt.Sprintf("arn:aws:s3:::%slabelbox/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%smarketing-data-analytics", prefix),
						fmt.Sprintf("arn:aws:s3:::%smarketing-data-analytics/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%smaps-places-migration", prefix),
						fmt.Sprintf("arn:aws:s3:::%smaps-places-migration/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%smixpanel", prefix),
						fmt.Sprintf("arn:aws:s3:::%smixpanel/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%snetsuite-delta-tables", prefix),
						fmt.Sprintf("arn:aws:s3:::%snetsuite-delta-tables/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%srds-delta-lake", prefix),
						fmt.Sprintf("arn:aws:s3:::%srds-delta-lake/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sreport-staging-tables", prefix),
						fmt.Sprintf("arn:aws:s3:::%sreport-staging-tables/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%ss3bigstats-delta-lake", prefix),
						fmt.Sprintf("arn:aws:s3:::%ss3bigstats-delta-lake/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%ss3-inventory", prefix),
						fmt.Sprintf("arn:aws:s3:::%ss3-inventory/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdatabricks-s3-inventory", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdatabricks-s3-inventory/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%szendesk", prefix),
						fmt.Sprintf("arn:aws:s3:::%szendesk/*", prefix),

						// EU buckets.
						"arn:aws:s3:::samsara-eu-datamodel-warehouse",
						"arn:aws:s3:::samsara-eu-datamodel-warehouse/*",
						"arn:aws:s3:::samsara-eu-databricks-warehouse",
						"arn:aws:s3:::samsara-eu-databricks-warehouse/*",

						// CA buckets.
						"arn:aws:s3:::samsara-ca-datamodel-warehouse",
						"arn:aws:s3:::samsara-ca-datamodel-warehouse/*",
						"arn:aws:s3:::samsara-ca-databricks-warehouse",
						"arn:aws:s3:::samsara-ca-databricks-warehouse/*",
					},
				},
			},
		},
	}

	clusterCustomPolicies := awsresource.IAMRolePolicy{
		Role: clusterRole.ResourceId(),
		Name: fmt.Sprintf("%s-%s", DatahubResourceBaseName, "eks-custom-policies"),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect:   "Allow",
					Action:   []string{"cloudwatch:PutMetricData"},
					Resource: []string{"*"},
				},
				{
					Effect: "Allow",
					Action: []string{
						"ec2:DescribeAccountAttributes",
						"ec2:DescribeAddresses",
						"ec2:DescribeInternetGateways",
						"ec2:AttachVolume",
						"ec2:CreateSnapshot",
						"ec2:CreateTags",
						"ec2:CreateVolume",
						"ec2:DeleteSnapshot",
						"ec2:DeleteTags",
						"ec2:DeleteVolume",
						"ec2:DescribeAvailabilityZones",
						"ec2:DescribeInstances",
						"ec2:DescribeSnapshots",
						"ec2:DescribeTags",
						"ec2:DescribeVolumes",
						"ec2:DescribeVolumesModifications",
						"ec2:DetachVolume",
						"ec2:ModifyVolume",
					},
					Resource: []string{"*"},
				},
			},
		},
	}

	nodeGroupScalingConfig := awsresource.EKSNodeGroupScalingConfig{
		DesiredSize: 1,
		MinSize:     1,
		MaxSize:     2,
	}

	ec2NodeGroup := awsresource.EKSNodeGroup{
		Name:          fmt.Sprintf("%s-%s", DatahubResourceBaseName, "eks-node-group"),
		ClusterName:   DatahubResourceBaseName,
		NodeRoleArn:   nodeRole.ResourceId().ReferenceAttr("arn"),
		ScalingConfig: nodeGroupScalingConfig,
		SubnetIds:     []string{fmt.Sprintf("${aws_subnet.%s-%sb-subnet-private.id}", DatahubResourceBaseName, config.Region)},
		Tags:          awsresource.ConvertAWSTagsToTagMap(getDatahubTags("eks-ec2-nodes")),
		LaunchTemplateASG: awsresource.LaunchTemplateASG{
			LaunchTemplateId: "lt-08ca45dd3f0b6c5fe",
			Version:          "$Latest",
		},
	}

	// uncomment this block to add a second node group temporarily for replacements

	// ec2NodeGroupTempForReplacements := awsresource.EKSNodeGroup{
	// 	Name:          fmt.Sprintf("%s-%s", DatahubResourceBaseName, "eks-node-group-2"),
	// 	ClusterName:   DatahubResourceBaseName,
	// 	NodeRoleArn:   nodeRole.ResourceId().ReferenceAttr("arn"),
	// 	ScalingConfig: nodeGroupScalingConfig,
	// 	SubnetIds:     []string{fmt.Sprintf("${aws_subnet.%s-%sb-subnet-private.id}", DatahubResourceBaseName, config.Region)},
	// 	Tags:          awsresource.ConvertAWSTagsToTagMap(getDatahubTags("eks-ec2-nodes")),
	// 	LaunchTemplateASG: awsresource.LaunchTemplateASG{
	// 		LaunchTemplateId: "lt-08ca45dd3f0b6c5fe",
	// 		Version:          "13",
	// 	},
	// }

	launchTemplate := awsresource.LaunchTemplate{
		Name:         fmt.Sprintf("%s-%s", DatahubResourceBaseName, "eks-node-group-launch-template"),
		ImageID:      "ami-0233aedb1db01d61e", // golden AMI samsara_golden_eks-1-33_al2023_x86 2026-01-12
		InstanceType: "m5.4xlarge",
		MetaDataOptions: awsresource.MetaDataOptions{
			HttpTokens: "required", // Enable IMDSv2
		},
		UserData: `MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="//"

--//
Content-Type: application/node.eks.aws

---
apiVersion: node.eks.aws/v1alpha1
kind: NodeConfig
spec:
  cluster:
    apiServerEndpoint: https://F89DF96197BE9D911A280AD16D82FEF7.gr7.us-west-2.eks.amazonaws.com
    certificateAuthority: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURCVENDQWUyZ0F3SUJBZ0lJTEdMR3poSDdhaFl3RFFZSktvWklodmNOQVFFTEJRQXdGVEVUTUJFR0ExVUUKQXhNS2EzVmlaWEp1WlhSbGN6QWVGdzB5TXpFeE1qY3hOVEV4TXpkYUZ3MHpNekV4TWpReE5URTJNemRhTUJVeApFekFSQmdOVkJBTVRDbXQxWW1WeWJtVjBaWE13Z2dFaU1BMEdDU3FHU0liM0RRRUJBUVVBQTRJQkR3QXdnZ0VLCkFvSUJBUUQwelhndGdRWVFCektBTkdmMnVZdU9UZW5qTFdzU1JGVk5kWFNGQ3B4ZlhIaHpFTXFDc1J2ZWYwNm0KVXdrajFic0h5bGxzS1FRZnVXTGk2VlhOYVEySFdMSDFzTEZZQVh3VmpNWlEzY0U5VXlpeCtuWFAxZ3ZVNmRmNgpoWWNsNDFvRDRRTUFTY2NqZCtzNnRrbjZnMy9BMDFzeGhVZG8xcnlqTFdodFd1KzhsK3p2WWczZndIMVdSUStMCitibzMwQi8ycUNKaSt3bzRjVTdDTmFVSVN2Z3dsRDNyY2NYRHdCZnVibzhJbzBsVzFWVW5rbi9lOVY3SUNKTnQKNDdlaVBKaXRFc3NzQmxFTXdvTmVqaEhVd2owRjRZalYwZWp3UkFFRTdYWldzNlJ1VFJRK29ZSUZianc2WWZWeQpjWGRDTkFraUJFckNtblNsQ3kwaHp1MjJoVlZIQWdNQkFBR2pXVEJYTUE0R0ExVWREd0VCL3dRRUF3SUNwREFQCkJnTlZIUk1CQWY4RUJUQURBUUgvTUIwR0ExVWREZ1FXQkJRaExGR3dseFdUY0ZyajZISlQrVm9VL3o0QTF6QVYKQmdOVkhSRUVEakFNZ2dwcmRXSmxjbTVsZEdWek1BMEdDU3FHU0liM0RRRUJDd1VBQTRJQkFRQ3E4SFZDaXFzOApDM0ZGcGZRY3FINkhMOUdrZXBlczRhTFpSNUk2OEg1RkI0ZUw2dVNNZlVEeHBxYlBqVkt0Q0VxZTBMdUFDQVl6CktkOVIwcTRLQzA3cjRob3VJMCtLWXV0cnZDaW9nTGZWNitpQ1N3RFgxUVRKTlQ1M3lPNFhmWW5Xa29wZkQ3MkIKdWNRS1VTd1J0QkpMaVpHZVNuUmt0MDZ0Tk1QaWVrOWNONHZkWWNSSmlsQWFQaHpFK0orNGV3NVpCNzV4VlhQaQp0NXllendLQ0ZIVWM4b1NoR1JJNkJFcnlOVWUxUmhERXMxTFQ0STdTay91KzhnMTl2cmh3WDlvTjllZWhHVGdhCkZ5bnZ1Q0hja1BBdWpSRmI4RENwRURPVjFpUlA3QjFEdmZIaDNURityUWlzMlBLWU5HcVVDclBJa0E0bDJwK3gKRE15Vnd5L09DcUlJCi0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K
    cidr: 172.20.0.0/16
    name: datahub
  kubelet:
    config:
      maxPods: 58
      clusterDNS:
      - 172.20.0.10
    flags:
    - "--node-labels=eks.amazonaws.com/nodegroup-image=ami-0233aedb1db01d61e,eks.amazonaws.com/capacityType=ON_DEMAND,eks.amazonaws.com/nodegroup=datahub-eks-node-group"

--//--`,
		VPCSecurityGroupIDs: []string{eksClusterSecurityGroupId},
		BlockDeviceMappings: []awsresource.LaunchTemplateBlockDeviceMapping{
			{
				DeviceName: "/dev/xvda",
				EBS: awsresource.LaunchTemplateBlockDeviceEBS{
					VolumeType: "gp2",
					VolumeSize: 40,
				},
			},
		},
	}

	asgName := fmt.Sprintf("${%s.resources[0].autoscaling_groups[0].name}", ec2NodeGroup.ResourceId().String())
	for _, tag := range getDatahubTags("eks-ec2-nodes") {
		eksResources = append(eksResources, &awsresource.AutoscalingGroupTag{
			Name:                 fmt.Sprintf("custom-tag-%s", strings.ReplaceAll(tag.Key, ":", "-")),
			AutoscalingGroupName: asgName,
			Tag: awsresource.ASGTag{
				Key:               tag.Key,
				Value:             tag.Value,
				PropagateAtLaunch: true,
			},
		})
	}

	fargatePodRole := awsresource.IAMRole{
		Name:        fmt.Sprintf("%s-%s", DatahubResourceBaseName, "eks-fargate-pods"),
		Description: "Role assumed by Fargate Pods running on Datahub eks cluster",
		AssumeRolePolicy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Principal: map[string]string{
						"Federated": fmt.Sprintf("arn:aws:iam::%d:oidc-provider/%s", accountId, oidcProvider),
					},
					Action: []string{"sts:AssumeRoleWithWebIdentity"},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{
							fmt.Sprintf("%s:aud", oidcProvider): "sts.amazonaws.com",
							fmt.Sprintf("%s:sub", oidcProvider): "system:serviceaccount:datahub-prod:datahub-datahub-mae-consumer",
						},
					},
				},
				{
					Effect: "Allow",
					Principal: map[string]string{
						"Federated": fmt.Sprintf("arn:aws:iam::%d:oidc-provider/%s", accountId, oidcProvider),
					},
					Action: []string{"sts:AssumeRoleWithWebIdentity"},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{
							fmt.Sprintf("%s:aud", oidcProvider): "sts.amazonaws.com",
							fmt.Sprintf("%s:sub", oidcProvider): "system:serviceaccount:datahub-prod:datahub-datahub-mce-consumer",
						},
					},
				},
				{
					Effect: "Allow",
					Principal: map[string]string{
						"Federated": fmt.Sprintf("arn:aws:iam::%d:oidc-provider/%s", accountId, oidcProvider),
					},
					Action: []string{"sts:AssumeRoleWithWebIdentity"},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{
							fmt.Sprintf("%s:aud", oidcProvider): "sts.amazonaws.com",
							fmt.Sprintf("%s:sub", oidcProvider): "system:serviceaccount:datahub-prod:datahub-datahub-gms",
						},
					},
				},
				{
					Effect: "Allow",
					Principal: map[string]string{
						"Federated": fmt.Sprintf("arn:aws:iam::%d:oidc-provider/%s", accountId, oidcProvider),
					},
					Action: []string{"sts:AssumeRoleWithWebIdentity"},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{
							fmt.Sprintf("%s:aud", oidcProvider): "sts.amazonaws.com",
							fmt.Sprintf("%s:sub", oidcProvider): "system:serviceaccount:datahub-prod:datahub-datahub-frontend",
						},
					},
				},
			},
		},
		Tags: awsresource.ConvertAWSTagsToTagMap(getDatahubTags("iam-eks-fargate-pods")),
	}

	fargatePodRoleCloudwatchPolicy := awsresource.IAMRolePolicy{
		Role: fargatePodRole.ResourceId(),
		Name: "cloudwatch-logging",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Action: []string{
						"logs:CreateLogStream",
						"logs:CreateLogGroup",
						"logs:DescribeLogStreams",
						"logs:PutLogEvents",
					},
					Resource: []string{"*"},
				},
			},
		},
	}

	dataCatalogBucketReadWritePolicy := awsresource.IAMRolePolicy{
		Role: fargatePodRole.ResourceId(),
		Name: "datacatalog-bucket-read-write-policy",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Action: []string{
						"s3:Put*",
						"s3:Get*",
						"s3:List*",
					},
					Resource: []string{
						fmt.Sprintf("arn:aws:s3:::%samundsen-metadata", prefix),
						fmt.Sprintf("arn:aws:s3:::%samundsen-metadata/*", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdatahub-metadata", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdatahub-metadata/*", prefix),
					},
				},
			},
		},
	}

	fargatePodRoleSSMGetParametesPolicy := awsresource.IAMRolePolicy{
		Role: fargatePodRole.ResourceId(),
		Name: "ssm-parameters-read",
		Policy: policy.AWSPolicy{
			Version:   policy.AWSPolicyVersion,
			Statement: ssmPolicyStatements,
		},
	}

	fargatePodRoleSTSPolicy := awsresource.IAMRolePolicy{
		Role: fargatePodRole.ResourceId(),
		Name: "sts-policy",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect:   "Allow",
					Action:   []string{"sts:AssumeRole"},
					Resource: []string{"arn:aws:iam::492164655156:role/datahub-eks-nodes"},
				},
				{
					Effect:   "Allow",
					Action:   []string{"sts:AssumeRole"},
					Resource: []string{"arn:aws:iam::492164655156:role/dagster-eks-nodes"},
				},
			},
		},
	}

	fargatePodRoleESPolicy := awsresource.IAMRolePolicy{
		Role: fargatePodRole.ResourceId(),
		Name: "elasticsearch-access",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Action: []string{
						"es:ESHttpGet",
						"es:ESHttpHead",
						"es:ESHttpPost",
						"es:ESHttpPut",
						"es:ESHttpDelete",
						"es:ESHttpPatch",
					},
					Resource: []string{fmt.Sprintf("arn:aws:es:%s:%d:domain/%s/*", config.Region, accountId, "datahub")},
				},
			},
		},
	}

	fargatePodLauncherRole := awsresource.IAMRole{
		Name:        fmt.Sprintf("%s-%s", DatahubResourceBaseName, "eks-fargate-pod-launcher"),
		Description: "Role used by Datahub EKS to launch pods on Fargate",
		AssumeRolePolicy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Principal: map[string]string{
						"Service": "eks-fargate-pods.amazonaws.com",
					},
					Action: []string{"sts:AssumeRole"},
				},
				{
					Effect: "Allow",
					Principal: map[string]string{
						"Service": "eks.amazonaws.com",
					},
					Action: []string{"sts:AssumeRole"},
				},
			},
		},
		Tags: awsresource.ConvertAWSTagsToTagMap(getDatahubTags("eks-fargate-pod-launcher")),
	}

	fargatePodLauncherServicePolicy := awsresource.IAMRolePolicyAttachment{
		Name:      fmt.Sprintf("%s-%s", DatahubResourceBaseName, "eks-fargate-pod-launcher-policy"),
		Role:      fargatePodLauncherRole.ResourceId().ReferenceAttr("name"),
		PolicyARN: "arn:aws:iam::aws:policy/AmazonEKSFargatePodExecutionRolePolicy",
	}

	fargatePodLauncherCloudwatchPolicy := awsresource.IAMRolePolicy{
		Role: fargatePodLauncherRole.ResourceId(),
		Name: "cloudwatch-logging",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Action: []string{
						"logs:CreateLogStream",
						"logs:CreateLogGroup",
						"logs:DescribeLogStreams",
						"logs:PutLogEvents",
					},
					Resource: []string{"*"},
				},
			},
		},
	}

	eksFargateProfile := awsresource.EKSFargateProfile{
		ClusterName:         eksCluster.ResourceId().ReferenceAttr("name"),
		FargateProfileName:  "datahub-fargate-profile",
		PodExecutionRoleArn: fargatePodLauncherRole.ResourceId().ReferenceAttr("arn"),
		SubnetIds:           []string{fmt.Sprintf("${aws_subnet.%s-%sb-subnet-private.id}", DatahubResourceBaseName, config.Region)},
		Selector: []awsresource.EKSFargateProfileSelector{
			{
				Namespace: "datahub-prod",
				Labels: map[string]string{
					"app.kubernetes.io/name": "datahub-mae-consumer",
				},
			},
			{
				Namespace: "datahub-prod",
				Labels: map[string]string{
					"app.kubernetes.io/name": "datahub-mce-consumer",
				},
			},
			{
				Namespace: "datahub-prod",
				Labels: map[string]string{
					"app.kubernetes.io/name": "datahub-gms",
				},
			},
			{
				Namespace: "datahub-prod",
				Labels: map[string]string{
					"app.kubernetes.io/name": "datahub-frontend",
				},
			},
		},
		Tags: awsresource.ConvertAWSTagsToTagMap(getDatahubTags("eks-fargate-profile")),
	}

	eksResources = append(eksResources,
		&eksCluster,
		&ec2NodeGroup,
		&launchTemplate,
		&eksFargateProfile,
		// &ec2NodeGroupTempForReplacements, // uncomment this line to add a second node group temporarily for replacements
	)

	clusterIam := []tf.Resource{
		&clusterRole,
		&clusterRolePolicy,
		&clusterCustomPolicies,
		&nodeRole,
		&nodeContainerRegistryPolicy,
		&nodeWorkerPolicy,
		&eksCNIPolicy,
		&nodeRoleCloudwatchPolicy,
		&nodeSsmPolicy,
		&clusterVpcPolicy,
		&clusterRoleSSMGetParametesPolicy,
		&gluePolicy,
		&s3WarehouseReadPolicy,
		&fargatePodRole,
		&fargatePodRoleCloudwatchPolicy,
		&fargatePodRoleSSMGetParametesPolicy,
		&dataCatalogBucketReadWritePolicy,
		&fargatePodRoleSTSPolicy,
		&fargatePodRoleESPolicy,
		&fargatePodLauncherRole,
		&fargatePodLauncherServicePolicy,
		&fargatePodLauncherCloudwatchPolicy,
	}

	buildkiteCIResources := buildkiteCIDataplatformDatahub(config)
	clusterIam = append(clusterIam, buildkiteCIResources...)

	return map[string][]tf.Resource{
		"eks":         eksResources,
		"cluster_iam": clusterIam,
	}, nil
}
