package dagsterprojects

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
var eksClusterSecurityGroupId = "sg-09afbe2daa1ccd0ec"

func buildkiteCIDataplatformDagster(config dataplatformconfig.DatabricksConfig) []tf.Resource {
	var resources []tf.Resource

	roleresource := &awsresource.IAMRole{
		Name: "buildkite-ci-dataplatform-dagster",
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
	prefix := awsregionconsts.RegionPrefix[config.Region]
	oidcProvider := DagsterEKSClusterOIDCRegionProvider[config.Region]
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
				fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", config.Region, accountId, "DAGSTER_DATABRICKS_TOKEN"),
				fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", config.Region, accountId, "DAGSTER_DATABRICKS_EU_TOKEN"),
				fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", config.Region, accountId, "DATAHUB_GMS_TOKEN"),
				fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", config.Region, accountId, "DATAHUB_GMS_SERVER"),
				fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", config.Region, accountId, "PAGERDUTY_DE_LOW_URGENCY_KEY"),
				fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", config.Region, accountId, "DATAHUB_DBT_API_TOKEN"),
				fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", config.Region, accountId, "DATAHUB_TABLEAU_TOKEN"),
				fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", config.Region, accountId, "DAGSTER_DATABRICKS_CLIENTID"),
				fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", config.Region, accountId, "DAGSTER_DATABRICKS_SECRET"),
				fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", config.Region, accountId, "DAGSTER_DATABRICKS_EU_CLIENTID"),
				fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", config.Region, accountId, "DAGSTER_DATABRICKS_EU_SECRET"),
				fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", config.Region, accountId, "DAGSTER_DATABRICKS_CA_CLIENTID"),
				fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", config.Region, accountId, "DAGSTER_DATABRICKS_CA_SECRET"),
			},
		},
	}

	clusterRole := awsresource.IAMRole{
		Name:        fmt.Sprintf("%s-%s", DagsterResourceBaseName, "eks"),
		Description: "Role used by EKS for Data Platform's Dagster deployment",
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
			"samsara:service":        "dagster",
			"samsara:team":           strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group":  strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
			"samsara:rnd-allocation": "1",
		},
	}

	clusterBucketReadPolicy := awsresource.IAMRolePolicy{
		Role: clusterRole.ResourceId(),
		Name: "bucket-read-policy",
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
						fmt.Sprintf("arn:aws:s3:::%sdataplatform-dagster", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdataplatform-dagster/*", prefix),
					},
				},
			},
		},
	}

	clusterBucketWritePolicy := awsresource.IAMRolePolicy{
		Role: clusterRole.ResourceId(),
		Name: "bucket-write-policy",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Action: []string{
						"s3:Put*",
					},
					Resource: []string{
						fmt.Sprintf("arn:aws:s3:::%sdataplatform-dagster", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdataplatform-dagster/*", prefix),
					},
				},
			},
		},
	}

	amundsenBucketReadWritePolicy := awsresource.IAMRolePolicy{
		Role: clusterRole.ResourceId(),
		Name: "amundsen-bucket-read-write-policy",
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

	clusterRoleSSMGetParametesPolicy := awsresource.IAMRolePolicy{
		Role: clusterRole.ResourceId(),
		Name: "ssm-get-parameters-policy",
		Policy: policy.AWSPolicy{
			Version:   policy.AWSPolicyVersion,
			Statement: ssmPolicyStatements,
		},
	}

	publicAccessCidrs := append(infraconsts.OfficeCidrs, infraconsts.DevHomeCidrs...)
	publicAccessCidrs = append(publicAccessCidrs, infraconsts.AwsBuildkiteNatGatewayIPs...)
	publicAccessCidrs = append(publicAccessCidrs, infraconsts.AwsBuildkiteNatGatewayIPsForDagster...)

	eksCluster := awsresource.EKSCluster{
		Name:    DagsterResourceBaseName,
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
		Tags: awsresource.ConvertAWSTagsToTagMap(getDagsterTags("eks-cluster")),
	}
	eksCluster.BaseResource.MetaParameters.Lifecycle.PreventDestroy = true

	clusterRolePolicy := awsresource.IAMRolePolicyAttachment{
		Name:      fmt.Sprintf("%s-%s", DagsterResourceBaseName, "eks-policy"),
		Role:      clusterRole.ResourceId().ReferenceAttr("name"),
		PolicyARN: "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy",
	}

	clusterVpcPolicy := awsresource.IAMRolePolicyAttachment{
		Name:      fmt.Sprintf("%s-%s", DagsterResourceBaseName, "eks-vpc-policy"),
		Role:      clusterRole.ResourceId().ReferenceAttr("name"),
		PolicyARN: "arn:aws:iam::aws:policy/AmazonEKSVPCResourceController",
	}

	nodeRole := awsresource.IAMRole{
		Name:        fmt.Sprintf("%s-%s", DagsterResourceBaseName, "eks-nodes"),
		Description: "Role used by EKS Nodes for Data Platform's Dagster deployment",
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
			},
		},
		Tags: map[string]string{
			"samsara:service":        "dagster",
			"samsara:team":           strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group":  strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
			"samsara:rnd-allocation": "1",
		},
	}

	nodeContainerRegistryPolicy := awsresource.IAMRolePolicyAttachment{
		Name:      fmt.Sprintf("%s-%s", DagsterResourceBaseName, "eks-nodes-ro-container-registry"),
		Role:      nodeRole.ResourceId().ReferenceAttr("name"),
		PolicyARN: "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
	}

	nodeWorkerPolicy := awsresource.IAMRolePolicyAttachment{
		Name:      fmt.Sprintf("%s-%s", DagsterResourceBaseName, "eks-nodes-worker-policy"),
		Role:      nodeRole.ResourceId().ReferenceAttr("name"),
		PolicyARN: "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy",
	}

	eksCNIPolicy := awsresource.IAMRolePolicyAttachment{
		Name:      fmt.Sprintf("%s-%s", DagsterResourceBaseName, "eks-cni-policy"),
		Role:      nodeRole.ResourceId().ReferenceAttr("name"),
		PolicyARN: "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy",
	}

	nodeRoleCloudwatchPolicy := awsresource.IAMRolePolicy{
		Name: "ec2-node-logging",
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
		Name: "ssm",
		Role: nodeRole.ResourceId(),
		Policy: policy.AWSPolicy{
			Version:   policy.AWSPolicyVersion,
			Statement: ssmPolicyStatements,
		},
	}

	clusterCustomPolicies := awsresource.IAMRolePolicy{
		Role: clusterRole.ResourceId(),
		Name: "custom-policies",
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
					},
					Resource: []string{"*"},
				},
			},
		},
	}

	nodeGroupScalingConfig := awsresource.EKSNodeGroupScalingConfig{
		DesiredSize: 1,
		MinSize:     1,
		MaxSize:     1,
	}

	ec2NodeGroup := awsresource.EKSNodeGroup{
		Name:          fmt.Sprintf("%s-%s", DagsterResourceBaseName, "eks-node-group"),
		ClusterName:   DagsterResourceBaseName,
		NodeRoleArn:   nodeRole.ResourceId().ReferenceAttr("arn"),
		ScalingConfig: nodeGroupScalingConfig,
		SubnetIds:     v.privateSubnetIds,
		Tags:          awsresource.ConvertAWSTagsToTagMap(getDagsterTags("eks-ec2-nodes")),
		LaunchTemplateASG: awsresource.LaunchTemplateASG{
			LaunchTemplateId: "lt-03b9e20e2ace474e4",
			Version:          "$Latest",
		},
	}

	// uncomment this block to add a second node group temporarily for replacements

	// ec2NodeGroupTempForReplacements := awsresource.EKSNodeGroup{
	// 	Name:          fmt.Sprintf("%s-%s", DagsterResourceBaseName, "eks-node-group-2"),
	// 	ClusterName:   DagsterResourceBaseName,
	// 	NodeRoleArn:   nodeRole.ResourceId().ReferenceAttr("arn"),
	// 	ScalingConfig: nodeGroupScalingConfig,
	// 	SubnetIds:     v.privateSubnetIds,
	// 	Tags:          awsresource.ConvertAWSTagsToTagMap(getDagsterTags("eks-ec2-nodes")),
	// 	LaunchTemplateASG: awsresource.LaunchTemplateASG{
	// 		LaunchTemplateId: "lt-03b9e20e2ace474e4",
	// 		Version:          "$Latest",
	// 	},
	// }

	launchTemplate := awsresource.LaunchTemplate{
		Name:         fmt.Sprintf("%s-%s", DagsterResourceBaseName, "eks-node-group-launch-template"),
		ImageID:      "ami-0233aedb1db01d61e", // golden AMI samsara_golden_eks-1-33_al2023_x86 2026-01-12
		InstanceType: "m5d.xlarge",
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
    apiServerEndpoint: https://E5061F343A8C6C15741CC4791DE88B05.gr7.us-west-2.eks.amazonaws.com
    certificateAuthority: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUMvakNDQWVhZ0F3SUJBZ0lCQURBTkJna3Foa2lHOXcwQkFRc0ZBREFWTVJNd0VRWURWUVFERXdwcmRXSmwKY201bGRHVnpNQjRYRFRJek1EWXhOREF5TXpreE5Gb1hEVE16TURZeE1UQXlNemt4TkZvd0ZURVRNQkVHQTFVRQpBeE1LYTNWaVpYSnVaWFJsY3pDQ0FTSXdEUVlKS29aSWh2Y05BUUVCQlFBRGdnRVBBRENDQVFvQ2dnRUJBSmhYClViMlAwS20zeTFmdG1EekpzVEhmb1RDUkpPN0Q4T25nKzdESlE1K2svMDE4SUJXeXE4bEdJa2tyL2oxMmtCL3YKV3UxR1UxRlJFS2I0UnczK3EwamtDa0o5YnVoMVlvVFk2b3luOCtFUHRBemt5clRRaHhTU2oxUm1xUXk5d2hwTwprTVFqK1lheGY2MkpINGJLM2lITStZZGZ6TTE5R2J2b1dER0hDQXV2dmlVUkFFMm04MTFvSHJRUS9TeVFvdUkxCnE2TlhjZURneG1EYll1WUhIVk1qVllUV1NkYm15MDNKNzlRN0tHNGpNYkxJMXZRdjJvczRmUVQ0RUhMSEMybmEKRzVZY1c5eWdiOTZtdXRYSUZ0WVZ6V05MT2hsdGw5NzlVU01mS2g5RmVSVTg0QlJaQ1hEK2MxbWJMTGdHbzlWaAptQS9qVk43Q2hGNlIrSU5oNGo4Q0F3RUFBYU5aTUZjd0RnWURWUjBQQVFIL0JBUURBZ0trTUE4R0ExVWRFd0VCCi93UUZNQU1CQWY4d0hRWURWUjBPQkJZRUZHcEd3K0RWa3BmYXdPQ1gvb0V3bE1NNEZuMlRNQlVHQTFVZEVRUU8KTUF5Q0NtdDFZbVZ5Ym1WMFpYTXdEUVlKS29aSWh2Y05BUUVMQlFBRGdnRUJBRER6dENvNUV2S3hzT1kzRXh6NQpEU3lqaUtRZUVTSVN2M2t3aEJaaGRUOGI5MFNCbi91ekNSM2JaRjAvaTVZUGpLZzNMSVd2UEJTSi9MWjhodDV6CmVtMGlTMkhEeVNnZXN6bWt1b1VhRHl4WlpvU01ySFdGNFBXODZveTFYWnRmVU54Z0RhbUM3djZnTjN2YjhmVXcKb2VFWS9zRXJaQ0d4bk1KT3RRM2RCRnNOaEZMdWRKWUIrazNWUEF1OGtwTGxaK21RTGZqNGp6QUhuT0ZwWE56VgpJMnU3V1hLQUZTRXM1RWxlS1lPMkt2Mks4QW5pVmdyNXowUjM2SENrK0s3L3hPS21RMzFWSnpzUldPV2toaUpZCmFRekg0UURMaVlZc25tS1Q3L2FTbFZCNzFDRjJONWpxTWlMV1Z0djlKK1lFVmRJWWpwdUV6MmZWNHJsNUQ1TDcKSFNZPQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg==
    cidr: 172.20.0.0/16
    name: dagster
  kubelet:
    config:
      maxPods: 58
      clusterDNS:
      - 172.20.0.10
    flags:
    - "--node-labels=eks.amazonaws.com/nodegroup-image=ami-0233aedb1db01d61e,eks.amazonaws.com/capacityType=ON_DEMAND,eks.amazonaws.com/nodegroup=dagster-eks-node-group"

--//--`,
		VPCSecurityGroupIDs: []string{eksClusterSecurityGroupId},
		BlockDeviceMappings: []awsresource.LaunchTemplateBlockDeviceMapping{
			{
				DeviceName: "/dev/xvda",
				EBS: awsresource.LaunchTemplateBlockDeviceEBS{
					VolumeType: "gp2",
					VolumeSize: 30,
				},
			},
		},
	}

	asgName := fmt.Sprintf("${%s.resources[0].autoscaling_groups[0].name}", ec2NodeGroup.ResourceId().String())
	for _, tag := range getDagsterTags("eks-ec2-nodes") {
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
		Name:        fmt.Sprintf("%s-%s", DagsterResourceBaseName, "eks-fargate-pods"),
		Description: "Role assumed by Fargate Pods running on Dagster eks cluster",
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
							fmt.Sprintf("%s:sub", oidcProvider): "system:serviceaccount:dagster-prod:dagster-user-deployments-user-deployments",
						},
					},
				},
			},
		},
		Tags: awsresource.ConvertAWSTagsToTagMap(getDagsterTags("iam-eks-fargate-pods")),
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

	databricksWorkspaceBucketReadWritePolicy := awsresource.IAMRolePolicy{
		Role: fargatePodRole.ResourceId(),
		Name: "databricks-workspace-s3-readwrite-policy",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Action: []string{
						"s3:Put*",
						"s3:Get*",
						"s3:List*",
						"s3:DeleteObject",
					},
					Resource: []string{
						fmt.Sprintf("arn:aws:s3:::samsara-databricks-dev-us-west-2-root/%s/%d/dagster_staging/*", dataplatformconsts.DatabricksDevUSShardName, dataplatformconsts.DatabricksDevUSWorkspaceId),
						fmt.Sprintf("arn:aws:s3:::samsara-eu-databricks-dev-eu-west-1-root/%s/%d/dagster_staging/*", dataplatformconsts.DatabricksDevEUShardName, dataplatformconsts.DatabricksDevEUWorkspaceId),
						fmt.Sprintf("arn:aws:s3:::samsara-ca-databricks-dev-ca-central-1-root/%s/%d/dagster_staging/*", dataplatformconsts.DatabricksDevCAShardName, dataplatformconsts.DatabricksDevCAWorkspaceId),
						fmt.Sprintf("arn:aws:s3:::samsara-databricks-dev-us-west-2-root/%s/%d/dagster_steps/*", dataplatformconsts.DatabricksDevUSShardName, dataplatformconsts.DatabricksDevUSWorkspaceId),
						fmt.Sprintf("arn:aws:s3:::samsara-eu-databricks-dev-eu-west-1-root/%s/%d/dagster_steps/*", dataplatformconsts.DatabricksDevEUShardName, dataplatformconsts.DatabricksDevEUWorkspaceId),
						fmt.Sprintf("arn:aws:s3:::samsara-ca-databricks-dev-ca-central-1-root/%s/%d/dagster_steps/*", dataplatformconsts.DatabricksDevCAShardName, dataplatformconsts.DatabricksDevCAWorkspaceId),
					},
				},
				{
					Effect: "Allow",
					Action: []string{
						"s3:List*",
					},
					Resource: []string{
						"arn:aws:s3:::samsara-databricks-dev-us-west-2-root",
						"arn:aws:s3:::samsara-eu-databricks-dev-eu-west-1-root",
						"arn:aws:s3:::samsara-ca-databricks-dev-ca-central-1-root",
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

	fargatePodRoleSecretsManagerPolicy := awsresource.IAMRolePolicy{
		Role: fargatePodRole.ResourceId(),
		Name: "secrets-manager-rds-access",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Action: []string{
						"secretsmanager:GetSecretValue",
						"secretsmanager:GetResourcePolicy",
					},
					Resource: []string{
						"arn:aws:secretsmanager:us-west-2:160875005101:secret:prod/db/frequentstops_database_password-*",
						"arn:aws:secretsmanager:us-west-2:160875005101:secret:prod/db/frequentstops_database_replica_password-*",
						"arn:aws:secretsmanager:eu-west-1:160875005101:secret:prod/db/frequentstops_database_password-*",
						"arn:aws:secretsmanager:eu-west-1:160875005101:secret:prod/db/frequentstops_database_replica_password-*",
						"arn:aws:secretsmanager:ca-central-1:160875005101:secret:prod/db/frequentstops_database_password-*",
						"arn:aws:secretsmanager:ca-central-1:160875005101:secret:prod/db/frequentstops_database_replica_password-*",
					},
				},
			},
		},
	}

	fargatePodLauncherRole := awsresource.IAMRole{
		Name:        fmt.Sprintf("%s-%s", DagsterResourceBaseName, "eks-fargate-pod-launcher"),
		Description: "Role used by Dagster EKS to launch pods on Fargate",
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
		Tags: awsresource.ConvertAWSTagsToTagMap(getDagsterTags("eks-fargate-pod-launcher")),
	}

	fargatePodLauncherServicePolicy := awsresource.IAMRolePolicyAttachment{
		Name:      fmt.Sprintf("%s-%s", DagsterResourceBaseName, "eks-fargate-pod-launcher-policy"),
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
		FargateProfileName:  "dagster-fargate-profile",
		PodExecutionRoleArn: fargatePodLauncherRole.ResourceId().ReferenceAttr("arn"),
		SubnetIds:           v.privateSubnetIds,
		Selector: []awsresource.EKSFargateProfileSelector{
			{
				Namespace: "dagster-prod",
				Labels: map[string]string{
					"app.kubernetes.io/name": "dagster",
				},
			},
			{
				Namespace: "dagster-prod",
				Labels: map[string]string{
					"app.kubernetes.io/name": "dagster-user-deployments",
				},
			},
		},
		Tags: awsresource.ConvertAWSTagsToTagMap(getDagsterTags("eks-fargate-profile")),
	}

	eksResources = append(eksResources,
		&eksCluster,
		&ec2NodeGroup,
		// &ec2NodeGroupTempForReplacements, // uncomment this line to add a second node group temporarily for replacements
		&launchTemplate,
		&eksFargateProfile,
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
		&clusterBucketReadPolicy,
		&clusterBucketWritePolicy,
		&clusterRoleSSMGetParametesPolicy,
		&amundsenBucketReadWritePolicy,
		&fargatePodRole,
		&fargatePodRoleCloudwatchPolicy,
		&fargatePodRoleSSMGetParametesPolicy,
		&dataCatalogBucketReadWritePolicy,
		&databricksWorkspaceBucketReadWritePolicy,
		&fargatePodRoleSTSPolicy,
		&fargatePodRoleSecretsManagerPolicy,
		&fargatePodLauncherRole,
		&fargatePodLauncherServicePolicy,
		&fargatePodLauncherCloudwatchPolicy,
	}

	buildkiteCIResources := buildkiteCIDataplatformDagster(config)
	clusterIam = append(clusterIam, buildkiteCIResources...)

	return map[string][]tf.Resource{
		"eks":         eksResources,
		"cluster_iam": clusterIam,
	}, nil
}
