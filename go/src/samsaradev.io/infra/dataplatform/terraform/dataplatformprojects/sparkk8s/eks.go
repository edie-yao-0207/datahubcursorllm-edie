package sparkk8s

import (
	"fmt"
	"strings"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/team"
)

// Creates all the required resources to setup the Spark EKS cluster
func sparkEksResources(config dataplatformconfig.DatabricksConfig, v vpcIds) (map[string][]tf.Resource, error) {

	// Create the EKS cluster role
	clusterRole := awsresource.IAMRole{
		Name:        fmt.Sprintf("%s-%s", SparkK8sResourceBaseName, "eks"),
		Description: "Role used by EKS for Data Platform's Spark EKS deployment",
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
			},
		},
		Tags: map[string]string{
			"samsara:service":       fmt.Sprintf("%s-%s", SparkK8sResourceBaseName, "eks"),
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}

	// Add office and dev home CIDRs to the public access CIDRs
	publicAccessCidrs := append(infraconsts.OfficeCidrs, infraconsts.DevHomeCidrs...)

	// Create the EKS cluster
	eksCluster := awsresource.EKSCluster{
		Name:    SparkK8sResourceBaseName,
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
		Tags: awsresource.ConvertAWSTagsToTagMap(getEksTags("poc-cluster")),
	}

	// Create the EKS cluster role policy
	clusterRolePolicy := awsresource.IAMRolePolicyAttachment{
		Name:      fmt.Sprintf("%s-%s", SparkK8sResourceBaseName, "policy"),
		Role:      clusterRole.ResourceId().ReferenceAttr("name"),
		PolicyARN: "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy",
	}

	// Create the EKS cluster VPC policy
	clusterVpcPolicy := awsresource.IAMRolePolicyAttachment{
		Name:      fmt.Sprintf("%s-%s", SparkK8sResourceBaseName, "vpc-policy"),
		Role:      clusterRole.ResourceId().ReferenceAttr("name"),
		PolicyARN: "arn:aws:iam::aws:policy/AmazonEKSVPCResourceController",
	}

	// Create the EKS cluster node service role
	nodeRole := awsresource.IAMRole{
		Name:        fmt.Sprintf("%s-%s", SparkK8sResourceBaseName, "nodes"),
		Description: "Role used by EKS Nodes and Karpenter for Data Platform's Spark EKS deployment",
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
			"samsara:service":       fmt.Sprintf("%s-%s", SparkK8sResourceBaseName, "nodes"),
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}

	karpenterControllerRoleName := fmt.Sprintf("%s-%s", SparkK8sResourceBaseName, "karpenter-controller-role")
	karpenterControllerRole := awsresource.IAMRole{
		Name:        karpenterControllerRoleName,
		Description: "Role used by EKS Nodes and Karpenter for Data Platform's Spark EKS deployment",
		AssumeRolePolicy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Principal: map[string]string{
						"Federated": fmt.Sprintf("arn:aws:iam::%d:oidc-provider/%s", infraconsts.SamsaraAWSDatabricksAccountID, SparkK8slusterOIDCProvider),
					},
					Action: []string{
						"sts:AssumeRole",
						"sts:AssumeRoleWithWebIdentity",
					},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{
							fmt.Sprintf("%s:aud", SparkK8slusterOIDCProvider): "sts.amazonaws.com",
							fmt.Sprintf("%s:sub", SparkK8slusterOIDCProvider): "system:serviceaccount:karpenter:karpenter",
						},
					},
				},
			},
		},
		Tags: map[string]string{
			"samsara:service":       karpenterControllerRoleName,
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}

	karpenterControllerCustomPolicy := awsresource.IAMRolePolicy{
		Name: fmt.Sprintf("%s-%s", SparkK8sResourceBaseName, "karpenter-controller-custom-policy"),
		Role: karpenterControllerRole.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect:   "Allow",
					Sid:      "Karpenter",
					Resource: []string{"*"},
					Action: []string{
						"iam:PassRole",
						"ssm:GetParameter",
						"ec2:DescribeImages",
						"ec2:RunInstances",
						"ec2:DescribeSubnets",
						"ec2:DescribeSecurityGroups",
						"ec2:DescribeLaunchTemplates",
						"ec2:DescribeInstances",
						"ec2:DescribeInstanceTypes",
						"ec2:DescribeInstanceTypeOfferings",
						"ec2:DescribeAvailabilityZones",
						"ec2:DeleteLaunchTemplate",
						"ec2:CreateTags",
						"ec2:CreateLaunchTemplate",
						"ec2:CreateFleet",
						"ec2:DescribeSpotPriceHistory",
						"pricing:GetProducts",
					},
				},
				{
					Effect:   "Allow",
					Resource: []string{"*"},
					Sid:      "ConditionalEC2Termination",
					Action:   []string{"ec2:TerminateInstances"},
					Condition: &policy.AWSPolicyCondition{
						StringLike: map[string][]string{
							"ec2:ResourceTag/karpenter.sh/provisioner-name": {"*"},
						},
					},
				},
				{
					Effect:   "Allow",
					Resource: []string{fmt.Sprintf("arn:aws:iam::%d:role/%s", infraconsts.SamsaraAWSDatabricksAccountID, nodeRole.ResourceId().ReferenceAttr("name"))},
					Sid:      "PassNodeIAMRole",
					Action:   []string{"iam:PassRole"},
				},
				{
					Effect:   "Allow",
					Resource: []string{fmt.Sprintf("arn:aws:eks:%s:%d:cluster/%s", infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSDatabricksAccountID, eksCluster.Name)},
					Sid:      "EKSClusterEndpointLookup",
					Action:   []string{"eks:DescribeCluster"},
				},
			},
		},
	}

	// Create the EKS cluster node ECR policy
	nodeContainerRegistryPolicy := awsresource.IAMRolePolicyAttachment{
		Name:      fmt.Sprintf("%s-%s", SparkK8sResourceBaseName, "nodes-ro-container-registry"),
		Role:      nodeRole.ResourceId().ReferenceAttr("name"),
		PolicyARN: "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
	}

	// Create the EKS cluster node worker policy
	nodeWorkerPolicy := awsresource.IAMRolePolicyAttachment{
		Name:      fmt.Sprintf("%s-%s", SparkK8sResourceBaseName, "nodes-worker-policy"),
		Role:      nodeRole.ResourceId().ReferenceAttr("name"),
		PolicyARN: "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy",
	}

	nodeSSManagedInstanceCorePolicy := awsresource.IAMRolePolicyAttachment{
		Name:      fmt.Sprintf("%s-%s", SparkK8sResourceBaseName, "nodes-sys-manager-policy"),
		Role:      nodeRole.ResourceId().ReferenceAttr("name"),
		PolicyARN: "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore",
	}

	// Create the EKS cluster node CNI policy
	eksCNIPolicy := awsresource.IAMRolePolicyAttachment{
		Name:      fmt.Sprintf("%s-%s", SparkK8sResourceBaseName, "cni-policy"),
		Role:      nodeRole.ResourceId().ReferenceAttr("name"),
		PolicyARN: "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy",
	}

	nodeInstanceProfile := awsresource.IAMInstanceProfile{
		Name: fmt.Sprintf("%s-%s", SparkK8sResourceBaseName, "node-instance-profile"),
		Role: nodeRole.ResourceId().ReferenceAttr("name"),
	}

	// Create the EKS cluster node cloudwatch policy
	nodeRoleCloudwatchPolicy := awsresource.IAMRolePolicy{
		Name: fmt.Sprintf("%s-%s", SparkK8sResourceBaseName, "ec2-node-logging"),
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

	clusterCustomPolicies := awsresource.IAMRolePolicy{
		Role: clusterRole.ResourceId(),
		Name: fmt.Sprintf("%s-%s", SparkK8sResourceBaseName, "custom-policies"),
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

	clusterRoleSSMGetParametesPolicy := awsresource.IAMRolePolicy{
		Name: fmt.Sprintf("%s-%s", SparkK8sResourceBaseName, "ssm-policies"),
		Role: clusterRole.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Action: []string{
						"ssm:GetParameter*",
					},
					Resource: []string{
						fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSDatabricksAccountID, dataplatformconsts.ParameterDatadogAppToken),
						fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSDatabricksAccountID, dataplatformconsts.ParameterDatadogApiToken),
					},
				},
			},
		},
	}

	nodeGroupScalingConfig := awsresource.EKSNodeGroupScalingConfig{
		DesiredSize: 0,
		MinSize:     0,
		MaxSize:     10,
	}

	ec2NodeGroup := awsresource.EKSNodeGroup{
		Name:          fmt.Sprintf("%s-%s", SparkK8sResourceBaseName, "on-demand-node-group"),
		ClusterName:   SparkK8sResourceBaseName,
		NodeRoleArn:   nodeRole.ResourceId().ReferenceAttr("arn"),
		ScalingConfig: nodeGroupScalingConfig,
		SubnetIds:     v.privateSubnetIds,
		InstanceTypes: []string{
			"m5d.large",
		},
		Tags: awsresource.ConvertAWSTagsToTagMap(getEksTags("poc-ec2-on-demand-nodes")),
	}

	nodeGroupScalingConfig = awsresource.EKSNodeGroupScalingConfig{
		DesiredSize: 2,
		MinSize:     0,
		MaxSize:     10,
	}

	// TODO add spot node support in TF provider.
	// This should still bring up on-demand nodes.
	ec2SpotNodeGroup := awsresource.EKSNodeGroup{
		Name:          fmt.Sprintf("%s-%s", SparkK8sResourceBaseName, "on-spot-node-group"),
		ClusterName:   SparkK8sResourceBaseName,
		NodeRoleArn:   nodeRole.ResourceId().ReferenceAttr("arn"),
		ScalingConfig: nodeGroupScalingConfig,
		SubnetIds:     v.privateSubnetIds,
		InstanceTypes: []string{
			"m5d.large",
		},
		Tags: awsresource.ConvertAWSTagsToTagMap(getEksTags("poc-ec2-on-spot-nodes")),
	}

	bucket := awsresource.S3Bucket{
		Name:                                 "spark-k8s-poc-data-bucket",
		Bucket:                               "spark-k8s-poc-data",
		Region:                               config.Region,
		ACL:                                  awsresource.S3BucketACLPrivate,
		DefineLifecycleRulesInBucketResource: true,
		DefineVersioningInBucketResource:     true,
		BaseResource: tf.BaseResource{
			TeamOwners: []string{team.DataPlatform.TeamName},
			MetaParameters: tf.MetaParameters{
				Lifecycle: tf.Lifecycle{
					PreventDestroy: true,
				},
			},
		},
		Tags: map[string]string{
			"samsara:service":       "spark-k8s-poc-data",
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}

	bucketARN := tf.Reference{
		ResourceId: bucket.ResourceId(),
		Output:     "arn",
	}.Interpolation()

	// role assumed by service account of spark driver/executor pods
	jobRole := awsresource.IAMRole{
		Name:        "spark-k8s-poc-s3-role-v1",
		Description: "Role used by service account associated with the spark driver/executor",
		AssumeRolePolicy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Principal: map[string]string{
						"Federated": fmt.Sprintf("arn:aws:iam::%d:oidc-provider/%s", infraconsts.SamsaraAWSDatabricksAccountID, SparkK8slusterOIDCProvider),
					},
					Action: []string{
						"sts:AssumeRole",
						"sts:AssumeRoleWithWebIdentity",
					},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{
							fmt.Sprintf("%s:aud", SparkK8slusterOIDCProvider): "sts.amazonaws.com",
							fmt.Sprintf("%s:sub", SparkK8slusterOIDCProvider): "system:serviceaccount:default:demo-sa", // demo-sa is the service account used by the spark driver/executor
						},
					},
				},
			},
		},
		Tags: map[string]string{
			"samsara:service":       "spark-k8s-poc-s3-role-v1",
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}

	// Policy used by the spark driver/executor
	jobRoleCustomPolicy := awsresource.IAMRolePolicy{
		Name: "spark-k8s-poc-s3-policy-v1",
		Role: jobRole.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Sid:    "AccessFromSparkPodstoPocBucket",
					Effect: "Allow",
					Action: []string{
						"s3:*",
					},
					Resource: []string{
						bucketARN,
						bucketARN + "/*",
					},
				},
			},
		},
	}

	jobS3ReadOnlyCustomPolicy := awsresource.IAMRolePolicy{
		Name: "spark-k8s-poc-s3-readonly-policy",
		Role: jobRole.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Sid:    "ReadAccessFromSparkPodstoDataBuckets",
					Effect: "Allow",
					Action: []string{
						"s3:Get*",
						"s3:List*",
					},
					Resource: []string{
						"arn:aws:s3:::samsara-rds-export",
						"arn:aws:s3:::samsara-rds-export/*",
						"arn:aws:s3:::samsara-dataplatform-deployed-artifacts",
						"arn:aws:s3:::samsara-dataplatform-deployed-artifacts/*",
					},
				},
			},
		},
	}

	jobSSMReadCustomPolicy := awsresource.IAMRolePolicy{
		Name: "spark-k8s-poc-ssm-read-policy",
		Role: jobRole.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Sid:    "AllowSparkPodsToFetchDataDogTokens",
					Effect: "Allow",
					Action: []string{
						"ssm:GetParameter*",
					},
					Resource: []string{
						fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSDatabricksAccountID, dataplatformconsts.ParameterDatadogAppToken),
						fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/%s", infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSDatabricksAccountID, dataplatformconsts.ParameterDatadogApiToken),
					},
				},
			},
		},
	}

	jobGlueCustomPolicy := awsresource.IAMRolePolicy{
		Name: "spark-k8s-poc-glue-policy",
		Role: jobRole.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Action: []string{
						"glue:BatchCreatePartition",
						"glue:BatchDeletePartition",
						"glue:BatchGetPartition",
						"glue:CreateDatabase",
						"glue:CreateTable",
						"glue:CreateUserDefinedFunction",
						"glue:DeleteDatabase",
						"glue:DeletePartition",
						"glue:DeleteTable",
						"glue:DeleteUserDefinedFunction",
						"glue:GetDatabase",
						"glue:GetDatabases",
						"glue:GetPartition",
						"glue:GetPartitions",
						"glue:GetTable",
						"glue:GetTables",
						"glue:GetUserDefinedFunction",
						"glue:GetUserDefinedFunctions",
						"glue:UpdateDatabase",
						"glue:UpdatePartition",
						"glue:UpdateTable",
						"glue:UpdateUserDefinedFunction",
					},
					Resource: []string{
						"arn:aws:glue:us-west-2:492164655156:catalog",
						"arn:aws:glue:us-west-2:492164655156:database/k8s_*", // access to only databases starting with k8s_
						"arn:aws:glue:us-west-2:492164655156:table/k8s_*",    // access to only tables in databases starting with databases k8s_
						"arn:aws:glue:us-west-2:492164655156:database/default",
						"arn:aws:glue:us-west-2:492164655156:database/parquet_k8s*",
						"arn:aws:glue:us-west-2:492164655156:table/parquet_k8s*",
						"arn:aws:glue:us-west-2:492164655156:database/global_temp*",
					},
				},
			},
		},
	}

	bucketPolicy := awsresource.S3BucketPolicy{
		Bucket: bucket.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Sid: "AccessFromSparkPodstoPocBucket",
					Principal: map[string]string{
						"AWS": fmt.Sprintf("arn:aws:iam::%d:role/%s", infraconsts.SamsaraAWSDatabricksAccountID, jobRole.ResourceId().ReferenceAttr("name")),
					},
					Effect: "Allow",
					Action: []string{
						"s3:*",
					},
					Resource: []string{
						bucketARN,
						bucketARN + "/*",
					},
				},

				{
					Principal: map[string]string{
						"AWS": fmt.Sprintf("arn:aws:iam::%d:root", infraconsts.SamsaraAWSAccountID),
					},
					Effect: "Allow",
					Action: []string{"s3:*"},
					Resource: []string{
						bucketARN,
						bucketARN + "/*",
					},
				},
			},
		},
	}

	cluster_iam_roles := []tf.Resource{
		&clusterRole,
		&clusterRolePolicy,
		&clusterCustomPolicies,
		&nodeRole,
		&nodeContainerRegistryPolicy,
		&nodeWorkerPolicy,
		&nodeSSManagedInstanceCorePolicy,
		&eksCNIPolicy,
		&nodeRoleCloudwatchPolicy,
		&nodeInstanceProfile,
		&clusterVpcPolicy,
		&clusterRoleSSMGetParametesPolicy,
		&karpenterControllerRole,
		&karpenterControllerCustomPolicy,
		&bucket,
		&bucketPolicy,
		&jobRoleCustomPolicy,
		&jobRole,
		&jobS3ReadOnlyCustomPolicy,
		&jobSSMReadCustomPolicy,
		&jobGlueCustomPolicy,
	}

	return map[string][]tf.Resource{
		"eks": {
			&eksCluster,
			&ec2NodeGroup,
			&ec2SpotNodeGroup,
		},
		"cluster_iam": cluster_iam_roles,
	}, nil
}
