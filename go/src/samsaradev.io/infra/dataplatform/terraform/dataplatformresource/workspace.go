package dataplatformresource

import (
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/definitions/vpc"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/dataplatformhelpers"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/team"
)

// Helper function to create an instance profile arn knowing the workspace/provider group and the name
func InstanceProfileArn(providerGroup string, name string) (string, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return "", oops.Wrapf(err, "")
	}
	accId := config.DatabricksAWSAccountId
	return fmt.Sprintf("arn:aws:iam::%d:instance-profile/%s", accId, name), nil
}

// Helper function to create the arn of an iam role the workspace/provider group
func IAMRoleArn(providerGroup string, name string) (string, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return "", oops.Wrapf(err, "")
	}
	accId := config.DatabricksAWSAccountId
	return fmt.Sprintf("arn:aws:iam::%d:role/%s", accId, name), nil
}

type Role struct {
	Name             string
	Description      string
	AssumeRolePolicy policy.AWSPolicyStatement
	InlinePolicies   []policy.AWSPolicyStatement
}

func (c Role) RoleResource() *awsresource.IAMRole {
	return &awsresource.IAMRole{
		Name:        c.Name,
		Description: c.Description,
		AssumeRolePolicy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				c.AssumeRolePolicy,
			},
		},
		Tags: map[string]string{
			"samsara:service":       fmt.Sprintf("%s-role", c.Name),
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}
}

func (c Role) InlinePoliciesResource() *awsresource.IAMRolePolicy {
	return &awsresource.IAMRolePolicy{
		Name: "inline",
		Role: c.RoleResource().ResourceId(),
		Policy: policy.AWSPolicy{
			Version:   policy.AWSPolicyVersion,
			Statement: c.InlinePolicies,
		},
	}
}

func (c Role) Resources() []tf.Resource {
	return []tf.Resource{
		c.RoleResource(),
		c.InlinePoliciesResource(),
	}
}

type VPC struct {
	Environment       string
	Region            string
	Cidr16            int
	ProviderGroup     string
	EnableVpcFlowLogs bool
}

func (c VPC) getSecurityGroupEgress(databricksControlPlaneIPs []string) []*awsresource.IngressEgressBlock {

	// These are new restrictive ingress.
	// Enable only for staging and Canada.
	// https://docs.databricks.com/en/security/network/classic/customer-managed-vpc.html#security-groups.
	return []*awsresource.IngressEgressBlock{
		&awsresource.IngressEgressBlock{
			Description: "allow-tcp-egress-internal",
			Protocol:    "tcp",
			FromPort:    0,
			ToPort:      65535,
			Self:        true,
		},
		&awsresource.IngressEgressBlock{
			Description: "allow-udp-egress-internal",
			Protocol:    "udp",
			FromPort:    0,
			ToPort:      65535,
			Self:        true,
		},

		&awsresource.IngressEgressBlock{
			Description: "allow-443-egress",
			Protocol:    "tcp",
			FromPort:    443,
			ToPort:      443,
			CidrBlocks:  []string{"0.0.0.0/0"},
		},

		&awsresource.IngressEgressBlock{
			Description: "allow-3306-egress",
			Protocol:    "tcp",
			FromPort:    3306,
			ToPort:      3306,
			CidrBlocks:  []string{"0.0.0.0/0"},
		},

		&awsresource.IngressEgressBlock{
			Description: "allow-6666-egress",
			Protocol:    "tcp",
			FromPort:    6666,
			ToPort:      6666,
			CidrBlocks:  []string{"0.0.0.0/0"},
		},

		&awsresource.IngressEgressBlock{
			Description: "allow-2443-egress",
			Protocol:    "tcp",
			FromPort:    2443,
			ToPort:      2443,
			CidrBlocks:  []string{"0.0.0.0/0"},
		},
		&awsresource.IngressEgressBlock{
			Description: "allow-8443-egress",
			Protocol:    "tcp",
			FromPort:    8443,
			ToPort:      8443,
			CidrBlocks:  []string{"0.0.0.0/0"},
		},

		&awsresource.IngressEgressBlock{
			Description: "allow-8444-egress",
			Protocol:    "tcp",
			FromPort:    8444,
			ToPort:      8444,
			CidrBlocks:  []string{"0.0.0.0/0"},
		},
		&awsresource.IngressEgressBlock{
			Description: "allow-8445-8451-egress",
			Protocol:    "tcp",
			FromPort:    8445,
			ToPort:      8451,
			CidrBlocks:  []string{"0.0.0.0/0"},
		},
		&awsresource.IngressEgressBlock{
			Description: "allow-22-egress",
			Protocol:    "tcp",
			FromPort:    22,
			ToPort:      22,
			CidrBlocks:  []string{"0.0.0.0/0"},
		},
		&awsresource.IngressEgressBlock{
			Description: "allow-80-egress",
			Protocol:    "tcp",
			FromPort:    80,
			ToPort:      80,
			CidrBlocks:  []string{"0.0.0.0/0"},
		},
	}

}

func (c VPC) getSecurityGroupIngress(databricksControlPlaneIPs []string) []*awsresource.IngressEgressBlock {

	// These are new restrictive ingress.
	// Enable only for staging and Canada.
	// https://docs.databricks.com/en/security/network/classic/customer-managed-vpc.html#security-groups.
	return []*awsresource.IngressEgressBlock{
		&awsresource.IngressEgressBlock{
			Description: "allow-tcp-ingress-internal",
			Protocol:    "tcp",
			FromPort:    0,
			ToPort:      65535,
			Self:        true,
		},
		&awsresource.IngressEgressBlock{
			Description: "allow-udp-ingress-internal",
			Protocol:    "udp",
			FromPort:    0,
			ToPort:      65535,
			Self:        true,
		},

		&awsresource.IngressEgressBlock{
			Description: "allow-databricks-control-plane-22",
			Protocol:    "tcp",
			FromPort:    22,
			ToPort:      22,
			CidrBlocks:  databricksControlPlaneIPs,
		},

		&awsresource.IngressEgressBlock{
			Description: "allow-databricks-control-plane-5557",
			Protocol:    "tcp",
			FromPort:    5557,
			ToPort:      5557,
			CidrBlocks:  databricksControlPlaneIPs,
		},
	}

}

// https://docs.databricks.com/administration-guide/cloud-configurations/aws/customer-managed-vpc.html#security-groups
func (c VPC) SecurityGroup(v2 bool) *awsresource.SecurityGroup {
	databricksControlPlaneIPs := []string{
		"52.27.216.188/32",  // us-west-{1,2}: Control plane NAT instance
		"52.35.116.98/32",   // us-west-{1,2}: Legacy NAT instance
		"52.88.31.94/32",    // us-west-{1,2}: Legacy NAT instance
		"54.156.226.103/32", // us-east-1: Control plane NAT instance
		"18.221.200.169/32", // us-east-2: Control plane NAT instance
		"46.137.47.49/32",   // eu-west-1: Control plane NAT instance
		"35.183.59.105/32",  // ca-central-1: Control plane NAT instance
	}

	environment := c.Environment

	// If v2 is true, we are using the new VPC define and the security group name should be different.
	// Since both VPC defines are co-existing, we need to differentiate the security group names to avoid conflicts.
	if v2 {
		environment = environment + "-v2"
	}

	name := fmt.Sprintf("%s-%s-cluster-sg", environment, c.Region)
	return &awsresource.SecurityGroup{
		Name:        name,
		Description: fmt.Sprintf("Default security group for Databricks-managed clusters in workspace %s in region %s", environment, c.Region),
		VpcID:       awsresource.VPCResourceId(fmt.Sprintf("%s-%s-vpc", environment, c.Region)).ReferenceAttr("id"),

		IngressBlocks: c.getSecurityGroupIngress(databricksControlPlaneIPs),

		EgressBlocks: c.getSecurityGroupEgress(databricksControlPlaneIPs),

		Tags: []*awsresource.Tag{
			{
				Key:   "Name",
				Value: name,
			},
		},
	}
}

func (c VPC) legacyVpcDefine() bool {
	// These are exisiting workspaces that were created using Legacy VPC define.
	// Anything new should use the new VPC define.

	if c.Environment == "dev-staging" {
		// dev-staging will use the new VPC define.
		// dev-staging environment is part of databricks-us provider group.
		return false
	}

	if c.ProviderGroup == "databricks-us" || c.ProviderGroup == "databricks-eu" || c.ProviderGroup == "databricks-prod-us" {
		return true
	}
	return false
}

func (c E2Workspace) vpcFlowLogsResource() []tf.Resource {
	resourceName := fmt.Sprintf("vpc-flow-logs-%s-%s", c.Environment, c.Region)
	// TODO: Update this to use the new VPC define.
	// Once all workspaces are migrated to the new VPC define, we can remove the existing VPC define and point vpcFlowLogsResource to the new VPC.
	vpcID := awsresource.VPCResourceId(fmt.Sprintf("%s-%s-vpc", c.Environment, c.Region)).ReferenceAttr("id")

	// We have migrated to the new VPC v2 define for EU & US prod for SCC
	if c.Region != infraconsts.SamsaraAWSCARegion && c.Environment != "dev-staging" {
		environment := c.Environment + "-v2"
		vpcID = awsresource.VPCResourceId(fmt.Sprintf("%s-%s-vpc", environment, c.Region)).ReferenceAttr("id")
	}
	return VpcFlowLogsResource(
		resourceName,
		vpcID,
		c.Region,
	)
}

func (c VPC) Resources() ([]tf.Resource, error) {

	// Additional VPC endpoint services to be added to the VPC.
	// Note S3 and DynamoDB are already added by default.
	additionalVpcEndpointServices := []vpc.EndpointInterfaceService{
		vpc.SNS,
		vpc.SQS,
		vpc.Logs,               // CloudWatch Logs
		vpc.KinesisDataStreams, // Kinesis Data Streams
	}

	tags := []*awsresource.Tag{
		{
			Key:   "samsara:team",
			Value: strings.ToLower(team.DataPlatform.TeamName),
		},
		{
			Key:   "samsara:product-group",
			Value: strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
		{
			Key:   "samsara:service",
			Value: "databricks-vpc",
		},
	}

	var err error
	var resources *vpc.VPCResources

	// Resources for the new VPC define.
	// They need to coexist with the existing VPC define until all workspaces are migrated to the new VPC define.
	// TODO: Once all workspaces are migrated to the new VPC define, we can remove the existing VPC define and point vpc flow logs & DNSResolverQueryLogs, etc. to the new VPC.
	var resources_new *vpc.VPCResources
	environment_new := c.Environment + "-v2"

	if c.legacyVpcDefine() {
		azs := []string{}

		if c.Region == infraconsts.SamsaraAWSDefaultRegion {
			// TODO: At the moment we would like to only create new (fourth) az and its subnet and not use it.
			// Once its tested, we will start rolling this out to jobs.
			// Once its rolled to all jobs, we add it to GetDatabricksAvailabilityZones and remove this.
			allAzs := []string{"us-west-2a", "us-west-2b", "us-west-2c", "us-west-2d"}
			azs, err = dataplatformhelpers.GetSupportedAzs(allAzs)
			if err != nil {
				return nil, oops.Errorf("Error getting Az for region %s", c.Region)
			}
		} else {
			allAzs := infraconsts.GetDatabricksAvailabilityZones(c.Region)
			if len(allAzs) == 0 {
				return nil, oops.Errorf("%s", "No Databricks AZ found for region: "+c.Region)
			}
			azs, err = dataplatformhelpers.GetSupportedAzs(allAzs)
			if err != nil {
				return nil, oops.Errorf("Error getting Az for region %s", c.Region)
			}
		}

		resources, err = vpc.DONOTUSE_DeprecatedDefine(vpc.VPCConfig{
			Environment: c.Environment,
			Region:      c.Region,
			Cidr16:      c.Cidr16,
			Tags:        tags,
			Azs:         azs,
		})

		// In addition to the existing VPC, we create a new VPC for legacy workspaces migrating to the new VPC define.
		// This is only done for the all regions except for Canada since Canada is already using the new VPC define.
		// Once all workspaces are migrated to the new VPC define, we can remove old VPC define and point vpc flow logs & DNSResolverQueryLogs, etc. to the new VPC.
		if c.Region != infraconsts.SamsaraAWSCARegion {
			resources_new, err = vpc.Define(vpc.VPCConfig{
				Environment:                  environment_new,
				Region:                       c.Region,
				Cidr16:                       c.Cidr16,
				Tags:                         tags,
				Azs:                          azs,
				CreatePrivateSubnets:         true,
				VpcEndpointInterfaceServices: additionalVpcEndpointServices,
			})
			if err != nil {
				return nil, oops.Wrapf(err, "")
			}
		}

	} else {
		// Canada Central supports ca-central-1a, ca-central-1b & ca-central-1d (Note ca-central-1c is not supported).

		azs := infraconsts.GetDatabricksAvailabilityZones(c.Region)

		if len(azs) == 0 {
			return nil, oops.Errorf("%s", "No Databricks AZ found for region: "+c.Region)
		}

		az, err := dataplatformhelpers.GetSupportedAzs(azs)

		if err != nil {
			return nil, oops.Errorf("Error getting Az for region %s", c.Region)
		}

		resources, err = vpc.Define(vpc.VPCConfig{
			Environment:                  c.Environment,
			Region:                       c.Region,
			Cidr16:                       c.Cidr16,
			Tags:                         tags,
			Azs:                          az,
			CreatePrivateSubnets:         true,
			VpcEndpointInterfaceServices: additionalVpcEndpointServices,
		})
	}

	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	// Add DNSSEC validation configuration for the VPC
	dnssecConfig := &awsresource.Route53ResolverDnssecConfig{
		Name:  fmt.Sprintf("%s-%s-dnssec-resolver", c.Environment, c.Region),
		VpcID: resources.VPC.ResourceId().Reference(),
	}

	allResources := append(resources.Resources(), c.SecurityGroup(false), dnssecConfig)
	if resources_new != nil {
		allResources = append(allResources, resources_new.Resources()...)
		allResources = append(allResources, c.SecurityGroup(true))

		dnssecConfig_new := &awsresource.Route53ResolverDnssecConfig{
			Name:  fmt.Sprintf("%s-%s-dnssec-resolver", environment_new, c.Region),
			VpcID: resources_new.VPC.ResourceId().Reference(),
		}
		allResources = append(allResources, dnssecConfig_new)
	}
	return allResources, nil
}

// CMK = Customer Managed Key.
type CMK struct {
	Name        string
	Description string
	AccountId   string
	Policies    []policy.AWSPolicyStatement
}

func (c CMK) Key() *awsresource.KMSKey {
	return &awsresource.KMSKey{
		ResourceName:      c.Name,
		Description:       c.Description,
		EnableKeyRotation: true,
		KeyUsage:          awsresource.KMSKeyUsageTypeEncryptDecrypt,
		Tags: map[string]string{
			"samsara:service":       "databricks",
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
		Policy: &policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: append(c.Policies, policy.AWSPolicyStatement{
				// Allow the account that owns this key to manage this key.
				Effect:   "Allow",
				Resource: []string{"*"},
				Action: []string{
					"kms:*",
				},
				Principal: map[string]string{
					"AWS": fmt.Sprintf("arn:aws:iam::%s:root", c.AccountId),
				},
			}),
		},
	}
}

func (c CMK) KeyAlias() *awsresource.KMSAlias {
	return &awsresource.KMSAlias{
		Name:        fmt.Sprintf("alias/%s", c.Name),
		TargetKeyId: c.Key().ResourceId().ReferenceAttr("key_id"),
	}
}

func (c CMK) Resources() []tf.Resource {
	return []tf.Resource{
		c.Key(),
		c.KeyAlias(),
	}
}

type E2Workspace struct {
	Environment         string
	Region              string
	AWSAccountId        string
	DatabricksAccountId string

	LoggingBucket string

	// VPC.
	VPCCidr16                  int
	ProviderGroup              string
	EnableVpcFlowLogs          bool
	EnableDNSResolverQueryLogs bool
}

func (c E2Workspace) Name() string {
	return fmt.Sprintf("%s-%s", c.Environment, c.Region)
}

func (c E2Workspace) CrossAccountRole() Role {
	r := Role{
		Name:        fmt.Sprintf("databricks-cross-account-%s-role", c.Name()),
		Description: fmt.Sprintf("Cross account role for workspace %s in region %s", c.Environment, c.Region),
		AssumeRolePolicy: policy.AWSPolicyStatement{
			Principal: map[string]string{
				"AWS": fmt.Sprintf("arn:aws:iam::%s:root", dataplatformconfig.DatabricksAWSAccount),
			},
			Condition: &policy.AWSPolicyCondition{
				StringEquals: map[string]string{
					"sts:ExternalId": c.DatabricksAccountId,
				},
			},
			Effect: "Allow",
			Action: []string{"sts:AssumeRole"},
		},
		InlinePolicies: []policy.AWSPolicyStatement{
			{
				Effect: "Allow",
				Action: []string{
					"ec2:AssignPrivateIpAddresses",
					"ec2:AssociateIamInstanceProfile",
					"ec2:AttachVolume",
					"ec2:AuthorizeSecurityGroupEgress",
					"ec2:AuthorizeSecurityGroupIngress",
					"ec2:CancelSpotFleetRequests",
					"ec2:CancelSpotInstanceRequests",
					"ec2:CreateFleet",
					"ec2:CreateKeyPair",
					"ec2:CreateLaunchTemplate",
					"ec2:CreateLaunchTemplateVersion",
					"ec2:CreatePlacementGroup",
					"ec2:CreateTags",
					"ec2:CreateVolume",
					"ec2:DeleteFleets",
					"ec2:DeleteKeyPair",
					"ec2:DeleteLaunchTemplate",
					"ec2:DeleteLaunchTemplateVersions",
					"ec2:DeletePlacementGroup",
					"ec2:DeleteTags",
					"ec2:DeleteVolume",
					"ec2:DescribeAvailabilityZones",
					"ec2:DescribeFleetHistory",
					"ec2:DescribeFleetInstances",
					"ec2:DescribeFleets",
					"ec2:DescribeIamInstanceProfileAssociations",
					"ec2:DescribeInstances",
					"ec2:DescribeInstanceStatus",
					"ec2:DescribeInternetGateways",
					"ec2:DescribeLaunchTemplates",
					"ec2:DescribeLaunchTemplateVersions",
					"ec2:DescribeNatGateways",
					"ec2:DescribeNetworkAcls",
					"ec2:DescribePlacementGroups",
					"ec2:DescribePrefixLists",
					"ec2:DescribeReservedInstancesOfferings",
					"ec2:DescribeRouteTables",
					"ec2:DescribeSecurityGroups",
					"ec2:DescribeSpotFleetInstances",
					"ec2:DescribeSpotFleetRequestHistory",
					"ec2:DescribeSpotFleetRequests",
					"ec2:DescribeSpotInstanceRequests",
					"ec2:DescribeSpotPriceHistory",
					"ec2:DescribeSubnets",
					"ec2:DescribeVolumes",
					"ec2:DescribeVpcAttribute",
					"ec2:DescribeVpcs",
					"ec2:DetachVolume",
					"ec2:DisassociateIamInstanceProfile",
					"ec2:GetLaunchTemplateData",
					"ec2:ModifyFleet",
					"ec2:ModifyLaunchTemplate",
					"ec2:ModifySpotFleetRequest",
					"ec2:ReplaceIamInstanceProfileAssociation",
					"ec2:RequestSpotFleet",
					"ec2:RequestSpotInstances",
					"ec2:RevokeSecurityGroupEgress",
					"ec2:RevokeSecurityGroupIngress",
					"ec2:RunInstances",
					"ec2:TerminateInstances",
					"ec2:GetSpotPlacementScores",
					"iam:CreateServiceLinkedRole",
					"iam:DeleteServiceLinkedRole",
					"iam:GetServiceLinkedRoleDeletionStatus",
				},
				Resource: []string{"*"},
			},
			{
				Effect: "Allow",
				Action: []string{
					"iam:CreateServiceLinkedRole",
					"iam:PutRolePolicy",
				},
				Resource: []string{
					"arn:aws:iam::*:role/aws-service-role/spot.amazonaws.com/AWSServiceRoleForEC2Spot",
				},
				Condition: &policy.AWSPolicyCondition{
					StringLike: map[string][]string{
						"iam:AWSServiceName": {"spot.amazonaws.com"},
					},
				},
			},
			{
				Effect: "Allow",
				Action: []string{
					"kms:Encrypt",
					"kms:Decrypt",
				},
				Resource: []string{
					c.CMK().Key().ResourceId().ReferenceAttr("arn"),
				},
			},
			{
				Effect: "Allow",
				Action: []string{
					"iam:PassRole",
				},
				Resource: []string{
					fmt.Sprintf("arn:aws:iam::%s:role/ec2-instance/*", c.AWSAccountId),
				},
			},
			// https://docs.databricks.com/clusters/configure.html#enforce-mandatory-tags
			{
				Sid:    "MustTagWithSamsaraService",
				Effect: "Deny",
				Action: []string{
					"ec2:CreateTags",
					"ec2:RunInstances",
				},
				Resource: []string{"arn:aws:ec2:*:*:instance/*"},
				Condition: &policy.AWSPolicyCondition{
					StringNotLike: map[string]string{
						"aws:RequestTag/samsara:service": "databricks*",
					},
				},
			},
			{
				Sid:    "MustProvideCostAllocationTag",
				Effect: "Deny",
				Action: []string{
					"ec2:CreateTags",
					"ec2:RunInstances",
				},
				Resource: []string{"arn:aws:ec2:*:*:instance/*"},
				Condition: &policy.AWSPolicyCondition{
					StringNotLike: map[string]string{
						"aws:RequestTag/samsara:rnd-allocation": "?*",
					},
				},
			},
		},
	}
	return r
}

// RootBucket builds S3 root bucket for a workspace.
// https://docs.databricks.com/administration-guide/multiworkspace/aws-storage-mws.html
func (c E2Workspace) RootBucket() Bucket {
	return Bucket{
		Name:   fmt.Sprintf("databricks-%s-root", c.Name()),
		Region: c.Region,
		// Delete non-current versions ASAP so we don't incur extra costs from query results uploaded by Cloud Fetch.
		NonCurrentExpirationDaysOverride: 1,
		LoggingBucket:                    c.LoggingBucket,
		Metrics:                          true,
		RnDCostAllocation:                1,
	}
}

// https://docs.databricks.com/administration-guide/multiworkspace/aws-storage-mws.html#step-2-apply-bucket-policy-workspace-creation-only
func (c E2Workspace) RootBucketPolicy() *awsresource.S3BucketPolicy {
	bucketArn := c.RootBucket().Bucket().ResourceId().ReferenceAttr("arn")
	// Workspaces were created directly through API curl commands: https://paper.dropbox.com/doc/Databricks-E2-Workspace-Deployment-Commands-GR5BDxljwuwZdo8GD54Lr
	// Thus, ID and shard name are not available programatically. However we do not expect these values to change, so it is fine to hard code them
	var shardName string
	var workspaceId int
	switch c.Region {
	case infraconsts.SamsaraAWSEURegion:
		shardName = dataplatformconsts.DatabricksDevEUShardName
		workspaceId = dataplatformconsts.DatabricksDevEUWorkspaceId
	case infraconsts.SamsaraAWSCARegion:
		shardName = dataplatformconsts.DatabricksDevCAShardName
		workspaceId = dataplatformconsts.DatabricksDevCAWorkspaceId
	default:
		shardName = dataplatformconsts.DatabricksDevUSShardName
		workspaceId = dataplatformconsts.DatabricksDevUSWorkspaceId
	}
	// This policy restricts DBFS access: https://paper.dropbox.com/doc/No-more-DBFS-Du2afVhTkugsMTV9QMBNp
	noDBFSPolicy := &awsresource.S3BucketPolicy{
		Bucket: c.RootBucket().Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				policy.AWSPolicyStatement{
					Principal: map[string]string{
						"AWS": fmt.Sprintf("arn:aws:iam::%s:root", dataplatformconfig.DatabricksAWSAccount),
					},
					Effect: "Allow",
					Action: []string{
						"s3:GetObject",
						"s3:GetObjectVersion",
						"s3:ListBucket",
						"s3:GetBucketLocation",
					},
					Resource: []string{
						bucketArn,
						bucketArn + "/*",
					},
				},
				policy.AWSPolicyStatement{
					Principal: map[string]string{
						"AWS": fmt.Sprintf("arn:aws:iam::%s:root", dataplatformconfig.DatabricksAWSAccount),
					},
					Effect: "Allow",
					Action: []string{
						"s3:PutObject",
						"s3:DeleteObject",
					},
					Resource: []string{
						fmt.Sprintf("%s/%s/0_databricks_dev", bucketArn, shardName),
						fmt.Sprintf("%s/ephemeral/%s/%d/*", bucketArn, shardName, workspaceId),
						fmt.Sprintf("%s/%s/%d.*/*", bucketArn, shardName, workspaceId),
						fmt.Sprintf("%s/%s/%d/databricks/init/*/*.sh", bucketArn, shardName, workspaceId),
						fmt.Sprintf("%s/%s/%d/user/hive/warehouse/*.db/", bucketArn, shardName, workspaceId),
						fmt.Sprintf("%s/%s/%d/user/hive/warehouse/*__PLACEHOLDER__/", bucketArn, shardName, workspaceId),
						fmt.Sprintf("%s/%s/%d/user/hive/warehouse/*.db/*__PLACEHOLDER__/", bucketArn, shardName, workspaceId),
						fmt.Sprintf("%s/%s/%d/FileStore/plots/*", bucketArn, shardName, workspaceId),
						fmt.Sprintf("%s/%s/%d/FileStore/tables/*", bucketArn, shardName, workspaceId),
						fmt.Sprintf("%s/%s/%d/FileStore/jars/maven/*", bucketArn, shardName, workspaceId),
						fmt.Sprintf("%s/%s/%d/FileStore/import-stage/*", bucketArn, shardName, workspaceId),
						fmt.Sprintf("%s/%s/%d/local_disk0/tmp/*", bucketArn, shardName, workspaceId),
						fmt.Sprintf("%s/%s/%d/tmp/*", bucketArn, shardName, workspaceId),
						fmt.Sprintf("%s/%s/%d/databricks/mlflow/*", bucketArn, shardName, workspaceId),
						fmt.Sprintf("%s/%s/%d/databricks/mlflow-*/*", bucketArn, shardName, workspaceId),
						fmt.Sprintf("%s/%s/%d/mlflow-*/*", bucketArn, shardName, workspaceId),
						fmt.Sprintf("%s/%s/%d/pipelines/*", bucketArn, shardName, workspaceId),
						fmt.Sprintf("%s/%s/%d/home/*", bucketArn, shardName, workspaceId),
						fmt.Sprintf("%s/%s/%d/dagster_staging/*", bucketArn, shardName, workspaceId),
						fmt.Sprintf("%s/%s/%d/dagster_steps/*", bucketArn, shardName, workspaceId),
						// This seems like a location that needs to be written to by the UCX uploading script which uploads
						// some files to dbfs.
						fmt.Sprintf("%s/%s/%d/Users/*", bucketArn, shardName, workspaceId),
					},
				},
				policy.AWSPolicyStatement{
					Principal: map[string]string{
						"AWS": fmt.Sprintf("arn:aws:iam::%d:root", infraconsts.SamsaraAWSDatabricksAccountID),
					},
					Effect: "Allow",
					Action: []string{
						"s3:PutObject",
						"s3:DeleteObject",
						"s3:Get*",
					},
					Resource: []string{
						fmt.Sprintf("%s/%s/%d/dagster_staging/*", bucketArn, shardName, workspaceId),
						fmt.Sprintf("%s/%s/%d/dagster_steps/*", bucketArn, shardName, workspaceId),
					},
				},
				// Validated by DBX to create a new workspace
				policy.AWSPolicyStatement{
					Principal: map[string]string{
						"AWS": fmt.Sprintf("arn:aws:iam::%s:root", dataplatformconfig.DatabricksAWSAccount),
					},
					Effect: "Allow",
					Action: []string{
						"s3:GetObject",
						"s3:GetObjectVersion",
						"s3:PutObject",
						"s3:DeleteObject",
						"s3:ListBucket",
						"s3:GetBucketLocation",
					},
					Resource: []string{
						bucketArn,
						bucketArn + "/*",
					},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{
							"aws:PrincipalTag/DatabricksAccountId": dataplatformconfig.DatabricksAccountId,
						},
					},
				},
			},
		},
	}

	// Allow the test databricks account access for staging root bucket.
	if c.Environment == "dev-staging" {
		noDBFSPolicy.Policy.Statement = append(noDBFSPolicy.Policy.Statement, policy.AWSPolicyStatement{
			Principal: map[string]string{
				"AWS": fmt.Sprintf("arn:aws:iam::%s:root", dataplatformconfig.DatabricksAWSAccount),
			},
			Effect: "Allow",
			Action: []string{
				"s3:GetObject",
				"s3:GetObjectVersion",
				"s3:PutObject",
				"s3:DeleteObject",
				"s3:ListBucket",
				"s3:GetBucketLocation",
			},
			Resource: []string{
				bucketArn,
				bucketArn + "/*",
			},
			Condition: &policy.AWSPolicyCondition{
				StringEquals: map[string]string{
					"aws:PrincipalTag/DatabricksAccountId": dataplatformconfig.DatabricksTestAccountId,
				},
			},
		})
	}
	return noDBFSPolicy
}

// VPC builds customer-managed VPC.
// https://docs.databricks.com/administration-guide/cloud-configurations/aws/customer-managed-vpc.html
func (c E2Workspace) VPC() VPC {
	return VPC{
		Environment:   c.Environment,
		Region:        c.Region,
		Cidr16:        c.VPCCidr16,
		ProviderGroup: c.ProviderGroup,
	}
}

// CMK builds customer-managed key used to encrypte notebooks.
// https://docs.databricks.com/security/keys/customer-managed-keys-notebook-aws.html
func (c E2Workspace) CMK() CMK {
	return CMK{
		Name:        fmt.Sprintf("databricks-%s", c.Name()),
		Description: fmt.Sprintf("Key used to encrypt Databricks notebooks in workspace %s", c.Name()),
		AccountId:   c.AWSAccountId,
		Policies: []policy.AWSPolicyStatement{
			{
				Sid:    "Allow Databricks to use KMS key for Notebooks",
				Effect: "Allow",
				Principal: map[string]string{
					"AWS": fmt.Sprintf("arn:aws:iam::%s:root", dataplatformconfig.DatabricksAWSAccount),
				},
				Action: []string{
					"kms:Encrypt",
					"kms:Decrypt",
				},
				Resource: []string{"*"},
			},
		},
	}
}

// Audit log role creates a role for Databricks to assume to place audit logs in our S3 bucket
// https://docs.databricks.com/administration-guide/account-settings/audit-logs.html
func (c E2Workspace) AuditLogRole() Role {
	regionPrefix := awsregionconsts.RegionPrefix[c.Region]
	// Note: This works for now, as we only have one workspace per region
	// If we add more workspaces, need to changes this to generate multiple prefixes per region
	s3Prefix := "dev-" + c.Region
	r := Role{
		Name:        fmt.Sprintf("databricks-audit-log-%s-role", c.Name()),
		Description: fmt.Sprintf("Databricks audit log role for region %s", c.Region),
		AssumeRolePolicy: policy.AWSPolicyStatement{
			Principal: map[string]string{
				"AWS": "arn:aws:iam::414351767826:role/SaasUsageDeliveryRole-prod-IAMRole-3PLHICCRR1TK",
			},
			Condition: &policy.AWSPolicyCondition{
				StringEquals: map[string]string{
					"sts:ExternalId": c.DatabricksAccountId,
				},
			},
			Effect: "Allow",
			Action: []string{"sts:AssumeRole"},
		},
		InlinePolicies: []policy.AWSPolicyStatement{
			{
				Effect: "Allow",
				Action: []string{
					"s3:GetBucketLocation",
				},
				Resource: []string{fmt.Sprintf("arn:aws:s3:::%sdatabricks-audit-log", regionPrefix)},
			},
			{
				Effect: "Allow",
				Action: []string{
					"s3:PutObject",
					"s3:GetObject",
					"s3:DeleteObject",
					"s3:PutObjectAcl",
					"s3:AbortMultipartUpload",
				},
				Resource: []string{
					fmt.Sprintf("arn:aws:s3:::%sdatabricks-audit-log/%s/", regionPrefix, s3Prefix),
					fmt.Sprintf("arn:aws:s3:::%sdatabricks-audit-log/%s/*", regionPrefix, s3Prefix),
				},
			},
			{
				Effect: "Allow",
				Action: []string{
					"s3:ListBucket",
					"s3:ListMultipartUploadParts",
					"s3:ListBucketMultipartUploads",
				},
				Resource: []string{fmt.Sprintf("arn:aws:s3:::%sdatabricks-audit-log", regionPrefix)},
				Condition: &policy.AWSPolicyCondition{
					ForAllValuesStringLike: map[string][]string{
						"s3:prefix": {
							s3Prefix,
							fmt.Sprintf("%s/*", s3Prefix),
						},
					},
				},
			},
		},
	}
	return r
}

func (c E2Workspace) Resources() (map[string][]tf.Resource, error) {
	vpcResources, err := c.VPC().Resources()
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	resources := map[string][]tf.Resource{
		"cross_account_role": c.CrossAccountRole().Resources(),
		"root_bucket":        append(c.RootBucket().Resources(), c.RootBucketPolicy()),
		"cmk":                c.CMK().Resources(),
		"vpc":                vpcResources,
		"audit_log_role":     c.AuditLogRole().Resources(),
	}

	if c.EnableVpcFlowLogs {
		vpcFlow := c.vpcFlowLogsResource()
		if len(vpcFlow) > 0 {
			resources["vpc_flow_logs"] = vpcFlow
		}
	}

	if c.EnableDNSResolverQueryLogs {
		// We have migrated to the new VPC v2 define for EU & US.
		// Canada & dev-staging were always using the new VPC define and hence v2 was not needed.
		environment := c.Environment
		if c.Region != infraconsts.SamsaraAWSCARegion && c.Environment != "dev-staging" {
			environment = environment + "-v2"
		}
		dnsResolverResources := DNSResolverResources(c.Region, c.Environment, awsresource.VPCResourceId(fmt.Sprintf("%s-%s-vpc", environment, c.Region)).ReferenceAttr("id"))
		resources["dns_resolver"] = dnsResolverResources
	}

	return resources, nil
}

func IsE2ProviderGroup(providerGroup string) bool {
	// Temporary helper function for identifying if a provider group is E2 or not
	return providerGroup == "databricks-dev-us" || providerGroup == "databricks-dev-eu" || providerGroup == "databricks-dev-ca"
}
