package datahubprojects

import (
	"fmt"
	"regexp"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource/rds"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/pointer"
)

const (
	defaultParameterGroup = "default.aurora-mysql8.0"
	engine                = "aurora-mysql"
	engineVersion         = "8.0.mysql_aurora.3.08.2"
	instanceClass         = infraconsts.R6G_2XLarge
)

func parseSubnetAvailabilityZone(subnetId string, region string) (string, error) {
	regexp := regexp.MustCompile(fmt.Sprintf("%s[abcd]", region))
	matches := regexp.FindStringSubmatch(subnetId)
	if len(matches) == 0 {
		return "", oops.Errorf("%s", fmt.Sprintf("cannot parse availability zone from subnet id %s", subnetId))
	}
	return matches[0], nil
}

func extractSubnetAvailabilityZones(subnetIds []string, region string) ([]string, error) {
	azs := []string{}
	for _, subnetId := range subnetIds {
		az, err := parseSubnetAvailabilityZone(subnetId, region)
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
		azs = append(azs, az)
	}
	return azs, nil
}

func rdsResources(config dataplatformconfig.DatabricksConfig, v vpcIds) (map[string][]tf.Resource, error) {
	var resources []tf.Resource

	accountId := awsregionconsts.RegionDatabricksAccountID[config.Region]
	clusterIdentifier := fmt.Sprintf("%s-prod-mysql", DatahubResourceBaseName)
	dbSubnetGroup := fmt.Sprintf("%s-%s-dbsubnet", DatahubResourceBaseName, config.Region)
	availabilityZones, err := extractSubnetAvailabilityZones(v.privateSubnetIds, config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to create rds resource")
	}

	cluster := rds.Cluster{
		ResourceName:      clusterIdentifier,
		ClusterIdentifier: clusterIdentifier,
		Engine:            pointer.StringPtr(engine),
		EngineVersion:     pointer.StringPtr(engineVersion),
		VpcSecurityGroupIds: []string{
			"${aws_security_group.datahub-vpn-ingress.id}",  // (tf managed in load balancer)
			eksClusterSecurityGroupId,                       // (created by EKS; not tf managed)
			"${aws_security_group.datahub-vpc-internal.id}", // (tf managed in vpc)
		},
		AvailabilityZones:           availabilityZones,
		DbSubnetGroupName:           dbSubnetGroup,
		DbClusterParameterGroupName: defaultParameterGroup,
		StorageEncrypted:            true,
		KmsKeyId:                    fmt.Sprintf("arn:aws:kms:%s:%d:key/be4e13d0-fff1-46a3-b63c-3e9e0f5e40d8", config.Region, accountId), // default aws kms key id
		EnabledCloudwatchLogsExports: []string{
			"audit",
		},
		SkipFinalSnapshot:     pointer.BoolPtr(true),
		DeleteProtection:      true,
		MasterUsername:        "datahub",
		MasterPassword:        "initial-hQLfVwkKv0eJZJWGOuGomVPQVHqAS7Z",
		DatabaseName:          "datahub",
		BackupRetentionPeriod: "35",
		CopyTagsToSnapshot:    true,
		Tags:                  getDatahubTags("rds-cluster"),
	}
	cluster.MetaParameters.Lifecycle.PreventDestroy = true
	cluster.MetaParameters.Lifecycle.IgnoreChanges = []string{}
	// Ignore password updates because we change this later.
	cluster.MetaParameters.Lifecycle.IgnoreChanges = append(cluster.MetaParameters.Lifecycle.IgnoreChanges, "master_password")

	writerInstance := rds.ClusterInstance{
		ResourceName:            fmt.Sprintf("%s-instance-1", clusterIdentifier),
		Identifier:              fmt.Sprintf("%s-instance-1", clusterIdentifier),
		ClusterIdentifier:       clusterIdentifier,
		InstanceClass:           string(instanceClass),
		DbSubnetGroupName:       dbSubnetGroup,
		DbParameterGroupName:    defaultParameterGroup,
		PromotionTier:           "1",
		MonitoringRoleArn:       fmt.Sprintf("arn:aws:iam::%d:role/rds-monitoring-role", accountId),
		MonitoringInterval:      pointer.IntPtr(30),
		AutoMinorVersionUpgrade: pointer.BoolPtr(true),
		Tags:                    getDatahubTags("rds-cluster-instance"),
		Engine:                  pointer.StringPtr(engine),
		EngineVersion:           pointer.StringPtr(engineVersion),
	}
	resources = append(resources, &writerInstance, &cluster)

	return map[string][]tf.Resource{
		"rds": resources,
	}, nil
}
