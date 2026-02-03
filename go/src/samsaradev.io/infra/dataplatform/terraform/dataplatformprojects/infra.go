package dataplatformprojects

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/app/generate_terraform/awsdynamodbresource"
	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/emitters/codeartifact"
	"samsaradev.io/infra/app/generate_terraform/emitters/ssooverrides"
	"samsaradev.io/infra/app/generate_terraform/genericresource"
	s3emitters "samsaradev.io/infra/app/generate_terraform/s3"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/datapipelines/graphs"
	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes"
	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes/outputtypes"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/dataplatform/unitycatalog/databaseregistries"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/pointer"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/reports/sparkreportregistry"
	"samsaradev.io/service/components"
	"samsaradev.io/team"
)

type dataModelDatabaseListType int

const (
	DevelopmentOnly = iota
	ProductionOnly
	BothProductionAndDevelopment
	GAOnly
)

// Theses are resources required to make terraform work
// It has terraform state bucket resources, terraform
// state table in DynamoDB and buildkite permissions.
// These are prerequisite for terraform to work.
func TerraformBootstrapSetupResources(config dataplatformconfig.DatabricksConfig) (map[string][]tf.Resource, error) {

	tfStateBucket := dataplatformresource.Bucket{
		Name:              "databricks-terraform-state",
		Region:            config.Region,
		RnDCostAllocation: 1,
		LoggingBucket:     fmt.Sprintf("%sdatabricks-s3-logging", awsregionconsts.RegionPrefix[config.Region]),
	}
	tfStateLockTable := &awsdynamodbresource.DynamoDBTable{
		BaseResource: tf.BaseResource{
			MetaParameters: tf.MetaParameters{
				Lifecycle: tf.Lifecycle{
					PreventDestroy: true,
				},
			},
		},
		ResourceName: "terraform_state_lock_dynamo",
		Name:         infraconsts.TFStateLockTable,
		BillingMode:  awsdynamodbresource.PayPerRequest,
		HashKey:      "LockID",
		TTL: awsdynamodbresource.DynamoDBTableTTL{
			AttributeName: "ExpiresAt",
			Enabled:       true,
		},
		Attributes: []awsdynamodbresource.DynamoDBTableAttribute{
			{
				KeyName: "LockID",
				Type:    awsdynamodbresource.String,
			},
		},
		Tags: map[string]string{
			"Name":                   infraconsts.TFStateLockTable,
			"samsara:service":        infraconsts.TFStateLockTable,
			"samsara:team":           strings.ToLower(team.DataPlatform.Name()),
			"samsara:product-group":  strings.ToLower(team.TeamProductGroup[team.DataPlatform.Name()]),
			"samsara:rnd-allocation": "1",
		},
	}

	resources := map[string][]tf.Resource{
		"tfstate_storage": append(
			tfStateBucket.Resources(),
			tfStateLockTable,
		),
	}

	// Add buildkite resources.
	for resourceKey, value := range BuildkiteResources() {
		resources[resourceKey] = value
	}

	return resources, nil
}

func CoreInfraProject(providerGroup string) (*project.Project, error) {
	p := &project.Project{
		RootTeam:        team.DataPlatform.Name(),
		Provider:        providerGroup,
		Class:           "infrastructure",
		ResourceGroups:  make(map[string][]tf.Resource),
		GenerateOutputs: true,
	}

	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	terraformBootstrapSetup, err := TerraformBootstrapSetupResources(config)

	if err != nil {
		return nil, oops.Wrapf(err, "terraform setup resources.")
	}

	jobBuckets, err := JobBuckets(config)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	ucBuckets, err := UnityCatalogMetastoreBuckets(config)
	if err != nil {
		return nil, oops.Wrapf(err, "failed creating metastore buckets")
	}

	coreProjects := []map[string][]tf.Resource{ // lint: +sorted
		AccountLocals(config),
		Cloudtrail(config),
		CloudwatchEventRules(),
		Databases(config),
		DatabricksResources(config),
		Datadog(config),
		DataScience(config),
		DeployedArtifacts(config),
		ECR(config),
		InitScripts(config),
		jobBuckets,
		Playground(config),
		SES(config),
		SparkLibraries(config),
		StaticArtifacts(config),
		terraformBootstrapSetup,
		ToolshedRole(config),
		ucBuckets,
		WarehouseBucket(config),
	}

	deployedArtifactObjects, err := DeployedArtifactObjects(config)
	if err != nil {
		return nil, oops.Wrapf(err, "Error creating DeployedArtifactObjects")
	}
	coreProjects = append(coreProjects, deployedArtifactObjects)

	storageResources, err := StorageResources(config)
	if err != nil {
		return nil, oops.Wrapf(err, "Error creating StorageResources")
	}
	coreProjects = append(coreProjects, storageResources)

	glueResources, err := Glue(config)
	if err != nil {
		return nil, oops.Wrapf(err, "Error creating Glue resources")
	}
	coreProjects = append(coreProjects, glueResources)

	mlFeaturePlatformResources, err := MlFeaturePlatform(config)
	if err != nil {
		return nil, oops.Wrapf(err, "Error creating Ml resources")
	}
	coreProjects = append(coreProjects, mlFeaturePlatformResources)

	mlCodeArtifactReadResources, err := MLCodeArtifactReadPolicy()
	if err != nil {
		return nil, oops.Wrapf(err, "Error creating MLCodeArtifactReadPolicy resources")
	}
	coreProjects = append(coreProjects, mlCodeArtifactReadResources)

	dataModelPermissionsResources := DataModelPermissonsResources(config)
	coreProjects = append(coreProjects, dataModelPermissionsResources)

	metricsRepoPermissionsResources := MetricsRepoPermissonsResources(config)
	coreProjects = append(coreProjects, metricsRepoPermissionsResources)

	ec2ClusterResources, err := Ec2Clusters(config)
	if err != nil {
		return nil, oops.Wrapf(err, "Error creating Ec2Clusters resources")
	}
	coreProjects = append(coreProjects, ec2ClusterResources)

	if providerGroup == "databricks-us" {
		dataPipelinesReplicatedGlueResources, err := DataPipelinesReplicateGlueResources(config)
		if err != nil {
			return nil, oops.Wrapf(err, "Error created resources for data pipeline replicated glue resources")
		}

		p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups,
			VPC(config),
			DataPipelinesReplicatedBuckets(config),
			dataPipelinesReplicatedGlueResources,
			InstanceProfileForDatabricksSupportCluster(),
		)

	} else {
		p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups,
			DataPipelinesReplicationIAMRoles(config),
		)
	}

	p.ResourceGroups = project.MergeResourceGroups(append(coreProjects, p.ResourceGroups)...)
	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups,
		map[string][]tf.Resource{
			"aws_provider":        resource.ProjectAWSProvider(p),
			"databricks_provider": dataplatformresource.DatabricksOauthProvider(config.Hostname),
			"tf_backend":          resource.ProjectTerraformBackend(p),
		})

	return p, nil
}

func MixpanelConnectorProject(providerGroup string) (*project.Project, error) {
	p := &project.Project{
		Name:            "mixpanel",
		RootTeam:        team.DataEngineering.Name(),
		Provider:        providerGroup,
		Class:           "infrastructure",
		ResourceGroups:  make(map[string][]tf.Resource),
		GenerateOutputs: true,
	}

	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups,
		MixpanelExportResources(config),
	)

	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups,
		map[string][]tf.Resource{
			"aws_provider": resource.ProjectAWSProvider(p),
			"tf_backend":   resource.ProjectTerraformBackend(p),
		})

	return p, nil
}

func AccountLocals(c dataplatformconfig.DatabricksConfig) map[string][]tf.Resource {
	dbHostname := &genericresource.StringLocal{
		Name:  "databricks_hostname",
		Value: fmt.Sprintf(`"%s"`, c.Hostname),
	}
	dbAwsAccount := &genericresource.StringLocal{
		Name:  "databricks_aws_account",
		Value: fmt.Sprintf(`"%s"`, dataplatformconfig.DatabricksAWSAccount),
	}
	dbExternalId := &genericresource.StringLocal{
		Name:  "databricks_external_id",
		Value: fmt.Sprintf(`"%s"`, c.ExternalId),
	}
	return map[string][]tf.Resource{
		"databricks": []tf.Resource{
			dbHostname,
			dbAwsAccount,
			dbExternalId,
		},
	}
}

func getDataModelDatabaseList(config dataplatformconfig.DatabricksConfig, databaseListType dataModelDatabaseListType) []databaseregistries.SamsaraDB {
	developmentNonGA := databaseregistries.GetDatabasesByGroup(config, databaseregistries.CustomBucketDataModelDevelopmentNonGADatabaseGroup, databaseregistries.LegacyOnlyDatabases)
	developmentGA := databaseregistries.GetDatabasesByGroup(config, databaseregistries.CustomBucketDataModelDevelopmentGADatabaseGroup, databaseregistries.LegacyOnlyDatabases)
	prodNonGA := databaseregistries.GetDatabasesByGroup(config, databaseregistries.CustomBucketDataModelProductionNonGADatabaseGroup, databaseregistries.LegacyOnlyDatabases)
	prodGA := databaseregistries.GetDatabasesByGroup(config, databaseregistries.CustomBucketDataModelProductionGADatabaseGroup, databaseregistries.LegacyOnlyDatabases)

	dataModelDatabaseList := append(developmentNonGA, developmentGA...)
	if databaseListType == ProductionOnly {
		dataModelDatabaseList = append(prodNonGA, prodGA...)
	} else if databaseListType == BothProductionAndDevelopment {
		dataModelDatabaseList = append(dataModelDatabaseList, prodNonGA...)
		dataModelDatabaseList = append(dataModelDatabaseList, prodGA...)
	} else if databaseListType == GAOnly {
		dataModelDatabaseList = append(developmentGA, prodGA...)
	}

	return dataModelDatabaseList
}

func getDataModelDatabaseNames(config dataplatformconfig.DatabricksConfig, databaseListType dataModelDatabaseListType) []string {
	dataModelDatabaseList := getDataModelDatabaseList(config, databaseListType)

	var dataModelDatabaseNames []string
	for _, samsaraDb := range dataModelDatabaseList {
		dataModelDatabaseNames = append(dataModelDatabaseNames, samsaraDb.Name)
	}
	return dataModelDatabaseNames
}

func getDataModelBuckets(config dataplatformconfig.DatabricksConfig, databaseListType dataModelDatabaseListType) []string {
	dataModelDatabaseList := getDataModelDatabaseList(config, databaseListType)

	var bucketNames []string
	bucketNamesMap := make(map[string]struct{})
	for _, samsaraDb := range dataModelDatabaseList {
		if _, ok := bucketNamesMap[samsaraDb.GetGlueBucket()]; !ok {
			bucketNamesMap[samsaraDb.GetGlueBucket()] = struct{}{}
			bucketNames = append(bucketNames, samsaraDb.GetGlueBucket())
		}
	}
	return bucketNames
}

func makeDatabaseGlueArns(c dataplatformconfig.DatabricksConfig, databaseNames []string) []string {

	var databaseArns []string
	for _, database := range databaseNames {
		databaseArns = append(databaseArns,
			fmt.Sprintf("arn:aws:glue:%s:%d:database/%s", c.Region, c.DatabricksAWSAccountId, database),
			fmt.Sprintf("arn:aws:glue:%s:%d:table/%s/*", c.Region, c.DatabricksAWSAccountId, database),
		)
	}
	return databaseArns

}

func makeDatabaseBucketArns(c dataplatformconfig.DatabricksConfig, bucketNames []string) []string {
	prefix := awsregionconsts.RegionPrefix[c.Region]

	var bucketArns []string
	for _, bucket := range bucketNames {
		bucketArns = append(bucketArns,
			fmt.Sprintf("arn:aws:s3:::%s", prefix+bucket),
			fmt.Sprintf("arn:aws:s3:::%s/*", prefix+bucket),
		)
	}
	return bucketArns
}

func DataModelPermissonsResources(c dataplatformconfig.DatabricksConfig) map[string][]tf.Resource {

	glueWritePolicyActions := []string{
		"glue:CreateTable",
		"glue:UpdateTable",
		"glue:DeleteTable",
		"glue:CreatePartition",
		"glue:DeletePartition",
		"glue:UpdatePartition",
		"glue:BatchCreatePartition",
		"glue:BatchDeletePartition",
		"glue:BatchGetPartition",
	}

	datamodelWritePolicy_DEVELOPMENT := &awsresource.IAMPolicy{
		ResourceName: "datamodel_dev_write",
		Name:         "datamodel-dev-write",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				policy.AWSPolicyStatement{
					Effect: "Allow",
					Action: []string{
						"s3:*",
					},
					Resource: makeDatabaseBucketArns(c, getDataModelBuckets(c, DevelopmentOnly)),
				},
				policy.AWSPolicyStatement{
					Effect:   "Allow",
					Action:   glueWritePolicyActions,
					Resource: makeDatabaseGlueArns(c, getDataModelDatabaseNames(c, DevelopmentOnly)),
				},
			},
		},
	}

	datamodelWritePolicy_PRODUCTION := &awsresource.IAMPolicy{
		ResourceName: "datamodel_prod_write",
		Name:         "datamodel-prod-write",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				policy.AWSPolicyStatement{
					Effect:   "Allow",
					Action:   []string{"s3:*"},
					Resource: makeDatabaseBucketArns(c, getDataModelBuckets(c, ProductionOnly)),
				},
				policy.AWSPolicyStatement{
					Effect:   "Allow",
					Action:   glueWritePolicyActions,
					Resource: makeDatabaseGlueArns(c, getDataModelDatabaseNames(c, ProductionOnly)),
				},
			},
		},
	}

	datamodelReadPolicy_DEVELOPMENT := &awsresource.IAMPolicy{
		ResourceName: "datamodel_dev_read",
		Name:         "datamodel-dev-read",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				policy.AWSPolicyStatement{
					Effect:   "Allow",
					Action:   []string{"s3:List*", "s3:Get*"},
					Resource: makeDatabaseBucketArns(c, getDataModelBuckets(c, DevelopmentOnly)),
				},
				policy.AWSPolicyStatement{
					Effect: "Allow",
					Action: []string{
						"glue:Get*",
					},
					Resource: makeDatabaseGlueArns(c, getDataModelDatabaseNames(c, DevelopmentOnly)),
				},
			},
		},
	}

	datamodelReadPolicy_PRODUCTION := &awsresource.IAMPolicy{
		ResourceName: "datamodel_prod_read",
		Name:         "datamodel-prod-read",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				policy.AWSPolicyStatement{
					Effect:   "Allow",
					Action:   []string{"s3:List*", "s3:Get*"},
					Resource: makeDatabaseBucketArns(c, getDataModelBuckets(c, ProductionOnly)),
				},
				policy.AWSPolicyStatement{
					Effect: "Allow",
					Action: []string{
						"glue:Get*",
					},
					Resource: makeDatabaseGlueArns(c, getDataModelDatabaseNames(c, ProductionOnly)),
				},
			},
		},
	}

	datamodelReadPolicy_GA := &awsresource.IAMPolicy{
		ResourceName: "datamodel_ga_read",
		Name:         "datamodel-ga-read",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				policy.AWSPolicyStatement{
					Effect:   "Allow",
					Action:   []string{"s3:List*", "s3:Get*"},
					Resource: makeDatabaseBucketArns(c, getDataModelBuckets(c, GAOnly)),
				},
				policy.AWSPolicyStatement{
					Effect: "Allow",
					Action: []string{
						"glue:Get*",
					},
					Resource: makeDatabaseGlueArns(c, getDataModelDatabaseNames(c, GAOnly)),
				},
			},
		},
	}

	return map[string][]tf.Resource{
		"datamodel_iam_policy": []tf.Resource{
			datamodelReadPolicy_DEVELOPMENT,
			datamodelWritePolicy_DEVELOPMENT,
			datamodelReadPolicy_PRODUCTION,
			datamodelWritePolicy_PRODUCTION,
			datamodelReadPolicy_GA,
		},
	}
}

func MetricsRepoPermissonsResources(c dataplatformconfig.DatabricksConfig) map[string][]tf.Resource {

	glueWritePolicyActions := []string{
		"glue:CreateTable",
		"glue:UpdateTable",
		"glue:DeleteTable",
		"glue:CreatePartition",
		"glue:DeletePartition",
		"glue:UpdatePartition",
		"glue:BatchCreatePartition",
		"glue:BatchDeletePartition",
		"glue:BatchGetPartition",
		"glue:CreateUserDefinedFunction",
		"glue:UpdateUserDefinedFunction",
		"glue:DeleteUserDefinedFunction",
	}

	glueResourcesProd := append(
		makeDatabaseGlueArns(c, []string{"metrics_repo", "metrics_api"}),
		fmt.Sprintf("arn:aws:glue:%s:%d:catalog", c.Region, c.DatabricksAWSAccountId),
		fmt.Sprintf("arn:aws:glue:%s:%d:userDefinedFunction/metrics_api/*", c.Region, c.DatabricksAWSAccountId),
	)

	metricRepoWritePolicy_PRODUCTION := &awsresource.IAMPolicy{
		ResourceName: "metrics_repo_prod_write",
		Name:         "metrics-repo-prod-write",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				policy.AWSPolicyStatement{
					Effect:   "Allow",
					Action:   glueWritePolicyActions,
					Resource: glueResourcesProd,
				},
			},
		},
	}

	glueResourcesDev := append(
		makeDatabaseGlueArns(c, []string{"datamodel_dev"}),
		fmt.Sprintf("arn:aws:glue:%s:%d:catalog", c.Region, c.DatabricksAWSAccountId),
		fmt.Sprintf("arn:aws:glue:%s:%d:userDefinedFunction/datamodel_dev/*", c.Region, c.DatabricksAWSAccountId),
	)
	metricRepoWritePolicy_DEVELOPMENT := &awsresource.IAMPolicy{
		ResourceName: "metrics_repo_dev_write",
		Name:         "metrics-repo-dev-write",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				policy.AWSPolicyStatement{
					Effect:   "Allow",
					Action:   glueWritePolicyActions,
					Resource: glueResourcesDev,
				},
				policy.AWSPolicyStatement{
					Effect:   "Allow",
					Action:   []string{"s3:List*", "s3:Get*"},
					Resource: makeDatabaseBucketArns(c, []string{"datamodel-warehouse-dev"}),
				},
			},
		},
	}

	return map[string][]tf.Resource{
		"metrics_repo_iam": []tf.Resource{
			metricRepoWritePolicy_PRODUCTION,
			metricRepoWritePolicy_DEVELOPMENT,
		},
	}
}

func StorageResources(c dataplatformconfig.DatabricksConfig) (map[string][]tf.Resource, error) {

	publicAccessBlock := &awsresource.S3AccountPublicAccessBlock{
		BlockPublicACLs:       true,
		BlockPublicPolicy:     true,
		IgnorePublicACLs:      true,
		RestrictPublicBuckets: true,
	}

	loggingBucket := dataplatformresource.Bucket{
		Name:                             "databricks-s3-logging",
		Region:                           c.Region,
		CurrentExpirationDays:            60,
		NonCurrentExpirationDaysOverride: 1,
		RnDCostAllocation:                1,
	}

	loggingBucketLocal := &genericresource.StringLocal{
		Name:  "logging_bucket",
		Value: fmt.Sprintf(`"%s"`, loggingBucket.Bucket().ResourceId().ReferenceAttr("bucket")),
	}

	// Create a bucket policy to allow server access logging
	// https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerLogs.html#how-logs-delivered
	bucketLoggingPolicy := &awsresource.S3BucketPolicy{
		Bucket: loggingBucket.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Sid:    "S3ServerAccessLogsPolicy",
					Effect: "Allow",
					Principal: map[string]string{
						"Service": "logging.s3.amazonaws.com",
					},
					Action: []string{"s3:PutObject"},
					Resource: []string{
						loggingBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", loggingBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
			},
		},
	}

	// cluster logs
	clusterLogBucket := dataplatformresource.Bucket{
		Name:                             "databricks-cluster-logs",
		Region:                           c.Region,
		NonCurrentExpirationDaysOverride: 1,
		CurrentExpirationDays:            60,
		RnDCostAllocation:                1,
		LoggingBucket:                    fmt.Sprintf("%sdatabricks-s3-logging", awsregionconsts.RegionPrefix[c.Region]),
	}
	// Policy for attachment as per: https://docs.databricks.com/administration-guide/cloud-configurations/aws/instance-profiles.html#step-1-create-an-instance-profile-to-access-an-s3-bucket
	clusterLoggingPolicy := &awsresource.IAMPolicy{
		ResourceName: "write_cluster_logs",
		Name:         "write-cluster-logs",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				policy.AWSPolicyStatement{
					Effect:   "Allow",
					Action:   []string{"s3:ListBucket"},
					Resource: []string{clusterLogBucket.Bucket().ResourceId().ReferenceAttr("arn")},
				},
				policy.AWSPolicyStatement{
					Effect: "Allow",
					Action: []string{
						"s3:PutObject",
						"s3:GetObject",
						"s3:DeleteObject",
						"s3:PutObjectAcl",
					},
					Resource: []string{
						clusterLogBucket.Bucket().ResourceId().ReferenceAttr("arn") + "/*",
					},
				},
			},
		},
	}

	// Managed Policy attachment for StandardDevBuckets as per: https://docs.databricks.com/administration-guide/cloud-configurations/aws/instance-profiles.html#step-1-create-an-instance-profile-to-access-an-s3-bucket
	prefix := awsregionconsts.RegionPrefix[c.Region]
	var readBucketArns []string
	for _, bucket := range standardDevReadBuckets {
		readBucketArns = append(readBucketArns,
			fmt.Sprintf("arn:aws:s3:::%s", prefix+bucket),
			fmt.Sprintf("arn:aws:s3:::%s/*", prefix+bucket),
		)
	}
	standardDevReadBucketsPolicy := &awsresource.IAMPolicy{
		ResourceName: "s3_standard_dev_buckets_read",
		Name:         "s3-standard-dev-buckets-read",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				policy.AWSPolicyStatement{
					Effect:   "Allow",
					Action:   []string{"s3:List*", "s3:Get*"},
					Resource: readBucketArns,
				},
			},
		},
	}

	var writeBucketArns []string
	for _, bucket := range standardDevReadWriteBuckets {
		writeBucketArns = append(writeBucketArns,
			fmt.Sprintf("arn:aws:s3:::%s", prefix+bucket),
			fmt.Sprintf("arn:aws:s3:::%s/*", prefix+bucket),
		)
	}

	standardDevWriteBucketsPolicy := &awsresource.IAMPolicy{
		ResourceName: "s3_standard_dev_buckets_write",
		Name:         "s3-standard-dev-buckets-write",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				policy.AWSPolicyStatement{
					Effect: "Allow",
					Action: []string{
						"s3:*",
					},
					Resource: writeBucketArns,
				},
			},
		},
	}

	sqlQueryHistoryBucket := dataplatformresource.Bucket{
		Name:                             "databricks-sql-query-history",
		Region:                           c.Region,
		RnDCostAllocation:                1,
		CurrentExpirationDays:            365,
		NonCurrentExpirationDaysOverride: 1,
		LoggingBucket:                    fmt.Sprintf("%sdatabricks-s3-logging", awsregionconsts.RegionPrefix[c.Region]),
	}
	sqlQueryHistoryBucketPolicy := &awsresource.S3BucketPolicy{
		Bucket: sqlQueryHistoryBucket.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				policy.AWSPolicyStatement{
					Principal: map[string]string{
						"AWS": fmt.Sprintf("arn:aws:iam::%d:root", c.AWSAccountId),
					},
					Effect: "Allow",
					Action: []string{
						"s3:PutObject",
					},
					Resource: []string{
						fmt.Sprintf("%s/*", sqlQueryHistoryBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{"s3:x-amz-acl": "bucket-owner-full-control"},
					},
				},
				{
					Sid:       "RequireTLS",
					Effect:    "Deny",
					Principal: map[string]string{"AWS": "*"},
					Action:    []string{"s3:*"},
					Resource: []string{
						sqlQueryHistoryBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", sqlQueryHistoryBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
					Condition: &policy.AWSPolicyCondition{
						Bool: &policy.AWSPolicyConditionBool{SecureTransportEnabled: pointer.BoolPtr(false)},
					},
				},
			},
		},
	}

	// https://docs.databricks.com/administration-guide/account-settings/audit-logs.html
	auditLogBucket := dataplatformresource.Bucket{
		Name:              "databricks-audit-log",
		Region:            c.Region,
		RnDCostAllocation: 1,
		// Databricks audit log tracks employee actions. Security says we should keep 1 year history.
		// https://samsara-net.slack.com/archives/C9X3A9RCM/p1622836675010000
		CurrentExpirationDays:            365,
		NonCurrentExpirationDaysOverride: 1,
		LoggingBucket:                    fmt.Sprintf("%sdatabricks-s3-logging", awsregionconsts.RegionPrefix[c.Region]),
	}

	// From https://accounts.cloud.databricks.com/#auditLogs.
	// It's the same in both regions.
	auditLogWriterRole := "arn:aws:iam::090101015318:role/DatabricksAuditLogs-WriterRole-VV4KJWX4FRIK"
	auditLogBucketPolicy := &awsresource.S3BucketPolicy{
		Bucket: auditLogBucket.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				policy.AWSPolicyStatement{
					Principal: map[string]string{
						"AWS": auditLogWriterRole,
					},
					Effect: "Allow",
					Action: []string{
						"s3:PutObject",
					},
					Resource: []string{
						fmt.Sprintf("%s/*", auditLogBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{"s3:x-amz-acl": "bucket-owner-full-control"},
					},
				},
				{
					Sid:       "RequireTLS",
					Effect:    "Deny",
					Principal: map[string]string{"AWS": "*"},
					Action:    []string{"s3:*"},
					Resource: []string{
						auditLogBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", auditLogBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
					Condition: &policy.AWSPolicyCondition{
						Bool: &policy.AWSPolicyConditionBool{SecureTransportEnabled: pointer.BoolPtr(false)},
					},
				},
			},
		},
	}

	var auditLogResources []tf.Resource

	// Replicate the databricks audit log data to an IT owned bucket that Splunk will read from.
	if c.DatabricksProviderGroup == "databricks-us" { // only US needed since audit logs from both regions are sent to this bucket in US
		const itDestinationBucketName = "samsara-databricks-audit-logs-splunk"
		auditLogReplicationIAMResources := createAuditLogReplicationIAMResources(c, itDestinationBucketName)
		auditLogBucket.ReplicationConfiguration = awsresource.S3ReplicationConfiguration{
			Role: fmt.Sprintf("arn:aws:iam::%d:role/%s", c.DatabricksAWSAccountId, "databricks-audit-log-replicaton-to-it-splunk-bucket"),
			Rules: []awsresource.S3ReplicationRule{
				{
					ID:     "replicate-entire-bucket-to-it-splunk-bucket",
					Status: "Enabled",
					Destination: awsresource.S3ReplicationDestination{
						Bucket:    fmt.Sprintf("arn:aws:s3:::%s", itDestinationBucketName),
						AccountID: infraconsts.SamsaraAWSITAccountID,
						AccessControlTranslation: awsresource.S3AccessControlTranslation{
							Owner: "Destination",
						},
					},
				},
			},
		}
		auditLogResources = append(auditLogResources, auditLogReplicationIAMResources...)
	}

	auditLogBucketResources := append(auditLogBucket.Resources(), auditLogBucketPolicy)
	auditLogResources = append(
		auditLogResources,
		auditLogBucketResources...,
	)

	var billingResources []tf.Resource
	// Billing is consolidated in one region.
	if c.Region == infraconsts.SamsaraAWSDefaultRegion {
		databricksBillBucket := dataplatformresource.Bucket{
			Name:                             "databricks-billing",
			Region:                           c.Region,
			RnDCostAllocation:                1,
			NonCurrentExpirationDaysOverride: 7,
			LoggingBucket:                    fmt.Sprintf("%sdatabricks-s3-logging", awsregionconsts.RegionPrefix[c.Region]),
		}
		billingBucketPolicy := &awsresource.S3BucketPolicy{
			Bucket: databricksBillBucket.Bucket().ResourceId(),
			Policy: policy.AWSPolicy{
				Version: policy.AWSPolicyVersion,
				Statement: []policy.AWSPolicyStatement{
					{
						Sid: "AllowReadFromMainAccount",
						Principal: map[string]string{
							"AWS": fmt.Sprintf(
								"arn:aws:iam::%d:root",
								c.AWSAccountId,
							),
						},
						Effect: "Allow",
						Action: []string{"s3:Get*", "s3:List*"},
						Resource: []string{
							databricksBillBucket.Bucket().ResourceId().ReferenceAttr("arn"),
							fmt.Sprintf("%s/*", databricksBillBucket.Bucket().ResourceId().ReferenceAttr("arn")),
						},
					},
					{
						Sid:       "RequireTLS",
						Effect:    "Deny",
						Principal: map[string]string{"AWS": "*"},
						Action:    []string{"s3:*"},
						Resource: []string{
							databricksBillBucket.Bucket().ResourceId().ReferenceAttr("arn"),
							fmt.Sprintf("%s/*", databricksBillBucket.Bucket().ResourceId().ReferenceAttr("arn")),
						},
						Condition: &policy.AWSPolicyCondition{
							Bool: &policy.AWSPolicyConditionBool{SecureTransportEnabled: pointer.BoolPtr(false)},
						},
					},
				},
			},
		}
		billingResources = append(databricksBillBucket.Resources(), billingBucketPolicy)
	}

	inventoryBucket := dataplatformresource.Bucket{
		Name:                             "databricks-s3-inventory",
		Region:                           c.Region,
		NonCurrentExpirationDaysOverride: 1,
		ExtraLifecycleRules: []awsresource.S3LifecycleRule{
			{
				ID:      "expire-after-ninety-days",
				Enabled: true,
				Expiration: awsresource.S3LifecycleRuleExpiration{
					Days: 90,
				},
			},
		},
		RnDCostAllocation: 1,
		LoggingBucket:     fmt.Sprintf("%sdatabricks-s3-logging", awsregionconsts.RegionPrefix[c.Region]),
	}
	inventoryBucketPolicy := &awsresource.S3BucketPolicy{
		Bucket: inventoryBucket.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Sid:    "AllowSameAccountBucketWrite",
					Effect: "Allow",
					Principal: map[string]string{
						"Service": "s3.amazonaws.com",
					},
					Action:   []string{"s3:PutObject"},
					Resource: []string{fmt.Sprintf("%s/*", inventoryBucket.Bucket().ResourceId().ReferenceAttr("arn"))},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{
							"aws:SourceAccount": strconv.Itoa(infraconsts.GetDatabricksAccountIdForRegion(c.Region)),
							"s3:x-amz-acl":      "bucket-owner-full-control",
						},
					},
				},
				{
					Sid:       "RequireTLS",
					Effect:    "Deny",
					Principal: map[string]string{"AWS": "*"},
					Action:    []string{"s3:*"},
					Resource: []string{
						inventoryBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", inventoryBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
					Condition: &policy.AWSPolicyCondition{
						Bool: &policy.AWSPolicyConditionBool{SecureTransportEnabled: pointer.BoolPtr(false)},
					},
				},
			},
		},
	}

	// biztechenterprisedataAndBiztechProdS3BucketReadWritePolicy is intended for use by
	// biztechenterprisedata-cluster and biztech-prod-cluster IAM roles. We are migrating shared
	// inline policies to managed policies to get back under the 10240 aggregate byte limit for
	// inline policies.
	biztechWriteBucketArns, err := getSharedBiztechS3ARNs(c)
	if err != nil {
		return nil, oops.Wrapf(err, "Error getting arns for biztech-s3-bucket-read-write managed policy")
	}

	var biztechenterprisedataAndBiztechProdS3BucketReadWritePolicy *awsresource.IAMPolicy

	// Set this Policy only when biztechWriteBucketArns resources > 0.
	if len(biztechWriteBucketArns) > 0 {

		biztechenterprisedataAndBiztechProdS3BucketReadWritePolicy = &awsresource.IAMPolicy{
			ResourceName: "biztech_s3_bucket_read_write",
			Name:         "biztech-s3-bucket-read-write",
			Policy: policy.AWSPolicy{
				Version: policy.AWSPolicyVersion,
				Statement: []policy.AWSPolicyStatement{
					policy.AWSPolicyStatement{
						Effect: "Allow",
						Action: []string{
							"s3:*",
						},
						Resource: biztechWriteBucketArns,
					},
				},
			},
		}
	}

	// Create a bucket to store VPC flow logs.
	// There will be one bucket per region/aws account.
	vpcFlowLogs := dataplatformresource.Bucket{
		Name:                             "databricks-aws-vpc-flow-logs",
		Region:                           c.Region,
		NonCurrentExpirationDaysOverride: 1,
		LoggingBucket:                    fmt.Sprintf("%sdatabricks-s3-logging", awsregionconsts.RegionPrefix[c.Region]),
		Metrics:                          true,
		RnDCostAllocation:                1,
		EnableS3IntelligentTiering:       true,
		CurrentExpirationDays:            7,
	}

	vpcFlowLogsPolicy := &awsresource.S3BucketPolicy{
		Bucket: vpcFlowLogs.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Sid:    "AWSLogDeliveryWrite",
					Effect: "Allow",
					Principal: map[string]string{
						"Service": "delivery.logs.amazonaws.com",
					},
					Action: []string{
						"s3:PutObject",
					},
					Resource: []string{
						vpcFlowLogs.Bucket().ResourceId().ReferenceAttr("arn"),
						vpcFlowLogs.Bucket().ResourceId().ReferenceAttr("arn") + "/*",
					},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{
							"aws:SourceAccount": fmt.Sprintf("%v", c.DatabricksAWSAccountId),
							"s3:x-amz-acl":      "bucket-owner-full-control",
						},
						ARNLike: &policy.ARNLike{
							SourceARN: fmt.Sprintf("arn:aws:logs:%s:%v:*", c.Region, c.DatabricksAWSAccountId),
						},
					},
				},
				{
					Sid:    "AWSLogDeliveryAclCheck",
					Effect: "Allow",
					Principal: map[string]string{
						"Service": "delivery.logs.amazonaws.com",
					},
					Action: []string{
						"s3:Get*",
						"s3:List*",
					},
					Resource: []string{
						vpcFlowLogs.Bucket().ResourceId().ReferenceAttr("arn"),
						vpcFlowLogs.Bucket().ResourceId().ReferenceAttr("arn") + "/*",
					},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{
							"aws:SourceAccount": fmt.Sprintf("%v", c.DatabricksAWSAccountId),
						},
						ARNLike: &policy.ARNLike{
							SourceARN: fmt.Sprintf("arn:aws:logs:%s:%v:*", c.Region, c.DatabricksAWSAccountId),
						},
					},
				},
			},
		},
	}

	// Create a bucket to store DNS Resolver Query logs.
	// There will be one bucket per region/aws account.
	dnsResolverQueryLogsBucket := dataplatformresource.Bucket{
		Name:                             "databricks-dns-resolver-query-logs",
		Region:                           c.Region,
		NonCurrentExpirationDaysOverride: 1,
		LoggingBucket:                    fmt.Sprintf("%sdatabricks-s3-logging", awsregionconsts.RegionPrefix[c.Region]),
		Metrics:                          true,
		RnDCostAllocation:                1,
		EnableS3IntelligentTiering:       true,
		CurrentExpirationDays:            7,
	}

	output := map[string][]tf.Resource{
		"block_public_access": []tf.Resource{
			publicAccessBlock,
		},
		"cluster_log": append(
			clusterLogBucket.Resources(),
			clusterLoggingPolicy,
		),
		"query_history": append(
			sqlQueryHistoryBucket.Resources(),
			sqlQueryHistoryBucketPolicy,
		),
		"audit_log": auditLogResources,
		"s3_logging": append(
			loggingBucket.Resources(),
			loggingBucketLocal,
			bucketLoggingPolicy,
		),
		"s3_inventory": append(
			inventoryBucket.Resources(),
			inventoryBucketPolicy,
		),
		"s3_standard_policies": []tf.Resource{
			standardDevReadBucketsPolicy,
			standardDevWriteBucketsPolicy,
		},
		"vpc_flow_logs_bucket": append(
			vpcFlowLogs.Resources(),
			vpcFlowLogsPolicy,
		),
		"dns_resolver_query_logs_bucket": dnsResolverQueryLogsBucket.Resources(),
	}
	if len(billingResources) > 0 {
		output["databricks_bill"] = billingResources
	}

	if biztechenterprisedataAndBiztechProdS3BucketReadWritePolicy != nil {
		output["s3_custom_policies"] = []tf.Resource{
			biztechenterprisedataAndBiztechProdS3BucketReadWritePolicy,
		}
	}

	return output, nil
}

func DatabricksResources(c dataplatformconfig.DatabricksConfig) map[string][]tf.Resource {
	crossAccountRole := &awsresource.IAMRole{
		ResourceName: "databricks_cross_account_role",
		Name:         "databricks-cross-account-role",
		Description:  "Role assumed by databricks to manage EC2 resources.",
		AssumeRolePolicy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				policy.AWSPolicyStatement{
					Principal: map[string]string{
						"AWS": fmt.Sprintf("arn:aws:iam::%s:root", tf.LocalId("databricks_aws_account").Reference()),
					},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{
							"sts:ExternalId": tf.LocalId("databricks_external_id").Reference(),
						},
					},
					Effect: "Allow",
					Action: []string{
						"sts:AssumeRole",
					},
				},
			},
		},
		Tags: map[string]string{
			"samsara:service":       "databricks-cross-account-role",
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}

	manageClusterPolicy := &awsresource.IAMRolePolicy{
		ResourceName: "databricks_cross_account_role_manage_cluster",
		Name:         "manage-cluster",
		Role:         crossAccountRole.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Action: []string{
						"ec2:CreateDhcpOptions",
						"ec2:AuthorizeSecurityGroupIngress",
						"ec2:DeleteSubnet",
						"ec2:DescribeInstances",
						"ec2:CreateKeyPair",
						"ec2:AttachInternetGateway",
						"ec2:DescribePlacementGroups",
						"ec2:ReplaceIamInstanceProfileAssociation",
						"ec2:AssociateRouteTable",
						"ec2:DeleteRouteTable",
						"ec2:DeleteVolume",
						"ec2:CreatePlacementGroup",
						"ec2:RevokeSecurityGroupEgress",
						"ec2:CreateRoute",
						"ec2:CreateInternetGateway",
						"ec2:DescribeVolumes",
						"ec2:DeleteInternetGateway",
						"ec2:CreateVpcPeeringConnection",
						"ec2:DescribeRouteTables",
						"ec2:CreateTags",
						"ec2:DescribeReservedInstancesOfferings",
						"ec2:RunInstances",
						"ec2:DetachInternetGateway",
						"ec2:DescribePrefixLists",
						"ec2:CreateVolume",
						"ec2:RevokeSecurityGroupIngress",
						"ec2:CancelSpotInstanceRequests",
						"ec2:DisassociateIamInstanceProfile",
						"ec2:DeleteVpc",
						"ec2:CreateSubnet",
						"ec2:DescribeSubnets",
						"ec2:DeleteKeyPair",
						"ec2:AttachVolume",
						"ec2:RequestSpotInstances",
						"ec2:DeleteTags",
						"ec2:CreateVpc",
						"ec2:DescribeSpotInstanceRequests",
						"ec2:DescribeSpotPriceHistory",
						"ec2:DescribeAvailabilityZones",
						"ec2:CreateSecurityGroup",
						"ec2:ModifyVpcAttribute",
						"ec2:DescribeInstanceStatus",
						"ec2:AuthorizeSecurityGroupEgress",
						"ec2:AssociateDhcpOptions",
						"ec2:TerminateInstances",
						"ec2:DeletePlacementGroup",
						"ec2:DescribeIamInstanceProfileAssociations",
						"ec2:DeleteRoute",
						"ec2:DescribeSecurityGroups",
						"ec2:DescribeVpcs",
						"ec2:DeleteSecurityGroup",
						"ec2:AssociateIamInstanceProfile",
					},
					Resource: []string{"*"},
				},
				{
					Effect: "Allow",
					Action: []string{"iam:CreateServiceLinkedRole"},
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
					Action: []string{"iam:PutRolePolicy"},
					Resource: []string{
						"arn:aws:iam::*:role/aws-service-role/spot.amazonaws.com/AWSServiceRoleForEC2Spot",
					},
				},
			},
		},
	}

	passEc2Roles := &awsresource.IAMRolePolicy{
		ResourceName: "databricks_cross_account_role_pass_ec2_instance_roles",
		Name:         "pass-ec2-instance-roles",
		Role:         crossAccountRole.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Action: []string{"iam:PassRole"},
					Resource: []string{
						fmt.Sprintf("arn:aws:iam::%s:role/ec2-instance/*",
							genericresource.DataResourceId("aws_caller_identity", "current").ReferenceAttr("account_id")),
					},
				},
			},
		},
	}

	// https://docs.databricks.com/clusters/configure.html#enforce-mandatory-tags
	denyUntagged := &awsresource.IAMRolePolicy{
		ResourceName: "databricks_cross_account_role_deny_untagged",
		Name:         "deny-untagged",
		Role:         crossAccountRole.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
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
					Sid:    "MustTagWithSamsaraTeam",
					Effect: "Deny",
					Action: []string{
						"ec2:CreateTags",
						"ec2:RunInstances",
					},
					Resource: []string{"arn:aws:ec2:*:*:instance/*"},
					Condition: &policy.AWSPolicyCondition{
						StringNotLike: map[string]string{
							"aws:RequestTag/samsara:team": "?*",
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
		},
	}

	type prefix struct {
		shardId     string
		workspaceId string
	}
	p := map[string]prefix{
		// US deployment is on the default multi-tenant shard.
		infraconsts.SamsaraAWSDefaultRegion: prefix{
			shardId:     "oregon-prod",
			workspaceId: "8972003451708087",
		},
		// EU deployment is on a single-tenant shard.
		infraconsts.SamsaraAWSEURegion: prefix{
			shardId:     "dbc-ce1cca55-f3f6",
			workspaceId: "0",
		},
		// CA deployment is on a single-tenant shard.
		infraconsts.SamsaraAWSCARegion: prefix{
			shardId:     "ca-central-shard-prod",
			workspaceId: "0",
		},
	}[c.Region]

	bucket := dataplatformresource.Bucket{
		Name:          "databricks",
		Region:        c.Region,
		LoggingBucket: tf.LocalId("logging_bucket").Reference(),
		ExtraLifecycleRules: []awsresource.S3LifecycleRule{
			// Expire cluster logs after 60 days.
			awsresource.ExpireCurrentVersionPrefixLifecycleRule(60, fmt.Sprintf("%s/%s/cluster-logs/", p.shardId, p.workspaceId)),
		},
		NonCurrentExpirationDaysOverride: 1,
		RnDCostAllocation:                1,
	}
	bucketPolicy := &awsresource.S3BucketPolicy{
		Bucket: bucket.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				policy.AWSPolicyStatement{
					Principal: map[string]string{
						"AWS": fmt.Sprintf("arn:aws:iam::%s:root", tf.LocalId("databricks_aws_account").Reference()),
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
						bucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", bucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
				{
					Sid:       "RequireTLS",
					Effect:    "Deny",
					Principal: map[string]string{"AWS": "*"},
					Action:    []string{"s3:*"},
					Resource: []string{
						bucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", bucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
					Condition: &policy.AWSPolicyCondition{
						Bool: &policy.AWSPolicyConditionBool{SecureTransportEnabled: pointer.BoolPtr(false)},
					},
				},
			},
		},
	}

	resources := append(
		bucket.Resources(),
		crossAccountRole,
		manageClusterPolicy,
		passEc2Roles,
		denyUntagged,
		bucketPolicy,
	)
	if c.Region == infraconsts.SamsaraAWSDefaultRegion {
		auditLogDeliveryRole := dataplatformresource.Role{
			Name:        "databricks-audit-log-delivery-role",
			Description: "Databricks audit log role for all workspaces",
			AssumeRolePolicy: policy.AWSPolicyStatement{
				Principal: map[string]string{
					"AWS": "arn:aws:iam::414351767826:role/SaasUsageDeliveryRole-prod-IAMRole-3PLHICCRR1TK",
				},
				Condition: &policy.AWSPolicyCondition{
					StringEquals: map[string]string{
						"sts:ExternalId": dataplatformconfig.DatabricksAccountId,
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
						"s3:ListBucket",
						"s3:ListBucketMultipartUploads",
					},
					Resource: []string{"arn:aws:s3:::samsara-databricks-audit-log"},
				},
				{
					Effect: "Allow",
					Action: []string{
						"s3:AbortMultipartUpload",
						"s3:DeleteObject",
						"s3:GetObject",
						"s3:GetObjectMetadata",
						"s3:ListMultipartUploadParts",
					},
					Resource: []string{"arn:aws:s3:::samsara-databricks-audit-log/*"},
				},
				{
					Effect: "Allow",
					Action: []string{
						"s3:PutObject",
						"s3:PutObjectAcl",
					},
					Resource: []string{
						"arn:aws:s3:::samsara-databricks-audit-log/*",
					},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{
							"s3:x-amz-acl": "bucket-owner-full-control",
						},
					},
				},
			},
		}
		resources = append(resources, auditLogDeliveryRole.Resources()...)
	}

	return map[string][]tf.Resource{
		"databricks": resources,
	}
}

func BuildkiteResources() map[string][]tf.Resource {
	role := &awsresource.IAMRole{
		ResourceName: "buildkite_ci_terraform",
		Name:         "buildkite-ci-terraform",
		Description:  "Role assumed by Buildkite CI.",
		AssumeRolePolicy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Principal: map[string]string{
						"AWS": fmt.Sprintf("arn:aws:iam::%d:root", infraconsts.SamsaraAWSBuildkiteAccountID),
					},
					Action: []string{
						"sts:AssumeRole",
					},
				},
			},
		},
		Tags: map[string]string{
			"samsara:service":       "buildkite",
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}
	adminAttachment := &awsresource.IAMRolePolicyAttachment{
		Name:      "buildkite_ci_terraform_admin",
		Role:      role.ResourceId().Reference(),
		PolicyARN: "arn:aws:iam::aws:policy/AdministratorAccess",
	}
	return map[string][]tf.Resource{
		"buildkite": []tf.Resource{
			role,
			adminAttachment,
		},
	}
}

func Cloudtrail(c dataplatformconfig.DatabricksConfig) map[string][]tf.Resource {
	bucket := dataplatformresource.Bucket{
		Name:                             "databricks-cloudtrail",
		Region:                           c.Region,
		CurrentExpirationDays:            90,
		NonCurrentExpirationDaysOverride: 1,
		RnDCostAllocation:                1,
		LoggingBucket:                    fmt.Sprintf("%sdatabricks-s3-logging", awsregionconsts.RegionPrefix[c.Region]),
	}
	bucketPolicy := &awsresource.S3BucketPolicy{
		Bucket: bucket.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				policy.AWSPolicyStatement{
					Sid:    "AWSCloudTrailAclCheck",
					Effect: "Allow",
					Principal: map[string]string{
						"Service": "cloudtrail.amazonaws.com",
					},
					Action:   []string{"s3:GetBucketAcl"},
					Resource: []string{bucket.Bucket().ResourceId().ReferenceAttr("arn")},
				},
				policy.AWSPolicyStatement{
					Sid:    "AWSCloudTrailWrite",
					Effect: "Allow",
					Principal: map[string]string{
						"Service": "cloudtrail.amazonaws.com",
					},
					Action: []string{"s3:PutObject"},
					Resource: []string{
						fmt.Sprintf(
							"%s/AWSLogs/%s/*",
							bucket.Bucket().ResourceId().ReferenceAttr("arn"),
							genericresource.DataResourceId("aws_caller_identity", "current").ReferenceAttr("account_id")),
					},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{
							"s3:x-amz-acl": "bucket-owner-full-control",
						},
					},
				},
				{
					Sid:       "RequireTLS",
					Effect:    "Deny",
					Principal: map[string]string{"AWS": "*"},
					Action:    []string{"s3:*"},
					Resource: []string{
						bucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", bucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
					Condition: &policy.AWSPolicyCondition{
						Bool: &policy.AWSPolicyConditionBool{SecureTransportEnabled: pointer.BoolPtr(false)},
					},
				},
			},
		},
	}
	trail := &awsresource.CloudTrail{
		Name:                       "trail",
		S3BucketName:               bucket.Bucket().ResourceId().Reference(),
		IncludeGlobalServiceEvents: true,
		EnableLogging:              true,
		Tags: map[string]string{
			"samsara:team":          strings.ToLower(team.DataPlatform.Name()),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.Name()]),
			"samsara:service":       "databricks-cloudtrail",
		},
		IsMultiRegionTrail: true,
		EventSelector: awsresource.CloudTrailEventSelector{
			ReadWriteType:           "All",
			IncludeManagementEvents: true,
		},
	}
	return map[string][]tf.Resource{
		"cloudtrail": append(
			bucket.Resources(),
			bucketPolicy,
			trail,
		),
	}
}

type VpcArgs struct {
	Id string `hcl:"id"`
}

type IgwArgs struct {
	Filter IgwFilter `hcl:"filter,block"`
}

type IgwFilter struct {
	Name   string   `hcl:"name"`
	Values []string `hcl:"values"`
}

func VPC(c dataplatformconfig.DatabricksConfig) map[string][]tf.Resource {
	databricksVpc := &genericresource.Data{
		Type: "aws_vpc",
		Name: "databricks",
		Args: VpcArgs{
			Id: c.DatabricksVPCId,
		},
	}
	databricksIgw := &genericresource.Data{
		Type: "aws_internet_gateway",
		Name: "databricks",
		Args: IgwArgs{
			Filter: IgwFilter{
				Name: "attachment.vpc-id",
				Values: []string{
					databricksVpc.ResourceId().Reference(),
				},
			},
		},
	}

	// TODO: is this correct for EU?
	prodUsPeering := &awsresource.VPCPeeringConnection{
		Name:        "prod-us",
		VpcId:       databricksVpc.ResourceId().Reference(),
		PeerOwnerId: strconv.Itoa(c.AWSAccountId),
		PeerVpcId:   c.ProdVPCId,
		Tags: map[string]string{
			"Name":                  "prod-us",
			"samsara:service":       "prod-us",
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}
	// Create a Route53 Resolver DNSSEC config
	dnssecConfig := &awsresource.Route53ResolverDnssecConfig{
		Name:  "databricks-dnssec-resolver",
		VpcID: databricksVpc.ResourceId().Reference(),
	}

	routeTable := &awsresource.DefaultRouteTable{
		Id: databricksVpc.ResourceId().ReferenceAttr("main_route_table_id"),
		Tags: []*awsresource.Tag{
			{
				Key: "Name", Value: "databricks",
			},
			{
				Key: "samsara:service", Value: "databricks",
			},
			{
				Key: "samsara:product-group", Value: strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
			},
			{
				Key: "samsara:team", Value: strings.ToLower(team.DataPlatform.TeamName),
			},
		},
		Routes: []awsresource.Route{
			awsresource.Route{
				CidrBlock:              "10.0.0.0/16",
				VPCPeeringConnectionId: prodUsPeering.ResourceId().Reference(),
			},
			awsresource.Route{
				CidrBlock:              "10.1.0.0/16",
				VPCPeeringConnectionId: prodUsPeering.ResourceId().Reference(),
			},
			awsresource.Route{
				CidrBlock:         "0.0.0.0/0",
				InternetGatewayId: databricksIgw.ResourceId().Reference(),
			},
		},
	}

	return map[string][]tf.Resource{
		"vpc": []tf.Resource{
			databricksVpc,
			databricksIgw,
			prodUsPeering,
			routeTable,
			dnssecConfig,
		},
	}
}

func Glue(config dataplatformconfig.DatabricksConfig) (map[string][]tf.Resource, error) {
	arnResource := fmt.Sprintf(
		"arn:aws:glue:%s:%s",
		genericresource.DataResourceId("aws_region", "current").ReferenceAttr("name"),
		genericresource.DataResourceId("aws_caller_identity", "current").ReferenceAttr("account_id"),
	)
	readOnlyPolicy := &awsresource.IAMPolicy{
		ResourceName: "glue_catalog_readonly",
		Name:         "glue-catalog-readonly",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect:   "Allow",
					Resource: []string{fmt.Sprintf("%s:*", arnResource)},
					Action: []string{
						"glue:GetDatabase",
						"glue:GetDatabases",
						"glue:GetPartition",
						"glue:GetPartitions",
						"glue:GetTable",
						"glue:GetTables",
						"glue:GetUserDefinedFunction",
						"glue:GetUserDefinedFunctions",
					},
				},
				{
					Effect: "Deny",
					Resource: []string{
						fmt.Sprintf("%s:database/playground", arnResource),
						fmt.Sprintf("%s:table/playground/*", arnResource),
					},
					Action: []string{
						"glue:GetDatabase",
						"glue:GetDatabases",
						"glue:GetPartition",
						"glue:GetPartitions",
						"glue:GetTable",
						"glue:GetTables",
						"glue:GetUserDefinedFunction",
						"glue:GetUserDefinedFunctions",
					},
				},
			},
		},
	}
	adminPolicy := &awsresource.IAMPolicy{
		ResourceName: "glue_catalog_admin",
		Name:         "glue-catalog-admin",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect:   "Allow",
					Resource: []string{fmt.Sprintf("%s:*", arnResource)},
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
				},
			},
		},
	}

	var rayWriteDeleteResourcePolicyStatement = policy.AWSPolicyStatement{
		Effect: "Allow",
		Action: []string{
			"glue:CreateTable",
			"glue:CreatePartition",
			"glue:UpdateDatabase",
			"glue:UpdateTable",
			"glue:UpdatePartition",
			"glue:DeleteTable",
			"glue:DeletePartition",
			"glue:DeleteTableVersion",
			"glue:BatchDeleteTableVersion",
		},
		Principal: map[string]string{
			"AWS": fmt.Sprintf("arn:aws:iam::%s:root", infraconsts.SamsaraAWSDataScienceAccountID),
		},
		Resource: []string{
			fmt.Sprintf("arn:aws:glue:%s:%d:catalog", config.Region, config.DatabricksAWSAccountId),
			fmt.Sprintf("arn:aws:glue:%s:%d:database/dojo", config.Region, config.DatabricksAWSAccountId),
			fmt.Sprintf("arn:aws:glue:%s:%d:table/dojo/*", config.Region, config.DatabricksAWSAccountId),
			fmt.Sprintf("arn:aws:glue:%s:%d:tableVersion/dojo/*/*", config.Region, config.DatabricksAWSAccountId),
			fmt.Sprintf("arn:aws:glue:%s:%d:database/dojo_tmp", config.Region, config.DatabricksAWSAccountId),
			fmt.Sprintf("arn:aws:glue:%s:%d:table/dojo_tmp/*", config.Region, config.DatabricksAWSAccountId),
			fmt.Sprintf("arn:aws:glue:%s:%d:tableVersion/dojo_tmp/*/*", config.Region, config.DatabricksAWSAccountId),
			fmt.Sprintf("arn:aws:glue:%s:%d:database/labelbox", config.Region, config.DatabricksAWSAccountId),
			fmt.Sprintf("arn:aws:glue:%s:%d:table/labelbox/*", config.Region, config.DatabricksAWSAccountId),
			fmt.Sprintf("arn:aws:glue:%s:%d:tableVersion/labelbox/*/*", config.Region, config.DatabricksAWSAccountId),
		},
		Condition: &policy.AWSPolicyCondition{
			ForAnyValueStringEquals: map[string][]string{
				"aws:PrincipalArn": []string{
					fmt.Sprintf("arn:aws:iam::%s:role/ray-head", infraconsts.SamsaraAWSDataScienceAccountID),
					fmt.Sprintf("arn:aws:iam::%s:role/ray-worker", infraconsts.SamsaraAWSDataScienceAccountID),
				},
			},
		},
	}

	var predictiveMaintenanceWriteDeletePolicyStatement = policy.AWSPolicyStatement{
		Effect: "Allow",
		Action: []string{
			"glue:CreateTable",
			"glue:CreatePartition",
			"glue:UpdateDatabase",
			"glue:UpdateTable",
			"glue:UpdatePartition",
			"glue:DeleteTable",
			"glue:DeletePartition",
			"glue:DeleteTableVersion",
			"glue:BatchDeleteTableVersion",
		},
		Principal: map[string]string{
			"AWS": fmt.Sprintf("arn:aws:iam::%s:root", infraconsts.SamsaraAWSDataScienceAccountID),
		},
		Resource: []string{
			fmt.Sprintf("arn:aws:glue:%s:%d:catalog", config.Region, config.DatabricksAWSAccountId),
			fmt.Sprintf("arn:aws:glue:%s:%d:database/aiml_predictive_maintenance", config.Region, config.DatabricksAWSAccountId),
			fmt.Sprintf("arn:aws:glue:%s:%d:table/aiml_predictive_maintenance/*", config.Region, config.DatabricksAWSAccountId),
			fmt.Sprintf("arn:aws:glue:%s:%d:tableVersion/aiml_predictive_maintenance/*/*", config.Region, config.DatabricksAWSAccountId),
			fmt.Sprintf("arn:aws:glue:%s:%d:database/aiml_staging_predictive_maintenance", config.Region, config.DatabricksAWSAccountId),
			fmt.Sprintf("arn:aws:glue:%s:%d:table/aiml_staging_predictive_maintenance/*", config.Region, config.DatabricksAWSAccountId),
			fmt.Sprintf("arn:aws:glue:%s:%d:tableVersion/aiml_staging_predictive_maintenance/*/*", config.Region, config.DatabricksAWSAccountId),
			fmt.Sprintf("arn:aws:glue:%s:%d:database/aiml_staging_predictive_maintenance_prediction", config.Region, config.DatabricksAWSAccountId),
			fmt.Sprintf("arn:aws:glue:%s:%d:table/aiml_staging_predictive_maintenance_prediction/*", config.Region, config.DatabricksAWSAccountId),
			fmt.Sprintf("arn:aws:glue:%s:%d:tableVersion/aiml_staging_predictive_maintenance_prediction/*/*", config.Region, config.DatabricksAWSAccountId),
		},
		Condition: &policy.AWSPolicyCondition{
			ForAnyValueStringEquals: map[string][]string{
				"aws:PrincipalArn": []string{
					fmt.Sprintf("arn:aws:iam::%s:role/ml-pipeline-head", infraconsts.SamsaraAWSDataScienceAccountID),
					fmt.Sprintf("arn:aws:iam::%s:role/ml-pipeline-worker", infraconsts.SamsaraAWSDataScienceAccountID),
				},
			},
		},
	}

	var predictiveMaintenanceShadowWriteDeletePolicyStatement = policy.AWSPolicyStatement{
		Effect: "Allow",
		Action: []string{
			"glue:CreateTable",
			"glue:CreatePartition",
			"glue:UpdateDatabase",
			"glue:UpdateTable",
			"glue:UpdatePartition",
			"glue:DeleteTable",
			"glue:DeletePartition",
			"glue:DeleteTableVersion",
			"glue:BatchDeleteTableVersion",
		},
		Principal: map[string]string{
			"AWS": fmt.Sprintf("arn:aws:iam::%s:root", infraconsts.SamsaraAWSDataScienceAccountID),
		},
		Resource: []string{
			fmt.Sprintf("arn:aws:glue:%s:%d:catalog", config.Region, config.DatabricksAWSAccountId),
			fmt.Sprintf("arn:aws:glue:%s:%d:database/aiml_shadow_predictive_maintenance_prediction", config.Region, config.DatabricksAWSAccountId),
			fmt.Sprintf("arn:aws:glue:%s:%d:table/aiml_shadow_predictive_maintenance_prediction/*", config.Region, config.DatabricksAWSAccountId),
			fmt.Sprintf("arn:aws:glue:%s:%d:tableVersion/aiml_shadow_predictive_maintenance_prediction/*/*", config.Region, config.DatabricksAWSAccountId),
		},
		Condition: &policy.AWSPolicyCondition{
			ForAnyValueStringEquals: map[string][]string{
				"aws:PrincipalArn": []string{
					fmt.Sprintf("arn:aws:iam::%s:role/ml-pipeline-shadow", infraconsts.SamsaraAWSDataScienceAccountID),
					fmt.Sprintf("arn:aws:iam::%s:role/ml-pipeline-shadow-worker", infraconsts.SamsaraAWSDataScienceAccountID),
				},
			},
		},
	}

	var rayReadOnlyResourcePolicyStatement = policy.AWSPolicyStatement{
		Effect: "Allow",
		Action: []string{
			"glue:GetPartition",
			"glue:GetPartitions",
			"glue:GetTable",
			"glue:GetTables",
			"glue:GetTableVersion",
			"glue:GetTableVersions",
			"glue:GetDatabase",
			"glue:GetDatabases",
			"glue:GetUserDefinedFunction",
			"glue:GetUserDefinedFunctions",
		},
		Principal: map[string]string{
			"AWS": fmt.Sprintf("arn:aws:iam::%s:root", infraconsts.SamsaraAWSDataScienceAccountID),
		},
		Resource: []string{
			fmt.Sprintf("arn:aws:glue:%s:%d:catalog", config.Region, config.DatabricksAWSAccountId),
			fmt.Sprintf("arn:aws:glue:%s:%d:database/*", config.Region, config.DatabricksAWSAccountId),
			fmt.Sprintf("arn:aws:glue:%s:%d:table/*/*", config.Region, config.DatabricksAWSAccountId),
			fmt.Sprintf("arn:aws:glue:%s:%d:tableVersion/*/*/*", config.Region, config.DatabricksAWSAccountId),
			fmt.Sprintf("arn:aws:glue:%s:%d:*", config.Region, config.DatabricksAWSAccountId),
		},
		Condition: &policy.AWSPolicyCondition{
			ForAnyValueStringEquals: map[string][]string{
				"aws:PrincipalArn": []string{
					fmt.Sprintf("arn:aws:iam::%s:role/ray-head", infraconsts.SamsaraAWSDataScienceAccountID),
					fmt.Sprintf("arn:aws:iam::%s:role/ray-worker", infraconsts.SamsaraAWSDataScienceAccountID),
					fmt.Sprintf("arn:aws:iam::%s:role/ml-pipeline-head", infraconsts.SamsaraAWSDataScienceAccountID),
					fmt.Sprintf("arn:aws:iam::%s:role/ml-pipeline-worker", infraconsts.SamsaraAWSDataScienceAccountID),
					fmt.Sprintf("arn:aws:iam::%s:role/ml-pipeline-shadow", infraconsts.SamsaraAWSDataScienceAccountID),
					fmt.Sprintf("arn:aws:iam::%s:role/ml-pipeline-shadow-worker", infraconsts.SamsaraAWSDataScienceAccountID),
				},
			},
		},
	}

	//var rayGlueEncryptionPolicyStatement = policy.AWSPolicyStatement{
	//	Effect: "Allow",
	//	Action: []string{
	//		"kms:Decrypt",
	//		"kms:Encrypt",
	//		"kms:GenerateDataKey",
	//	},
	//	Principal: map[string]string{
	//		"AWS": fmt.Sprintf("arn:aws:iam::%s:root", infraconsts.SamsaraAWSDataScienceAccountID),
	//	},
	//	Resource: []string{
	//		fmt.Sprintf("arn:aws:kms:%s:%d:alias/aws/glue", config.Region, config.DatabricksAWSAccountId),
	//	},
	//	Condition: &policy.AWSPolicyCondition{
	//		ForAnyValueStringEquals: map[string][]string{
	//			"aws:PrincipalArn": []string{
	//				fmt.Sprintf("arn:aws:iam::%s:role/ray-head", infraconsts.SamsaraAWSDataScienceAccountID),
	//				fmt.Sprintf("arn:aws:iam::%s:role/ray-worker", infraconsts.SamsaraAWSDataScienceAccountID),
	//				fmt.Sprintf("arn:aws:iam::%s:role/ml-pipeline-head", infraconsts.SamsaraAWSDataScienceAccountID),
	//				fmt.Sprintf("arn:aws:iam::%s:role/ml-pipeline-worker", infraconsts.SamsaraAWSDataScienceAccountID),
	//				fmt.Sprintf("arn:aws:iam::%s:role/ml-pipeline-shadow", infraconsts.SamsaraAWSDataScienceAccountID),
	//				fmt.Sprintf("arn:aws:iam::%s:role/ml-pipeline-shadow-worker", infraconsts.SamsaraAWSDataScienceAccountID),
	//			},
	//		},
	//	},
	//}

	// NOTE: There can only be a single Glue resource policy per account, and that policy should < 10KB
	// Reference: https://docs.aws.amazon.com/glue/latest/dg/glue-resource-policies.html
	glueResourcePolicy := &awsresource.GlueResourcePolicy{
		Name: "databricks_account_glue_resource_policy",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Action: []string{
						"glue:GetTable",
						"glue:GetTables",
						"glue:GetTableVersion",
						"glue:GetTableVersions",
						"glue:CreatePartition",
						"glue:GetDatabases",
						"glue:BatchDeleteTableVersion",
						"glue:DeleteTableVersion",
					},
					Principal: map[string]string{
						"AWS": fmt.Sprintf("arn:aws:iam::%d:root", awsregionconsts.RegionAccountID[config.Region]),
					},
					Resource: []string{
						fmt.Sprintf("arn:aws:glue:%s:%d:catalog", config.Region, config.DatabricksAWSAccountId),
						fmt.Sprintf("arn:aws:glue:%s:%d:database/*", config.Region, config.DatabricksAWSAccountId),
						fmt.Sprintf("arn:aws:glue:%s:%d:table/*/*", config.Region, config.DatabricksAWSAccountId),
						fmt.Sprintf("arn:aws:glue:%s:%d:tableVersion/*/*/*", config.Region, config.DatabricksAWSAccountId),
					},
				},
				rayReadOnlyResourcePolicyStatement,
				rayWriteDeleteResourcePolicyStatement,
				predictiveMaintenanceWriteDeletePolicyStatement,
				predictiveMaintenanceShadowWriteDeletePolicyStatement,
			},
		},
	}

	// Policy that gives clusters write access to all DBs in playground Bucket
	// TODO: Refactor to be able to give access to DBs in finer granularity
	glueBase := fmt.Sprintf(
		"arn:aws:glue:%s:%s",
		genericresource.DataResourceId("aws_region", "current").ReferenceAttr("name"),
		genericresource.DataResourceId("aws_caller_identity", "current").ReferenceAttr("account_id"),
	)
	glueBaseArn := fmt.Sprintf("%s:catalog", glueBase)
	arns := []string{glueBaseArn}
	for _, db := range databaseregistries.GetDatabasesByGroup(config, databaseregistries.PlaygroundDatabaseGroup_LEGACY, databaseregistries.LegacyOnlyDatabases) {
		arns = append(arns,
			fmt.Sprintf("%s:database/%s", glueBase, db.Name),
			fmt.Sprintf("%s:table/%s/*", glueBase, db.Name),
		)
	}
	playgroundBucketDatabasesPolicy := &awsresource.IAMPolicy{
		ResourceName: "glue_playground_bucket_databases_write",
		Name:         "glue-playground-bucket-databases-write",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect:   "Allow",
					Resource: arns,
					Action: []string{
						"glue:Get*",
						"glue:UpdateDatabase",
						"glue:CreateTable",
						"glue:UpdateTable",
						"glue:DeleteTable",
						"glue:BatchCreatePartition",
						"glue:BatchDeletePartition",
						"glue:BatchGetPartition",
						"glue:CreatePartition",
						"glue:DeletePartition",
						"glue:UpdatePartition",
					},
				},
			},
		},
	}

	// biztechenterprisedataAndBiztechProdGlueDatabaseReadWritePolicy is intended for use by
	// biztechenterprisedata-cluster and biztech-prod-cluster IAM roles. We are migrating shared
	// inline policies to managed policies to get back under the 10240 aggregate byte limit for
	// inline policies.
	biztechGlueDatabaseReadWriteARNs, err := getSharedBiztechGlueARNs(config, glueBaseArn, glueBase)
	if err != nil {
		return nil, oops.Wrapf(err, "Error getting arns for biztech-glue-database-read-write managed policy")
	}
	glueReadActions := []string{
		"glue:GetDatabase",
		"glue:GetTable",
		"glue:BatchGetPartition",
	}
	biztechenterprisedataAndBiztechProdGlueDatabaseReadWritePolicy := &awsresource.IAMPolicy{
		ResourceName: "biztech_glue_database_read_write",
		Name:         "biztech-glue-database-read-write",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect:   "Allow",
					Resource: biztechGlueDatabaseReadWriteARNs,
					Action: append([]string{
						// Glue Write Actions
						"glue:UpdateDatabase",
						"glue:CreateTable",
						"glue:UpdateTable",
						"glue:DeleteTable",
						"glue:BatchCreatePartition",
						"glue:BatchDeletePartition",
						"glue:CreatePartition",
						"glue:DeletePartition",
						"glue:UpdatePartition",
					}, glueReadActions...),
				},
			},
		},
	}

	// Give read privileges for all other dev DBs.
	readOnlyDevDbs := append([]string{glueBaseArn}, getReadOnlyDevDbNames(config)...)

	var readOnlyDevDb []string
	for _, db := range readOnlyDevDbs {
		readOnlyDevDb = append(readOnlyDevDb,
			fmt.Sprintf("%s:database/%s", glueBase, db),
			fmt.Sprintf("%s:table/%s/*", glueBase, db),
		)
	}

	devTeamGlueDatabaseReadPolicy := &awsresource.IAMPolicy{
		ResourceName: "dev_team_glue_database_read",
		Name:         "dev-team-glue-database-read",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect:   "Allow",
					Resource: readOnlyDevDb,
					Action:   glueReadActions,
				},
			},
		},
	}

	resources := []tf.Resource{readOnlyPolicy,
		adminPolicy,
		glueResourcePolicy,
		playgroundBucketDatabasesPolicy,
		biztechenterprisedataAndBiztechProdGlueDatabaseReadWritePolicy,
		devTeamGlueDatabaseReadPolicy,
	}

	/*
		if config.Region == infraconsts.SamsaraAWSCARegion {

			type kmsArgs struct {
				KeyId string `hcl:"key_id"`
			}

			// Use AWS Managed KMS key.
			kmsKeyDataSource := &genericresource.Data{
				Type: awsresource.KMSKeyResourceType,
				Name: "aws_managed_glue_kms_key",
				Args: kmsArgs{KeyId: "alias/aws/glue"},
			}

			resources = append(resources, kmsKeyDataSource)

			glueDataCatalogEncryptionSettings := &awsresource.GlueDataCatalogEncryptionSettings{
				Name: "aws_glue_data_catalog_encryption_settings",
				DataCatalogEncryptionSettings: awsresource.DataCatalogEncryptionSettings{
					ConnectionPasswordEncryption: awsresource.ConnectionPasswordEncryption{
						ReturnConnectionPasswordEncrypted: true,
						AwsKmsKeyId:                       kmsKeyDataSource.ResourceId().ReferenceAttr("arn"),
					},
					EncryptionAtRest: awsresource.EncryptionAtRest{
						CatalogEncryptionMode: awsresource.CatalogEncryptionModeTypeSseKms,
						SseAwsKmsKeyId:        kmsKeyDataSource.ResourceId().ReferenceAttr("arn"),
					},
				},
			}

			resources = append(resources, glueDataCatalogEncryptionSettings)

		}
	*/
	return map[string][]tf.Resource{
		"glue": resources,
	}, nil
}

func MlFeaturePlatform(config dataplatformconfig.DatabricksConfig) (map[string][]tf.Resource, error) {
	mlFeaturePlatformPolicy := &awsresource.IAMPolicy{
		ResourceName: "ml_feature_platform_policy",
		Name:         "ml-feature-platform-policy",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Action: []string{"sts:AssumeRole"},
					Resource: []string{
						fmt.Sprintf("arn:aws:iam::%d:role/ml-feature-platform-role", infraconsts.GetAccountIdForRegion(config.Region)),
					},
				},
			},
		},
	}

	return map[string][]tf.Resource{
		"ml_feature_platform": {mlFeaturePlatformPolicy},
	}, nil
}

func MLCodeArtifactReadPolicy() (map[string][]tf.Resource, error) {
	mlCodeArtifactReadPolicy := &awsresource.IAMPolicy{
		ResourceName: "ml_code_artifact_read_policy",
		Name:         "ml-code-artifact-read-policy",
		Policy: policy.AWSPolicy{
			Version:   policy.AWSPolicyVersion,
			Statement: codeartifact.DojoCodeArtifactReadPolicies(),
		},
	}

	return map[string][]tf.Resource{
		"ml_code_artifact_read": {mlCodeArtifactReadPolicy},
	}, nil
}

func getReadOnlyDevDbNames(config dataplatformconfig.DatabricksConfig) []string {
	dbs := []string{
		"s3inventories_delta",
	}
	for _, db := range databaseregistries.GetDatabasesByGroup(config, databaseregistries.TeamDevDatabaseGroup, databaseregistries.LegacyOnlyDatabases) {
		// We don't create _dev DB for teams that are not managed within the backend repository.
		if db.OwnerTeam.TeamName != "" {
			dbs = append(dbs, strings.ToLower(db.OwnerTeam.TeamName)+"_dev")
		}
	}
	return dbs
}

// DataPipelinesReplicateGlueResources creates the databases + tables in Glue that will point
// at the data that is being sent to the US buckets that hold data pipeline outputs from other regions.
// If a data pipeline node has its output synced from EU to US (via the `replicate_to_us_from_regions` config),
// we want that node's output that is replicated to the US to be accessible via a `database`.`table` in Databricks.
func DataPipelinesReplicateGlueResources(c dataplatformconfig.DatabricksConfig) (map[string][]tf.Resource, error) {
	var databaseResources []tf.Resource
	var tableResources []tf.Resource
	for _, region := range dataplatformconfig.AllRegions {
		if region == infraconsts.SamsaraAWSDefaultRegion {
			continue
		}

		nodesToReplicate, err := getNodesToReplicateFromRegionToUs(region)
		if err != nil {
			return nil, oops.Wrapf(err, "error getting data pipeline nodes to replicate to us from region %s", region)
		}

		dbNamesSet := make(map[string]struct{})
		for _, node := range nodesToReplicate {

			dbName := fmt.Sprintf("%s_from_%s", node.Output().DAGName(), strings.Replace(region, "-", "_", -1))
			table, ok := node.Output().(*outputtypes.TableOutput)
			if !ok {
				continue
			}
			tableName := table.TableName
			bucketName := fmt.Sprintf("%s%s", awsregionconsts.RegionPrefix[infraconsts.SamsaraAWSDefaultRegion], GetDataPipelineReplicatedBucketNameForRegion(region))
			if _, ok := dbNamesSet[dbName]; !ok {
				// Create database resource
				databaseResources = append(databaseResources, &awsresource.GlueCatalogDatabase{
					Name:        dbName,
					LocationUri: fmt.Sprintf(`s3://%s/%s`, bucketName, node.Output().DAGName()),
				})
				dbNamesSet[dbName] = struct{}{}
			}

			// Create table resource
			tableResources = append(tableResources, &awsresource.GlueCatalogTable{
				Name:         tableName,
				DatabaseName: dbName,
				TableType:    "EXTERNAL_TABLE",
				Parameters: map[string]string{
					"EXTERNAL":                   "TRUE",
					"spark.sql.sources.provider": "delta",
				},
				StorageDescriptor: awsresource.GlueCatalogTableStorageDescriptor{
					Location:     "placeholder",
					InputFormat:  "org.apache.hadoop.mapred.SequenceFileInputFormat",
					OutputFormat: "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
					SerDeInfo: awsresource.GlueCatalogTableStorageDescriptorSerDeInfo{
						Name:                 "Parquet",
						SerializationLibrary: "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
						Parameters: map[string]string{
							"path":                              fmt.Sprintf(`s3://%s/%s/%s`, bucketName, node.Output().DAGName(), tableName),
							"serialization.format":              "1",
							"spark.sql.sources.schema.numParts": "1",
						},
					},
				},
			})
		}
	}

	return map[string][]tf.Resource{
		"datapipeline_replicated_databases": databaseResources,
		"datapipeline_replicated_tables":    tableResources,
	}, nil
}

func Playground(c dataplatformconfig.DatabricksConfig) map[string][]tf.Resource {
	bucket := dataplatformresource.Bucket{
		Name:                             "databricks-playground",
		Region:                           c.Region,
		LoggingBucket:                    tf.LocalId("logging_bucket").Reference(),
		RnDCostAllocation:                1,
		NonCurrentExpirationDaysOverride: 1,
		EnableS3IntelligentTiering:       true,
	}
	bucketLocal := &genericresource.StringLocal{
		Name:  "playground_bucket",
		Value: fmt.Sprintf(`"%s"`, bucket.Bucket().ResourceId().ReferenceAttr("bucket")),
	}

	rayNodesReadOnlyPolicyStatements := s3emitters.RayCrossAccountReadOnlyBucketPolicyStatements(c.Cloud, bucket.Name)
	bucketPolicy := &awsresource.S3BucketPolicy{
		Bucket: bucket.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: append([]policy.AWSPolicyStatement{
				policy.AWSPolicyStatement{
					Sid: "Grant dev access",
					Principal: map[string]string{
						"AWS": fmt.Sprintf(
							"arn:aws:iam::%d:root",
							c.AWSAccountId,
						),
					},
					Effect: "Allow",
					Action: []string{"s3:*"},
					Resource: []string{
						bucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", bucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
				policy.AWSPolicyStatement{
					Sid: "Grant playground access",
					Principal: map[string]string{
						"AWS": fmt.Sprintf(
							"arn:aws:iam::%d:root",
							infraconsts.SamsaraAWSPlaygroundAccountID,
						),
					},
					Effect: "Allow",
					Action: []string{"s3:Get*", "s3:List*"},
					Resource: []string{
						bucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", bucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
				{
					Sid:       "RequireTLS",
					Effect:    "Deny",
					Principal: map[string]string{"AWS": "*"},
					Action:    []string{"s3:*"},
					Resource: []string{
						bucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", bucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
					Condition: &policy.AWSPolicyCondition{
						Bool: &policy.AWSPolicyConditionBool{SecureTransportEnabled: pointer.BoolPtr(false)},
					},
				},
			},
				rayNodesReadOnlyPolicyStatements...),
		},
	}
	return map[string][]tf.Resource{
		"playground": append(
			bucket.Resources(),
			bucketLocal,
			bucketPolicy,
		),
	}
}

func WarehouseBucket(c dataplatformconfig.DatabricksConfig) map[string][]tf.Resource {
	bucket := dataplatformresource.Bucket{
		Name:          "databricks-warehouse",
		Region:        c.Region,
		LoggingBucket: tf.LocalId("logging_bucket").Reference(),
		// This bucket manages databricks databases, mostly.
		// We don't really need to keep around noncurrent versions of objects.
		NonCurrentExpirationDaysOverride: 1,
		RnDCostAllocation:                1,
		EnableS3IntelligentTiering:       true,
	}
	rayNodesReadOnlyPolicyStatements := s3emitters.RayCrossAccountReadOnlyBucketPolicyStatements(c.Cloud, bucket.Name)
	rayNodesReadOnlyPolicyStatements = append(rayNodesReadOnlyPolicyStatements,
		policy.AWSPolicyStatement{
			Sid:       "RequireTLS",
			Effect:    "Deny",
			Principal: map[string]string{"AWS": "*"},
			Action:    []string{"s3:*"},
			Resource: []string{
				bucket.Bucket().ResourceId().ReferenceAttr("arn"),
				fmt.Sprintf("%s/*", bucket.Bucket().ResourceId().ReferenceAttr("arn")),
			},
			Condition: &policy.AWSPolicyCondition{
				Bool: &policy.AWSPolicyConditionBool{SecureTransportEnabled: pointer.BoolPtr(false)},
			},
		})
	rayNodesReadOnlyPolicyStatements = append(rayNodesReadOnlyPolicyStatements, policy.AWSPolicyStatement{
		Sid:    "AllowDagsterDevEUDatabricksRead",
		Effect: "Allow",
		Principal: map[string]string{
			"AWS": fmt.Sprintf(
				"arn:aws:iam::%d:role/ec2-instance/dataplatform-dagster-dev-cluster",
				infraconsts.SamsaraAWSEUDatabricksAccountID,
			),
		},
		Action: []string{"s3:Get*", "s3:List*"},
		Resource: []string{
			bucket.Bucket().ResourceId().ReferenceAttr("arn"),
			fmt.Sprintf("%s/*", bucket.Bucket().ResourceId().ReferenceAttr("arn")),
		},
	})
	rayNodesReadOnlyPolicyStatements = append(rayNodesReadOnlyPolicyStatements, policy.AWSPolicyStatement{
		Sid:    "AllowDagsterProdEUDatabricksRead",
		Effect: "Allow",
		Principal: map[string]string{
			"AWS": fmt.Sprintf(
				"arn:aws:iam::%d:role/ec2-instance/dataplatform-dagster-prod-cluster",
				infraconsts.SamsaraAWSEUDatabricksAccountID,
			),
		},
		Action: []string{"s3:Get*", "s3:List*"},
		Resource: []string{
			bucket.Bucket().ResourceId().ReferenceAttr("arn"),
			fmt.Sprintf("%s/*", bucket.Bucket().ResourceId().ReferenceAttr("arn")),
		},
	})
	bucketLocal := &genericresource.StringLocal{
		Name:  "databricks_warehouse",
		Value: fmt.Sprintf(`"%s"`, bucket.Bucket().ResourceId().ReferenceAttr("bucket")),
	}
	bucketPolicy := &awsresource.S3BucketPolicy{
		Bucket: bucket.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version:   policy.AWSPolicyVersion,
			Statement: rayNodesReadOnlyPolicyStatements,
		},
	}
	return map[string][]tf.Resource{
		"warehouse": append(bucket.Resources(), bucketLocal, bucketPolicy),
	}
}

func DeployedArtifacts(c dataplatformconfig.DatabricksConfig) map[string][]tf.Resource {
	bucket := dataplatformresource.Bucket{
		Name:                             "dataplatform-deployed-artifacts",
		Region:                           c.Region,
		LoggingBucket:                    tf.LocalId("logging_bucket").Reference(),
		NonCurrentExpirationDaysOverride: 14,
		RnDCostAllocation:                1,
	}
	readPolicy := &awsresource.IAMPolicy{
		ResourceName: "read_dataplatform_deployed_artifacts",
		Name:         "read-dataplatform-deployed-artifacts",
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
						bucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", bucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
			},
		},
	}
	deployedArtifactsBucketPolicyStatements := s3emitters.DataplatformDeployedArtifactsBucketPolicyStatements(c.Region)
	deployedArtifactsBucketPolicyStatements = append(deployedArtifactsBucketPolicyStatements,
		policy.AWSPolicyStatement{
			Sid:       "RequireTLS",
			Effect:    "Deny",
			Principal: map[string]string{"AWS": "*"},
			Action:    []string{"s3:*"},
			Resource: []string{
				bucket.Bucket().ResourceId().ReferenceAttr("arn"),
				fmt.Sprintf("%s/*", bucket.Bucket().ResourceId().ReferenceAttr("arn")),
			},
			Condition: &policy.AWSPolicyCondition{
				Bool: &policy.AWSPolicyConditionBool{SecureTransportEnabled: pointer.BoolPtr(false)},
			},
		})
	writePolicy := &awsresource.S3BucketPolicy{
		Bucket: bucket.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version:   policy.AWSPolicyVersion,
			Statement: append(s3emitters.DataplatformDeployedArtifactsBucketPolicyStatements(c.Region), s3emitters.RayCrossAccountReadOnlyBucketPolicyStatements(c.Cloud, bucket.Name)...),
		},
	}

	return map[string][]tf.Resource{
		"deployed_artifacts": append(
			bucket.Resources(),
			readPolicy,
			writePolicy,
		),
	}
}

func DeployedArtifactObjects(c dataplatformconfig.DatabricksConfig) (map[string][]tf.Resource, error) {
	symLinkTeamWorkspaceInitScript, err := dataplatformresource.DeployedArtifactObjectNoHash(c.Region, "tools/jars/symlink-team-workspaces.sh")
	ctx := context.Background()
	if err != nil {
		slog.Errorw(ctx, oops.Wrapf(err, "failed to upload symlink team workspaces init script"), nil)
		return nil, oops.Wrapf(err, "failed to upload symlink team workspaces init script")
	}

	return map[string][]tf.Resource{
		"deployed_artifact_objects": {
			symLinkTeamWorkspaceInitScript,
		},
	}, nil
}

func JobBuckets(c dataplatformconfig.DatabricksConfig) (map[string][]tf.Resource, error) {
	ksDeltaLakeBucket := dataplatformresource.Bucket{
		Name:                             "kinesisstats-delta-lake",
		Region:                           c.Region,
		LoggingBucket:                    tf.LocalId("logging_bucket").Reference(),
		NonCurrentExpirationDaysOverride: 1,
		Metrics:                          true,
		RnDCostAllocation:                dataplatformconsts.KinesisstatsStorageRndAllocation,
		EnableS3IntelligentTiering:       true,
	}
	ksDeltaLakeBucketPolicy := &awsresource.S3BucketPolicy{
		Bucket: ksDeltaLakeBucket.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: append([]policy.AWSPolicyStatement{
				policy.AWSPolicyStatement{
					Principal: map[string]string{
						"AWS": fmt.Sprintf(
							"arn:aws:iam::%d:root",
							c.AWSAccountId,
						),
					},
					Effect: "Allow",
					Action: []string{"s3:Get*", "s3:List*"},
					Resource: []string{
						ksDeltaLakeBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", ksDeltaLakeBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
				{
					Sid: "AllowDatalakeDeleterCrossAccountAccess",
					Principal: map[string]string{
						"AWS": fmt.Sprintf(
							"arn:aws:iam::%d:role/datalakedeleter_service",
							infraconsts.GetAccountIdForRegion(c.Region),
						),
					},
					Effect: "Allow",
					Action: []string{"s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"},
					Resource: []string{
						ksDeltaLakeBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", ksDeltaLakeBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
				{
					Sid: "AllowDagsterDevUSDatabricksRead",
					Principal: map[string]string{
						"AWS": fmt.Sprintf(
							"arn:aws:iam::%d:role/ec2-instance/dataplatform-dagster-dev-cluster",
							infraconsts.SamsaraAWSDatabricksAccountID,
						),
					},
					Effect: "Allow",
					Action: []string{"s3:Get*", "s3:List*"},
					Resource: []string{
						ksDeltaLakeBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", ksDeltaLakeBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
				{
					Sid: "AllowDagsterProdUSDatabricksRead",
					Principal: map[string]string{
						"AWS": fmt.Sprintf(
							"arn:aws:iam::%d:role/ec2-instance/dataplatform-dagster-prod-cluster",
							infraconsts.SamsaraAWSDatabricksAccountID,
						),
					},
					Effect: "Allow",
					Action: []string{"s3:Get*", "s3:List*"},
					Resource: []string{
						ksDeltaLakeBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", ksDeltaLakeBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
				{
					Sid:       "RequireTLS",
					Effect:    "Deny",
					Principal: map[string]string{"AWS": "*"},
					Action:    []string{"s3:*"},
					Resource: []string{
						ksDeltaLakeBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", ksDeltaLakeBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
					Condition: &policy.AWSPolicyCondition{
						Bool: &policy.AWSPolicyConditionBool{SecureTransportEnabled: pointer.BoolPtr(false)},
					},
				},
			},

				s3emitters.RayCrossAccountReadOnlyBucketPolicyStatements(c.Cloud, "kinesisstats-delta-lake")...),
		},
	}

	reportStagingBucket := dataplatformresource.Bucket{
		Name:                             "report-staging-tables",
		Region:                           c.Region,
		LoggingBucket:                    tf.LocalId("logging_bucket").Reference(),
		NonCurrentExpirationDaysOverride: 1,
		EnableS3IntelligentTiering:       true,
		ExtraLifecycleRules: []awsresource.S3LifecycleRule{
			awsresource.S3LifecycleRule{
				ID:      "delete-temp",
				Enabled: true,
				Prefix:  "temp/",
				Expiration: awsresource.S3LifecycleRuleExpiration{
					Days: 2,
				},
			},
		},
		RnDCostAllocation: 0,
	}

	// Since we moved to AWS SSO roles, the team roles stopped to have S3 bucket policies since we have the limit of 10k
	// chars per role policy. This block is adding permissions for report-staging-tables bucket as S3 bucket policy allowing
	// access for the roles of teams based on the reports that they own.
	reports := sparkreportregistry.AllReportsInRegion(c.Region)
	var statements []policy.AWSPolicyStatement
	// Get all report teams and find the associated bucket keys arns.
	for _, report := range reports {
		teamName := report.TeamName
		// Allow reading report_aggregator files.
		arns := []string{
			fmt.Sprintf("arn:aws:s3:::%s/report_aggregator/%s/*", awsregionconsts.RegionPrefix[c.Region]+"report-staging-tables", report.Name),
			fmt.Sprintf("arn:aws:s3:::%s/report_aggregator/event_intervals_%s/*", awsregionconsts.RegionPrefix[c.Region]+"report-staging-tables", report.Name),
		}
		team, ok := team.TeamByName[teamName]
		if !ok {
			return nil, oops.Errorf("could not find team with name: %s", teamName)
		}
		teamWriterRoleName := ssooverrides.AdminSSOPermissionSetName(team, c.Cloud, false)

		statements = append(statements, policy.AWSPolicyStatement{
			Effect: "Allow",
			Principal: map[string]string{
				"AWS": fmt.Sprintf(
					"arn:aws:iam::%d:root",
					c.AWSAccountId,
				),
			},
			Action: []string{
				"s3:Get*",
				"s3:List*",
			},
			Resource: arns,
			Condition: &policy.AWSPolicyCondition{

				ForAnyValueStringLike: map[string][]string{
					"aws:PrincipalArn": []string{fmt.Sprintf("arn:aws:iam::%d:role/aws-reserved/sso.amazonaws.com/us-west-2/AWSReservedSSO_%s_*", c.AWSAccountId, teamWriterRoleName)},
				},
			},
		})

	}

	statements = append(statements, policy.AWSPolicyStatement{
		Principal: map[string]string{
			"AWS": fmt.Sprintf(
				"arn:aws:iam::%d:root",
				c.AWSAccountId,
			),
		},
		Effect: "Allow",
		Action: []string{"s3:*"},
		Resource: []string{
			reportStagingBucket.Bucket().ResourceId().ReferenceAttr("arn"),
			fmt.Sprintf("%s/*", reportStagingBucket.Bucket().ResourceId().ReferenceAttr("arn")),
		},
	})
	statements = append(statements, policy.AWSPolicyStatement{
		Sid:       "RequireTLS",
		Effect:    "Deny",
		Principal: map[string]string{"AWS": "*"},
		Action:    []string{"s3:*"},
		Resource: []string{
			reportStagingBucket.Bucket().ResourceId().ReferenceAttr("arn"),
			fmt.Sprintf("%s/*", reportStagingBucket.Bucket().ResourceId().ReferenceAttr("arn")),
		},
		Condition: &policy.AWSPolicyCondition{
			Bool: &policy.AWSPolicyConditionBool{SecureTransportEnabled: pointer.BoolPtr(false)},
		},
	})

	reportStagingBucketPolicy := &awsresource.S3BucketPolicy{
		Bucket: reportStagingBucket.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version:   policy.AWSPolicyVersion,
			Statement: statements,
		},
	}

	rdsDeltaLakeBucket := dataplatformresource.Bucket{
		Name:                             "rds-delta-lake",
		Region:                           c.Region,
		LoggingBucket:                    tf.LocalId("logging_bucket").Reference(),
		NonCurrentExpirationDaysOverride: 1,
		Metrics:                          true,
		RnDCostAllocation:                dataplatformconsts.RdsStorageRndAllocation,
		EnableS3IntelligentTiering:       true,
	}
	rayNodesReadOnlyPolicyStatements := s3emitters.RayCrossAccountReadOnlyBucketPolicyStatements(c.Cloud, rdsDeltaLakeBucket.Name)
	rdsDeltaLakeBucketPolicy := &awsresource.S3BucketPolicy{
		Bucket: rdsDeltaLakeBucket.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: append([]policy.AWSPolicyStatement{
				policy.AWSPolicyStatement{
					Principal: map[string]string{
						"AWS": fmt.Sprintf(
							"arn:aws:iam::%d:root",
							c.AWSAccountId,
						),
					},
					Effect: "Allow",
					Action: []string{"s3:Get*", "s3:List*"},
					Resource: []string{
						rdsDeltaLakeBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", rdsDeltaLakeBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
				{
					Sid:       "RequireTLS",
					Effect:    "Deny",
					Principal: map[string]string{"AWS": "*"},
					Action:    []string{"s3:*"},
					Resource: []string{
						rdsDeltaLakeBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", rdsDeltaLakeBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
					Condition: &policy.AWSPolicyCondition{
						Bool: &policy.AWSPolicyConditionBool{SecureTransportEnabled: pointer.BoolPtr(false)},
					},
				},
				{
					Sid: "AllowDagsterDevUSDatabricksRead",
					Principal: map[string]string{
						"AWS": fmt.Sprintf(
							"arn:aws:iam::%d:role/ec2-instance/dataplatform-dagster-dev-cluster",
							infraconsts.SamsaraAWSDatabricksAccountID,
						),
					},
					Effect: "Allow",
					Action: []string{"s3:Get*", "s3:List*"},
					Resource: []string{
						rdsDeltaLakeBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", rdsDeltaLakeBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
				{
					Sid: "AllowDagsterProdUSDatabricksRead",
					Principal: map[string]string{
						"AWS": fmt.Sprintf(
							"arn:aws:iam::%d:role/ec2-instance/dataplatform-dagster-prod-cluster",
							infraconsts.SamsaraAWSDatabricksAccountID,
						),
					},
					Effect: "Allow",
					Action: []string{"s3:Get*", "s3:List*"},
					Resource: []string{
						rdsDeltaLakeBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", rdsDeltaLakeBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
				{
					Sid:       "AllowCertificateArnUpdateFromDataPlatformDagsterDMSRoleMain",
					Effect:    "Allow",
					Principal: map[string]string{"AWS": fmt.Sprintf("arn:aws:iam::%d:root", c.AWSAccountId)},
					Action: []string{
						"s3:PutObject",
						"s3:PutObjectAcl",
					},
					Resource: []string{
						rdsDeltaLakeBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/ssl_certificate_arn/*", rdsDeltaLakeBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
			}, rayNodesReadOnlyPolicyStatements...),
		},
	}

	datapipelinesBucket := dataplatformresource.Bucket{
		Name:                             "data-pipelines-delta-lake",
		Region:                           c.Region,
		LoggingBucket:                    tf.LocalId("logging_bucket").Reference(),
		NonCurrentExpirationDaysOverride: 1,
		EnableS3IntelligentTiering:       true,
		ExtraLifecycleRules: []awsresource.S3LifecycleRule{
			awsresource.ExpireCurrentVersionPrefixLifecycleRule(7, "tmp/"),
		},
		RnDCostAllocation: dataplatformconsts.DatapipelinesStorageRndAllocation,
	}

	if c.Region != infraconsts.SamsaraAWSDefaultRegion {
		nodesToReplicateToUs, err := getNodesToReplicateFromRegionToUs(c.Region)
		if err != nil {
			return nil, oops.Wrapf(err, "error getting which nodes need to be replicated from %s to US", c.Region)
		}
		sort.Slice(nodesToReplicateToUs, func(i, j int) bool {
			return nodesToReplicateToUs[i].Name() < nodesToReplicateToUs[j].Name()
		})

		// Add the replication rule only when there are nodes to replicate.
		if len(nodesToReplicateToUs) > 0 {

			// Create a replication rule for every node that should be replicated. The rule specifies what prefix
			// to copy from. The prefix is the output destination where the node's data is.
			// AWS states that the maximum number of rules is 1,000 (https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketReplication.html)
			// If we ever have more than 1,000 nodes that want to be replicated cross region, we'll need to see if they can up the limit
			// or consider another solution.
			replicationRules := make([]awsresource.S3ReplicationRule, 0, len(nodesToReplicateToUs))
			for idx, node := range nodesToReplicateToUs {
				replicationRules = append(replicationRules, awsresource.S3ReplicationRule{
					ID:       fmt.Sprintf("data-pipelines-replication-%s", node.Name()),
					Status:   "Enabled",
					Priority: idx + 1,
					Filter: awsresource.S3ReplicationFilter{
						Prefix: node.Output().DestinationPrefix(),
					},
					Destination: awsresource.S3ReplicationDestination{
						Bucket:    fmt.Sprintf("arn:aws:s3:::%s", awsregionconsts.RegionPrefix[infraconsts.SamsaraAWSDefaultRegion]+GetDataPipelineReplicatedBucketNameForRegion(c.Region)),
						AccountID: fmt.Sprintf("%d", infraconsts.SamsaraAWSDatabricksAccountID),
						AccessControlTranslation: awsresource.S3AccessControlTranslation{
							Owner: "Destination",
						},
					},
				})
			}

			sort.Slice(replicationRules, func(i, j int) bool {
				return replicationRules[i].ID < replicationRules[j].ID
			})

			iamRoleName := fmt.Sprintf("data-pipelines-to-us-replicator-from-%s", c.Region)
			iamRoleArn := fmt.Sprintf("arn:aws:iam::%d:role/%s", c.DatabricksAWSAccountId, iamRoleName)
			datapipelinesBucket.ReplicationConfiguration = awsresource.S3ReplicationConfiguration{
				Role:  iamRoleArn,
				Rules: replicationRules,
			}
		}
	}

	datapipelinesBucketPolicy := &awsresource.S3BucketPolicy{
		Bucket: datapipelinesBucket.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: append([]policy.AWSPolicyStatement{
				{
					Principal: map[string]string{"AWS": fmt.Sprintf("arn:aws:iam::%d:root", c.AWSAccountId)},
					Effect:    "Allow",
					Action:    []string{"s3:Get*", "s3:List*"},
					Resource: []string{
						datapipelinesBucket.Bucket().ResourceId().ReferenceAttr("arn"),
					},
				},
				{
					Principal: map[string]string{"AWS": fmt.Sprintf("arn:aws:iam::%d:root", c.AWSAccountId)},
					Effect:    "Allow",
					Action:    []string{"s3:*"},
					Resource: []string{
						fmt.Sprintf("%s/*", datapipelinesBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
				{
					Sid: "AllowDagsterDevUSDatabricksRead",
					Principal: map[string]string{
						"AWS": fmt.Sprintf(
							"arn:aws:iam::%d:role/ec2-instance/dataplatform-dagster-dev-cluster",
							infraconsts.SamsaraAWSDatabricksAccountID,
						),
					},
					Effect: "Allow",
					Action: []string{"s3:Get*", "s3:List*"},
					Resource: []string{
						datapipelinesBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", datapipelinesBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
				{
					Sid: "AllowDagsterProdUSDatabricksRead",
					Principal: map[string]string{
						"AWS": fmt.Sprintf(
							"arn:aws:iam::%d:role/ec2-instance/dataplatform-dagster-prod-cluster",
							infraconsts.SamsaraAWSDatabricksAccountID,
						),
					},
					Effect: "Allow",
					Action: []string{"s3:Get*", "s3:List*"},
					Resource: []string{
						datapipelinesBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", datapipelinesBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
				{
					Sid:       "RequireTLS",
					Effect:    "Deny",
					Principal: map[string]string{"AWS": "*"},
					Action:    []string{"s3:*"},
					Resource: []string{
						datapipelinesBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", datapipelinesBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
					Condition: &policy.AWSPolicyCondition{
						Bool: &policy.AWSPolicyConditionBool{SecureTransportEnabled: pointer.BoolPtr(false)},
					},
				},
			},
				s3emitters.RayCrossAccountReadOnlyBucketPolicyStatements(c.Cloud, datapipelinesBucket.Name)...),
		},
	}

	dataStreamLakeBucket := dataplatformresource.Bucket{
		Name:                             "data-stream-lake",
		Region:                           c.Region,
		LoggingBucket:                    tf.LocalId("logging_bucket").Reference(),
		NonCurrentExpirationDaysOverride: 1,
		RnDCostAllocation:                dataplatformconsts.DatastreamsRawStorageRndAllocation,
		S3GlacierIRDays:                  7,
		CurrentExpirationDays:            90,
	}

	dataStreamLakeBucketPolicy := &awsresource.S3BucketPolicy{
		Bucket: dataStreamLakeBucket.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: append([]policy.AWSPolicyStatement{
				{
					Sid: "Grant Prod Account access to Data Stream Bucket so Firehose can access it.",
					Principal: map[string]string{
						"AWS": fmt.Sprintf("arn:aws:iam::%d:root", infraconsts.GetAccountIdForRegion(c.Region)),
					},
					Effect: "Allow",
					Action: []string{
						"s3:AbortMultipartUpload",
						"s3:GetBucketLocation",
						"s3:GetObject",
						"s3:ListBucket",
						"s3:ListBucketMultipartUploads",
					},
					Resource: []string{
						dataStreamLakeBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", dataStreamLakeBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
				{
					Sid: "Grant Prod Account access to Data Stream Bucket so Firehose can write to it.",
					Principal: map[string]string{
						"AWS": fmt.Sprintf("arn:aws:iam::%d:root", infraconsts.GetAccountIdForRegion(c.Region)),
					},
					Effect: "Allow",
					Action: []string{
						"s3:PutObject",
						"s3:PutObjectAcl",
					},
					Resource: []string{
						fmt.Sprintf("%s/*", dataStreamLakeBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{
							"s3:x-amz-acl": "bucket-owner-full-control",
						},
					},
				},
				{
					Sid: "Grant Dev Dagster access to data-stream-lake bucket",
					Principal: map[string]string{
						"AWS": fmt.Sprintf(
							"arn:aws:iam::%d:role/ec2-instance/dataplatform-dagster-dev-cluster",
							infraconsts.SamsaraAWSDatabricksAccountID,
						),
					},
					Effect: "Allow",
					Action: []string{"s3:Get*", "s3:List*"},
					Resource: []string{
						dataStreamLakeBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", dataStreamLakeBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
				{
					Sid: "Grant Prod Dagster access to data-stream-lake bucket",
					Principal: map[string]string{
						"AWS": fmt.Sprintf(
							"arn:aws:iam::%d:role/ec2-instance/dataplatform-dagster-prod-cluster",
							infraconsts.SamsaraAWSDatabricksAccountID,
						),
					},
					Effect: "Allow",
					Action: []string{"s3:Get*", "s3:List*"},
					Resource: []string{
						dataStreamLakeBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", dataStreamLakeBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
				{
					Sid:       "RequireTLS",
					Effect:    "Deny",
					Principal: map[string]string{"AWS": "*"},
					Action:    []string{"s3:*"},
					Resource: []string{
						dataStreamLakeBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", dataStreamLakeBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
					Condition: &policy.AWSPolicyCondition{
						Bool: &policy.AWSPolicyConditionBool{SecureTransportEnabled: pointer.BoolPtr(false)},
					},
				},
			},
				s3emitters.RayCrossAccountReadOnlyBucketPolicyStatements(c.Cloud, dataStreamLakeBucket.Name)...),
		},
	}

	s3BigStatsLakeBucket := dataplatformresource.Bucket{
		Name:                             "s3bigstats-delta-lake",
		Region:                           c.Region,
		LoggingBucket:                    tf.LocalId("logging_bucket").Reference(),
		NonCurrentExpirationDaysOverride: 1,
		Metrics:                          true,
		RnDCostAllocation:                dataplatformconsts.S3bigstatsStorageRndAllocation,
		EnableS3IntelligentTiering:       true,
	}
	s3BigStatsLakeBucketPolicyStatements := s3emitters.RayCrossAccountReadOnlyBucketPolicyStatements(
		c.Cloud,
		s3BigStatsLakeBucket.Name)
	s3BigStatsLakeBucketPolicyStatements = append(s3BigStatsLakeBucketPolicyStatements, policy.AWSPolicyStatement{
		Sid:       "RequireTLS",
		Effect:    "Deny",
		Principal: map[string]string{"AWS": "*"},
		Action:    []string{"s3:*"},
		Resource: []string{
			s3BigStatsLakeBucket.Bucket().ResourceId().ReferenceAttr("arn"),
			fmt.Sprintf("%s/*", s3BigStatsLakeBucket.Bucket().ResourceId().ReferenceAttr("arn")),
		},
		Condition: &policy.AWSPolicyCondition{
			Bool: &policy.AWSPolicyConditionBool{SecureTransportEnabled: pointer.BoolPtr(false)},
		},
	})
	s3BigStatsLakeBucketPolicyStatements = append(s3BigStatsLakeBucketPolicyStatements, policy.AWSPolicyStatement{
		Sid:    "AllowDagsterDevUSDatabricksRead",
		Effect: "Allow",
		Principal: map[string]string{
			"AWS": fmt.Sprintf(
				"arn:aws:iam::%d:role/ec2-instance/dataplatform-dagster-dev-cluster",
				infraconsts.SamsaraAWSDatabricksAccountID,
			),
		},
		Action: []string{"s3:Get*", "s3:List*"},
		Resource: []string{
			s3BigStatsLakeBucket.Bucket().ResourceId().ReferenceAttr("arn"),
			fmt.Sprintf("%s/*", s3BigStatsLakeBucket.Bucket().ResourceId().ReferenceAttr("arn")),
		},
	})
	s3BigStatsLakeBucketPolicyStatements = append(s3BigStatsLakeBucketPolicyStatements, policy.AWSPolicyStatement{
		Sid:    "AllowDagsterProdUSDatabricksRead",
		Effect: "Allow",
		Principal: map[string]string{
			"AWS": fmt.Sprintf(
				"arn:aws:iam::%d:role/ec2-instance/dataplatform-dagster-prod-cluster",
				infraconsts.SamsaraAWSDatabricksAccountID,
			),
		},
		Action: []string{"s3:Get*", "s3:List*"},
		Resource: []string{
			s3BigStatsLakeBucket.Bucket().ResourceId().ReferenceAttr("arn"),
			fmt.Sprintf("%s/*", s3BigStatsLakeBucket.Bucket().ResourceId().ReferenceAttr("arn")),
		},
	})
	s3BigStatsLakeBucketPolicy := &awsresource.S3BucketPolicy{
		Bucket: s3BigStatsLakeBucket.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version:   policy.AWSPolicyVersion,
			Statement: s3BigStatsLakeBucketPolicyStatements,
		},
	}

	dataStreamsDeltaLakeBucket := dataplatformresource.Bucket{
		Name:                             "data-streams-delta-lake",
		Region:                           c.Region,
		LoggingBucket:                    tf.LocalId("logging_bucket").Reference(),
		NonCurrentExpirationDaysOverride: 1,
		Metrics:                          true,
		RnDCostAllocation:                dataplatformconsts.DatastreamsDeltaStorageRndAllocation,
		EnableS3IntelligentTiering:       true,
	}
	dataStreamsDeltaLakeBucketPolicyStatements := s3emitters.RayCrossAccountReadOnlyBucketPolicyStatements(
		c.Cloud,
		dataStreamsDeltaLakeBucket.Name)
	dataStreamsDeltaLakeBucketPolicyStatements = append(dataStreamsDeltaLakeBucketPolicyStatements, []policy.AWSPolicyStatement{
		{Sid: "RequireTLS",
			Effect:    "Deny",
			Principal: map[string]string{"AWS": "*"},
			Action:    []string{"s3:*"},
			Resource: []string{
				dataStreamsDeltaLakeBucket.Bucket().ResourceId().ReferenceAttr("arn"),
				fmt.Sprintf("%s/*", dataStreamsDeltaLakeBucket.Bucket().ResourceId().ReferenceAttr("arn")),
			},
			Condition: &policy.AWSPolicyCondition{
				Bool: &policy.AWSPolicyConditionBool{SecureTransportEnabled: pointer.BoolPtr(false)},
			},
		},
		{
			Sid: "AllowDagsterDevUSDatabricksRead",
			Principal: map[string]string{
				"AWS": fmt.Sprintf(
					"arn:aws:iam::%d:role/ec2-instance/dataplatform-dagster-dev-cluster",
					infraconsts.SamsaraAWSDatabricksAccountID,
				),
			},
			Effect: "Allow",
			Action: []string{"s3:Get*", "s3:List*"},
			Resource: []string{
				dataStreamsDeltaLakeBucket.Bucket().ResourceId().ReferenceAttr("arn"),
				fmt.Sprintf("%s/*", dataStreamsDeltaLakeBucket.Bucket().ResourceId().ReferenceAttr("arn")),
			},
		},
		{
			Sid: "AllowDagsterProdUSDatabricksRead",
			Principal: map[string]string{
				"AWS": fmt.Sprintf(
					"arn:aws:iam::%d:role/ec2-instance/dataplatform-dagster-prod-cluster",
					infraconsts.SamsaraAWSDatabricksAccountID,
				),
			},
			Effect: "Allow",
			Action: []string{"s3:Get*", "s3:List*"},
			Resource: []string{
				dataStreamsDeltaLakeBucket.Bucket().ResourceId().ReferenceAttr("arn"),
				fmt.Sprintf("%s/*", dataStreamsDeltaLakeBucket.Bucket().ResourceId().ReferenceAttr("arn")),
			},
		},
	}...,
	)
	dataStreamsDeltaLakeBucketPolicy := &awsresource.S3BucketPolicy{
		Bucket: dataStreamsDeltaLakeBucket.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version:   policy.AWSPolicyVersion,
			Statement: dataStreamsDeltaLakeBucketPolicyStatements,
		},
	}

	datamodelBucket := dataplatformresource.Bucket{
		Name:                             "datamodel-warehouse",
		Region:                           c.Region,
		LoggingBucket:                    tf.LocalId("logging_bucket").Reference(),
		NonCurrentExpirationDaysOverride: 1,
		EnableS3IntelligentTiering:       true,
		ExtraLifecycleRules: []awsresource.S3LifecycleRule{
			awsresource.ExpireCurrentVersionPrefixLifecycleRule(7, "tmp/"),
		},
		RnDCostAllocation: 1,
	}
	datamodelBucketPolicyStatements := append(dagsterBucketPolicyStatements(c, datamodelBucket.Bucket().ResourceId().ReferenceAttr("arn")),
		s3emitters.RayCrossAccountReadOnlyBucketPolicyStatements(c.Cloud, datamodelBucket.Name)...)
	datamodelBucketPolicyStatements = append(datamodelBucketPolicyStatements, s3emitters.ProdRayCrossAccountReadOnlyBucketPolicyStatements(c.Cloud, datamodelBucket.Name)...)
	datamodelBucketPolicyStatements = append(datamodelBucketPolicyStatements, policy.AWSPolicyStatement{
		Sid:       "RequireTLS",
		Effect:    "Deny",
		Principal: map[string]string{"AWS": "*"},
		Action:    []string{"s3:*"},
		Resource: []string{
			datamodelBucket.Bucket().ResourceId().ReferenceAttr("arn"),
			fmt.Sprintf("%s/*", datamodelBucket.Bucket().ResourceId().ReferenceAttr("arn")),
		},
		Condition: &policy.AWSPolicyCondition{
			Bool: &policy.AWSPolicyConditionBool{SecureTransportEnabled: pointer.BoolPtr(false)},
		},
	})
	datamodelBucketPolicyStatements = append(datamodelBucketPolicyStatements, policy.AWSPolicyStatement{
		Sid:    "AllowDagsterDevEUDatabricksRead",
		Effect: "Allow",
		Principal: map[string]string{
			"AWS": fmt.Sprintf(
				"arn:aws:iam::%d:role/ec2-instance/dataplatform-dagster-dev-cluster",
				infraconsts.SamsaraAWSEUDatabricksAccountID,
			),
		},
		Action: []string{"s3:Get*", "s3:List*"},
		Resource: []string{
			datamodelBucket.Bucket().ResourceId().ReferenceAttr("arn"),
			fmt.Sprintf("%s/*", datamodelBucket.Bucket().ResourceId().ReferenceAttr("arn")),
		},
	})
	datamodelBucketPolicyStatements = append(datamodelBucketPolicyStatements, policy.AWSPolicyStatement{
		Sid:    "AllowDagsterProdEUDatabricksRead",
		Effect: "Allow",
		Principal: map[string]string{
			"AWS": fmt.Sprintf(
				"arn:aws:iam::%d:role/ec2-instance/dataplatform-dagster-prod-cluster",
				infraconsts.SamsaraAWSEUDatabricksAccountID,
			),
		},
		Action: []string{"s3:Get*", "s3:List*"},
		Resource: []string{
			datamodelBucket.Bucket().ResourceId().ReferenceAttr("arn"),
			fmt.Sprintf("%s/*", datamodelBucket.Bucket().ResourceId().ReferenceAttr("arn")),
		},
	})
	datamodelBucketPolicy := &awsresource.S3BucketPolicy{
		Bucket: datamodelBucket.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version:   policy.AWSPolicyVersion,
			Statement: datamodelBucketPolicyStatements,
		},
	}

	datamodelDevBucket := dataplatformresource.Bucket{
		Name:                             "datamodel-warehouse-dev",
		Region:                           c.Region,
		LoggingBucket:                    tf.LocalId("logging_bucket").Reference(),
		NonCurrentExpirationDaysOverride: 1,
		EnableS3IntelligentTiering:       true,
		ExtraLifecycleRules: []awsresource.S3LifecycleRule{
			awsresource.ExpireCurrentVersionPrefixLifecycleRule(7, "tmp/"),
		},
		RnDCostAllocation: 1,
	}
	datamodelDevBucketPolicyStatements := append(dagsterBucketPolicyStatements(c, datamodelDevBucket.Bucket().ResourceId().ReferenceAttr("arn")),
		s3emitters.RayCrossAccountReadOnlyBucketPolicyStatements(c.Cloud, datamodelDevBucket.Name)...)
	datamodelDevBucketPolicyStatements = append(datamodelDevBucketPolicyStatements, s3emitters.ProdRayCrossAccountReadOnlyBucketPolicyStatements(c.Cloud, datamodelDevBucket.Name)...)
	datamodelDevBucketPolicyStatements = append(datamodelDevBucketPolicyStatements, policy.AWSPolicyStatement{
		Sid:       "RequireTLS",
		Effect:    "Deny",
		Principal: map[string]string{"AWS": "*"},
		Action:    []string{"s3:*"},
		Resource: []string{
			datamodelDevBucket.Bucket().ResourceId().ReferenceAttr("arn"),
			fmt.Sprintf("%s/*", datamodelDevBucket.Bucket().ResourceId().ReferenceAttr("arn")),
		},
		Condition: &policy.AWSPolicyCondition{
			Bool: &policy.AWSPolicyConditionBool{SecureTransportEnabled: pointer.BoolPtr(false)},
		},
	})
	datamodelDevBucketPolicy := &awsresource.S3BucketPolicy{
		Bucket: datamodelDevBucket.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version:   policy.AWSPolicyVersion,
			Statement: datamodelDevBucketPolicyStatements,
		},
	}

	dynamodbDeltaBucket := dataplatformresource.Bucket{
		Name:                             "dynamodb-delta-lake",
		Region:                           c.Region,
		LoggingBucket:                    tf.LocalId("logging_bucket").Reference(),
		NonCurrentExpirationDaysOverride: 1,
		Metrics:                          true,
		RnDCostAllocation:                dataplatformconsts.DynamodbStorageRndAllocation,
		EnableS3IntelligentTiering:       true,
		PreventDestroy:                   true,
	}

	dynamodbDeltaLakeRayNodesReadOnlyPolicyStatements := s3emitters.RayCrossAccountReadOnlyBucketPolicyStatements(c.Cloud, dynamodbDeltaBucket.Name)
	dynamodbDeltaBucketPolicy := &awsresource.S3BucketPolicy{
		Bucket: dynamodbDeltaBucket.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: append([]policy.AWSPolicyStatement{
				policy.AWSPolicyStatement{
					Principal: map[string]string{
						"AWS": fmt.Sprintf(
							"arn:aws:iam::%d:root",
							c.AWSAccountId,
						),
					},
					Effect: "Allow",
					Action: []string{"s3:Get*", "s3:List*"},
					Resource: []string{
						dynamodbDeltaBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", dynamodbDeltaBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
				{
					Sid:       "RequireTLS",
					Effect:    "Deny",
					Principal: map[string]string{"AWS": "*"},
					Action:    []string{"s3:*"},
					Resource: []string{
						dynamodbDeltaBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", dynamodbDeltaBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
					Condition: &policy.AWSPolicyCondition{
						Bool: &policy.AWSPolicyConditionBool{SecureTransportEnabled: pointer.BoolPtr(false)},
					},
				},
				{
					Sid: "AllowDagsterDevUSDatabricksRead",
					Principal: map[string]string{
						"AWS": fmt.Sprintf(
							"arn:aws:iam::%d:role/ec2-instance/dataplatform-dagster-dev-cluster",
							infraconsts.SamsaraAWSDatabricksAccountID,
						),
					},
					Effect: "Allow",
					Action: []string{"s3:Get*", "s3:List*"},
					Resource: []string{
						dynamodbDeltaBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", dynamodbDeltaBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
				{
					Sid: "AllowDagsterProdUSDatabricksRead",
					Principal: map[string]string{
						"AWS": fmt.Sprintf(
							"arn:aws:iam::%d:role/ec2-instance/dataplatform-dagster-prod-cluster",
							infraconsts.SamsaraAWSDatabricksAccountID,
						),
					},
					Effect: "Allow",
					Action: []string{"s3:Get*", "s3:List*"},
					Resource: []string{
						dynamodbDeltaBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", dynamodbDeltaBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
			}, dynamodbDeltaLakeRayNodesReadOnlyPolicyStatements...),
		},
	}

	return map[string][]tf.Resource{
		"ks_bucket": append(
			ksDeltaLakeBucket.Resources(),
			ksDeltaLakeBucketPolicy,
		),
		"rds_bucket": append(
			rdsDeltaLakeBucket.Resources(),
			rdsDeltaLakeBucketPolicy,
		),
		"report_bucket": append(
			reportStagingBucket.Resources(),
			reportStagingBucketPolicy,
		),
		"datapipelines_bucket": append(
			datapipelinesBucket.Resources(),
			datapipelinesBucketPolicy,
		),
		"datastream_bucket": append(
			dataStreamLakeBucket.Resources(),
			dataStreamLakeBucketPolicy,
		),
		"datastreams_delta_bucket": append(
			dataStreamsDeltaLakeBucket.Resources(),
			dataStreamsDeltaLakeBucketPolicy,
		),
		"s3bigstats_bucket": append(
			s3BigStatsLakeBucket.Resources(),
			s3BigStatsLakeBucketPolicy,
		),
		"datamodel_warehouse_bucket": append(
			datamodelBucket.Resources(),
			datamodelBucketPolicy,
		),
		"datamodel_warehouse_dev_bucket": append(
			datamodelDevBucket.Resources(),
			datamodelDevBucketPolicy,
		),
		"dynamodb_delta_bucket": append(
			dynamodbDeltaBucket.Resources(),
			dynamodbDeltaBucketPolicy,
		),
	}, nil
}

func dagsterBucketPolicyStatements(c dataplatformconfig.DatabricksConfig, bucketArn string) []policy.AWSPolicyStatement {
	statements := []policy.AWSPolicyStatement{
		{
			Principal: map[string]string{"AWS": fmt.Sprintf("arn:aws:iam::%d:root", c.AWSAccountId)},
			Effect:    "Allow",
			Action:    []string{"s3:Get*", "s3:List*"},
			Resource: []string{
				bucketArn,
			},
		},
		{
			Principal: map[string]string{"AWS": fmt.Sprintf("arn:aws:iam::%d:root", c.AWSAccountId)},
			Effect:    "Allow",
			Action:    []string{"s3:*"},
			Resource: []string{
				fmt.Sprintf("%s/*", bucketArn),
			},
		},
	}

	// Grant cross-account read-only access on data model s3 buckets in non-US Databricks AWS accounts (EU, CA)
	// to Dagster iam roles and datahub-eks-nodes in the US Databricks AWS account.
	// This enables Dagster runs and DataHub services launched in the US region to read data in other regions.
	if c.Region == infraconsts.SamsaraAWSEURegion || c.Region == infraconsts.SamsaraAWSCARegion {
		statements = append(statements, []policy.AWSPolicyStatement{
			{
				Principal: map[string]string{"AWS": fmt.Sprintf("arn:aws:iam::%d:role/ec2-instance/dataplatform-dagster-prod-cluster", infraconsts.SamsaraAWSDatabricksAccountID)},
				Effect:    "Allow",
				Action:    []string{"s3:Get*", "s3:List*"},
				Resource: []string{
					bucketArn,
					fmt.Sprintf("%s/*", bucketArn),
				},
			},
			{
				Principal: map[string]string{"AWS": fmt.Sprintf("arn:aws:iam::%d:role/ec2-instance/dataplatform-dagster-dev-cluster", infraconsts.SamsaraAWSDatabricksAccountID)},
				Effect:    "Allow",
				Action:    []string{"s3:Get*", "s3:List*"},
				Resource: []string{
					bucketArn,
					fmt.Sprintf("%s/*", bucketArn),
				},
			},
			{
				Principal: map[string]string{"AWS": fmt.Sprintf("arn:aws:iam::%d:role/datahub-eks-nodes", infraconsts.SamsaraAWSDatabricksAccountID)},
				Effect:    "Allow",
				Action:    []string{"s3:Get*", "s3:List*"},
				Resource: []string{
					bucketArn,
					fmt.Sprintf("%s/*", bucketArn),
				},
			},
		}...)
	}

	return statements
}

// GetDataPipelineReplicatedBucketNameForRegion returns the ARN for the bucket that exists in the US
// that will contain data replicated from a certain region.
func GetDataPipelineReplicatedBucketNameForRegion(region string) string {
	return fmt.Sprintf("data-pipelines-delta-lake-from-%s", region)
}

// DataPipelinesReplicatedBuckets creates S3 buckets in the US that will hold data from data pipeline nodes
// from all the regions that we want to replicate data from over to this bucket in the US.
// So for example for the EU, we create a bucket in the US that will hold the data we replicate from EU to US.
func DataPipelinesReplicatedBuckets(c dataplatformconfig.DatabricksConfig) map[string][]tf.Resource {
	tfResources := make(map[string][]tf.Resource)
	for _, regionToReplicateFrom := range dataplatformconfig.AllRegions {
		if regionToReplicateFrom == infraconsts.SamsaraAWSDefaultRegion {
			// We don't need to replicate from the US -> US
			continue
		}
		bucketName := GetDataPipelineReplicatedBucketNameForRegion(regionToReplicateFrom)
		datapipelinesReplicatedDataBucket := dataplatformresource.Bucket{
			Name:                             bucketName,
			Region:                           c.Region,
			LoggingBucket:                    tf.LocalId("logging_bucket").Reference(),
			NonCurrentExpirationDaysOverride: 1,
			RnDCostAllocation:                0,
			EnableS3IntelligentTiering:       true,
		}

		// A policy which allows the source region to replicate to this bucket in the US.
		// This is needed in addition to the IAM role policy in the source region
		datapipelinesReplicatedDataBucketPolicy := &awsresource.S3BucketPolicy{
			Bucket: datapipelinesReplicatedDataBucket.Bucket().ResourceId(),
			Policy: policy.AWSPolicy{
				Version: policy.AWSPolicyVersion,
				Statement: []policy.AWSPolicyStatement{
					{
						Principal: map[string]string{"AWS": fmt.Sprintf("arn:aws:iam::%d:root", infraconsts.GetDatabricksAccountIdForRegion(regionToReplicateFrom))},
						Effect:    "Allow",
						Action: []string{
							"s3:GetBucketVersioning",
							"s3:PutBucketVersioning",
							"s3:ReplicateObject",
							"s3:ReplicateDelete",
							"s3:ObjectOwnerOverrideToBucketOwner",
						},
						Resource: []string{
							fmt.Sprintf("arn:aws:s3:::%s", awsregionconsts.RegionPrefix[c.Region]+bucketName),
							fmt.Sprintf("arn:aws:s3:::%s/*", awsregionconsts.RegionPrefix[c.Region]+bucketName),
						},
					},
					{
						Sid:       "RequireTLS",
						Effect:    "Deny",
						Principal: map[string]string{"AWS": "*"},
						Action:    []string{"s3:*"},
						Resource: []string{
							datapipelinesReplicatedDataBucket.Bucket().ResourceId().ReferenceAttr("arn"),
							fmt.Sprintf("%s/*", datapipelinesReplicatedDataBucket.Bucket().ResourceId().ReferenceAttr("arn")),
						},
						Condition: &policy.AWSPolicyCondition{
							Bool: &policy.AWSPolicyConditionBool{SecureTransportEnabled: pointer.BoolPtr(false)},
						},
					},
				},
			},
		}

		tfResources[fmt.Sprintf("datapipelines_replicated_data_bucket_from_%s", regionToReplicateFrom)] = append(datapipelinesReplicatedDataBucket.Resources(), datapipelinesReplicatedDataBucketPolicy)
	}

	return tfResources
}

// DataPipelinesReplicationIAMRoles creates IAM roles that allow S3 to replicate data pipeline node outputs from the EU over to the US.
func DataPipelinesReplicationIAMRoles(c dataplatformconfig.DatabricksConfig) map[string][]tf.Resource {
	tfResources := make(map[string][]tf.Resource)
	maxSessionDuration := 12 * 60 * 60

	var resourcesForRegion []tf.Resource
	datapipelinesToUSReplicatorRole := &awsresource.IAMRole{
		Name: fmt.Sprintf("data-pipelines-to-us-replicator-from-%s", c.Region),
		AssumeRolePolicy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Action: []string{"sts:AssumeRole"},
					Principal: map[string]string{
						"Service": "s3.amazonaws.com",
					},
					Effect: "Allow",
				},
			},
		},
		MaxSessionDuration: maxSessionDuration,
		Tags: map[string]string{
			"samsara:service":       fmt.Sprintf("data-pipelines-to-us-replicator-from-%s", c.Region),
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}
	resourcesForRegion = append(resourcesForRegion, datapipelinesToUSReplicatorRole)

	datapipelinesToUSReplicatorPolicy := &awsresource.IAMRolePolicy{
		Name: fmt.Sprintf("replicate-data-pipelines-output-to-us-from-%s", c.Region),
		Role: datapipelinesToUSReplicatorRole.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Action: []string{
						"s3:Get*",
					},
					Resource: []string{fmt.Sprintf("arn:aws:s3:::%s/*", awsregionconsts.RegionPrefix[c.Region]+"data-pipelines-delta-lake")},
				},
				{
					Effect: "Allow",
					Action: []string{
						"s3:GetReplicationConfiguration",
						"s3:ListBucket",
					},
					Resource: []string{fmt.Sprintf("arn:aws:s3:::%s", awsregionconsts.RegionPrefix[c.Region]+"data-pipelines-delta-lake")},
				},
				{
					Effect: "Allow",
					Action: []string{
						"s3:ReplicateObject",
						"s3:ReplicateDelete",
						"s3:ReplicateTags",
						"s3:GetObjectVersionTagging",
						"s3:ObjectOwnerOverrideToBucketOwner",
					},
					Resource: []string{fmt.Sprintf("arn:aws:s3:::%s/*", awsregionconsts.RegionPrefix[infraconsts.SamsaraAWSDefaultRegion]+GetDataPipelineReplicatedBucketNameForRegion(c.Region))},
				},
			},
		},
	}
	resourcesForRegion = append(resourcesForRegion, datapipelinesToUSReplicatorPolicy)

	tfResources[fmt.Sprintf("datapipelines_replication_iam_roles_for_%s", c.Region)] = resourcesForRegion

	return tfResources
}

// getNodesToReplicateFromRegionToUs returns the datapipeline nodes that we want to replicate
// the output from a given region over to the US. This is based on the `replicate_to_us_from_regions` configuration field.
func getNodesToReplicateFromRegionToUs(sourceRegion string) ([]nodetypes.Node, error) {
	dags, err := graphs.BuildDAGs()
	if err != nil {
		return nil, oops.Wrapf(err, "error building dags")
	}

	var nodesToReplicate []nodetypes.Node
	seenNodes := make(map[nodetypes.Node]struct{})
	for _, dag := range dags {
		for _, node := range dag.GetNodes() {
			for _, region := range node.RegionsToReplicateToUs() {
				if region == sourceRegion {
					if _, ok := seenNodes[node]; !ok {
						nodesToReplicate = append(nodesToReplicate, node)
						seenNodes[node] = struct{}{}
					}
				}
			}
		}
	}

	// Sort so that terraform output is deterministic
	sort.Slice(nodesToReplicate, func(i, j int) bool {
		return nodesToReplicate[i].Name() < nodesToReplicate[i].Name()
	})

	return nodesToReplicate, nil
}

func Datadog(c dataplatformconfig.DatabricksConfig) map[string][]tf.Resource {
	datadogAWSIntegrationIAMPolicy := &awsresource.IAMPolicy{
		Name: "datadog_aws_integration",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Action: []string{
						"apigateway:GET",
						"autoscaling:Describe*",
						"backup:List*",
						"budgets:ViewBudget",
						"cloudfront:GetDistributionConfig",
						"cloudfront:ListDistributions",
						"cloudtrail:DescribeTrails",
						"cloudtrail:GetTrailStatus",
						"cloudtrail:LookupEvents",
						"cloudwatch:Describe*",
						"cloudwatch:Get*",
						"cloudwatch:List*",
						"codedeploy:List*",
						"codedeploy:BatchGet*",
						"directconnect:Describe*",
						"dynamodb:List*",
						"dynamodb:Describe*",
						"ec2:Describe*",
						"ecs:Describe*",
						"ecs:List*",
						"elasticache:Describe*",
						"elasticache:List*",
						"elasticfilesystem:DescribeFileSystems",
						"elasticfilesystem:DescribeTags",
						"elasticfilesystem:DescribeAccessPoints",
						"elasticloadbalancing:Describe*",
						"elasticmapreduce:List*",
						"elasticmapreduce:Describe*",
						"es:ListTags",
						"es:ListDomainNames",
						"es:DescribeElasticsearchDomains",
						"events:CreateEventBus",
						"fsx:DescribeFileSystems",
						"fsx:ListTagsForResource",
						"health:DescribeEvents",
						"health:DescribeEventDetails",
						"health:DescribeAffectedEntities",
						"kinesis:List*",
						"kinesis:Describe*",
						"lambda:GetPolicy",
						"lambda:List*",
						"logs:DeleteSubscriptionFilter",
						"logs:DescribeLogGroups",
						"logs:DescribeLogStreams",
						"logs:DescribeSubscriptionFilters",
						"logs:FilterLogEvents",
						"logs:PutSubscriptionFilter",
						"logs:TestMetricFilter",
						"organizations:Describe*",
						"organizations:List*",
						"rds:Describe*",
						"rds:List*",
						"redshift:DescribeClusters",
						"redshift:DescribeLoggingStatus",
						"route53:List*",
						"s3:GetBucketLogging",
						"s3:GetBucketLocation",
						"s3:GetBucketNotification",
						"s3:GetBucketTagging",
						"s3:ListAllMyBuckets",
						"s3:PutBucketNotification",
						"ses:Get*",
						"sns:List*",
						"sns:Publish",
						"sqs:ListQueues",
						"states:ListStateMachines",
						"states:DescribeStateMachine",
						"support:DescribeTrustedAdvisor*",
						"support:RefreshTrustedAdvisorCheck",
						"tag:GetResources",
						"tag:GetTagKeys",
						"tag:GetTagValues",
						"xray:BatchGetTraces",
						"xray:GetTraceSummaries",
					},
					Resource: []string{"*"},
				},
			},
		},
	}

	datadogAWSIAMRole := &awsresource.IAMRole{
		Name:        fmt.Sprintf("datadog_aws_integration-%s", c.Region),
		Description: "Role for Datadog AWS Integration",
		AssumeRolePolicy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Action: []string{"sts:AssumeRole"},
					Principal: map[string]string{
						"AWS": "arn:aws:iam::464622532012:root",
					},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{
							"sts:ExternalId": infraconsts.GetDatadogExternalIdForDatabricksAccountRegion(c.Region),
						},
					},
				},
			},
		},
		Tags: map[string]string{
			"samsara:service":       fmt.Sprintf("datadog_aws_integration-%s", c.Region),
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}

	datadogAWSIAMRolePolicyAttachment := &awsresource.IAMRolePolicyAttachment{
		Name:      "datadog_aws_integration",
		Role:      datadogAWSIAMRole.ResourceId().ReferenceAttr("name"),
		PolicyARN: datadogAWSIntegrationIAMPolicy.ResourceId().ReferenceAttr("arn"),
	}

	return map[string][]tf.Resource{
		"datadog_aws_integration": {
			datadogAWSIntegrationIAMPolicy,
			datadogAWSIAMRole,
			datadogAWSIAMRolePolicyAttachment,
		},
	}
}

func SES(c dataplatformconfig.DatabricksConfig) map[string][]tf.Resource {
	// SES outbound sandbox restriction is lifted via manual support tickets.
	// AWS Account  | Support ticket ID
	// 492164655156 | 7162478401
	//
	// SES email identity verification is done manually:
	// 1. Domain needs to be verified first, it needs this PR:https://github.com/samsara-dev/backend/pull/179408 to ship & apply first.
	// 2. If the domain is still unverified, delete domain from SES and CNAME entries in route53 of same account from console and recreate it manually.
	// 3. Find no-reply@ email address on SES console.
	// 4. Click on "resend" verification email.
	// 5. Find the email in "databricks-alerts-email" S3 bucket and click the link inside.
	domain := dataplatformresource.DatabricksAlertsDomain(c.Region)
	email := dataplatformresource.DatabricksAlertsSender(c.Region)
	zone := &awsresource.Route53Zone{
		Name: domain,
		Tags: map[string]string{
			"samsara:service":       "databricks-alerts",
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}
	domainIdentity := &awsresource.SESDomainIdentity{
		Domain: domain,
	}
	emailIdentity := &awsresource.SESEmailIdentity{
		Email: email,
	}
	sesDkim := &awsresource.SESDomainDKIM{
		Domain: domain,
	}
	token := &awsresource.Route53Record{
		ResourceName: "ses_verification_token",
		ZoneId:       zone.ResourceId().ReferenceAttr("zone_id"),
		RecordName:   fmt.Sprintf("_amazonses.%s", domain),
		Type:         "TXT",
		TTL:          "300",
		Records: []string{
			domainIdentity.ResourceId().ReferenceAttr("verification_token"),
		},
	}
	mx := &awsresource.Route53Record{
		ResourceName: "mx",
		ZoneId:       zone.ResourceId().ReferenceAttr("zone_id"),
		RecordName:   domain,
		Type:         "MX",
		TTL:          "300",
		Records: []string{
			fmt.Sprintf("10 inbound-smtp.%s.amazonaws.com", c.Region),
		},
	}
	spf := &awsresource.Route53Record{
		ResourceName: "spf",
		ZoneId:       zone.ResourceId().ReferenceAttr("zone_id"),
		RecordName:   domain,
		Type:         "TXT",
		TTL:          "300",
		Records: []string{
			"v=spf1 include:amazonses.com -all",
		},
	}
	var dkims []tf.Resource
	for i := 0; i < 3; i++ {
		dkims = append(dkims, &awsresource.Route53Record{
			ResourceName: fmt.Sprintf("dkim-%d", i),
			ZoneId:       zone.ResourceId().ReferenceAttr("zone_id"),
			RecordName:   fmt.Sprintf("${element(%s.dkim_tokens, %d)}._domainkey.%s", sesDkim.ResourceId(), i, domain),
			Type:         "CNAME",
			TTL:          "300",
			Records: []string{
				fmt.Sprintf("${element(%s.dkim_tokens, %d)}.dkim.amazonses.com", sesDkim.ResourceId(), i),
			},
		})
	}

	// Set up email receiving in order to verify email address.
	bucket := dataplatformresource.Bucket{
		Name:          "databricks-alerts-email",
		Region:        c.Region,
		LoggingBucket: tf.LocalId("logging_bucket").Reference(),
		ExtraLifecycleRules: []awsresource.S3LifecycleRule{
			awsresource.S3LifecycleRule{
				ID:      "expire-30d",
				Enabled: true,
				Expiration: awsresource.S3LifecycleRuleExpiration{
					Days: 30,
				},
			},
		},
		NonCurrentExpirationDaysOverride: 1,
		RnDCostAllocation:                1,
	}
	bucketPolicy := &awsresource.S3BucketPolicy{
		Bucket: bucket.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				policy.AWSPolicyStatement{
					Sid: "AllowSESPuts",
					Principal: map[string]string{
						"Service": "ses.amazonaws.com",
					},
					Effect: "Allow",
					Action: []string{
						"s3:PutObject",
					},
					Resource: []string{
						fmt.Sprintf("%s/*", bucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{
							"aws:Referer": strconv.Itoa(infraconsts.GetDatabricksAccountIdForRegion(c.Region)),
						},
					},
				},
				{
					Sid:       "RequireTLS",
					Effect:    "Deny",
					Principal: map[string]string{"AWS": "*"},
					Action:    []string{"s3:*"},
					Resource: []string{
						bucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", bucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
					Condition: &policy.AWSPolicyCondition{
						Bool: &policy.AWSPolicyConditionBool{SecureTransportEnabled: pointer.BoolPtr(false)},
					},
				},
			},
		},
	}
	ruleSet := &awsresource.SESReceiptRuleSet{
		RuleSetName: "primary",
	}
	rule := &awsresource.SESReceiptRule{
		Name: domain,
		BaseResource: tf.BaseResource{
			MetaParameters: tf.MetaParameters{
				DependsOn: []string{
					ruleSet.ResourceId().String(),
					bucketPolicy.ResourceId().String(),
				},
			},
		},
		RuleSetName: ruleSet.RuleSetName,
		Recipients:  []string{domain},
		Enabled:     true,
		S3Action: &awsresource.SESReceiptRuleS3Action{
			BucketName: bucket.Bucket().ResourceId().ReferenceAttr("bucket"),
			Position:   1,
		},
	}
	activeRuleSet := &awsresource.SESActiveReceiptRuleSet{
		RuleSetName: ruleSet.RuleSetName,
		BaseResource: tf.BaseResource{
			MetaParameters: tf.MetaParameters{
				DependsOn: []string{
					ruleSet.ResourceId().String(),
				},
			},
		},
	}

	// Allow sending emails to @samsara.com, @samsara-net.slack.com, and @samsara.pagerduty.com recipients from databricks-alerts.samsara.com domain.
	// Moving this policy from an inline policy to a managed policy because this is shared among multiple roles, and moving some inline policies to
	// managed will help us get back under the 10240 byte limit on aggregated inline policy lengths for IAM roles.
	sesIAMPolicy := &awsresource.IAMPolicy{
		ResourceName: "ses_send_email",
		Name:         "ses-send-email",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				policy.AWSPolicyStatement{
					Effect: "Allow",
					Action: []string{
						"ses:SendEmail",
						"ses:SendRawEmail",
					},
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
							"ses:FromAddress": dataplatformresource.DatabricksAlertsSender(c.Region),
						},
					},
				},
			},
		},
	}

	resources := map[string][]tf.Resource{
		"ses": append(
			append(
				bucket.Resources(),
				zone,
				domainIdentity,
				emailIdentity,
				sesDkim,
				token,
				mx,
				spf,
				bucketPolicy,
				rule,
				ruleSet,
				activeRuleSet,
				sesIAMPolicy,
			),
			dkims...),
	}

	resources["ses"] = append(resources["ses"], &awsresource.SESDomainIdentityVerification{
		BaseResource: tf.BaseResource{
			MetaParameters: tf.MetaParameters{
				DependsOn: []string{
					domainIdentity.ResourceId().String(),
					token.ResourceId().String(),
				},
			},
		},
		Domain: domain,
	})

	return resources
}

func ToolshedRole(c dataplatformconfig.DatabricksConfig) map[string][]tf.Resource {
	role := &awsresource.IAMRole{
		Name: "toolshed-role",
		AssumeRolePolicy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Action: []string{"sts:AssumeRole"},
					Principal: map[string]string{
						"AWS": fmt.Sprintf("arn:aws:iam::%d:root", infraconsts.GetAccountIdForRegion(c.Region)),
					},
					Effect: "Allow",
				},
			},
		},
		Tags: map[string]string{
			"samsara:service":       "toolshed-role",
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}

	readMetastorePolicy := &awsresource.IAMRolePolicy{
		Name: "read-data-pipelines-metastore",
		Role: role.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Action: []string{
						"dynamodb:GetItem",
					},
					Resource: []string{
						fmt.Sprintf("arn:aws:dynamodb:%s:%d:table/datapipeline-execution-state", c.Region, c.DatabricksAWSAccountId),
					},
				},
			},
		},
	}

	readStepFunctions := &awsresource.IAMRolePolicyAttachment{
		Name:      "read-step-functions",
		Role:      role.ResourceId().Reference(),
		PolicyARN: "arn:aws:iam::aws:policy/AWSStepFunctionsReadOnlyAccess",
	}

	startStepFunctionPolicy := &awsresource.IAMRolePolicy{
		Name: "start-step-function",
		Role: role.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Action: []string{
						"states:StartExecution",
					},
					Resource: []string{
						fmt.Sprintf("arn:aws:states:%s:%d:stateMachine:*", c.Region, c.DatabricksAWSAccountId),
					},
				},
			},
		},
	}

	deleteDynamoBackfillStateEntriesPolicy := &awsresource.IAMRolePolicy{
		Name: "delete-dynamo-backfill-state-entries",
		Role: role.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Action: []string{"dynamodb:DeleteItem"},
					Resource: []string{
						fmt.Sprintf("arn:aws:dynamodb:%s:%d:table/datapipeline-backfill-state", c.Region, infraconsts.GetDatabricksAccountIdForRegion(c.Region)),
					},
				},
			},
		},
	}

	s3DeleteNodeDataPolicy := &awsresource.IAMRolePolicy{
		Name: "s3-delete-node-data",
		Role: role.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Action: []string{"s3:List*", "s3:DeleteObject*"},
					Resource: []string{
						fmt.Sprintf("arn:aws:s3:::%s", awsregionconsts.RegionPrefix[c.Region]+"data-pipelines-delta-lake"),
						fmt.Sprintf("arn:aws:s3:::%s/*", awsregionconsts.RegionPrefix[c.Region]+"data-pipelines-delta-lake"),
					},
				},
			},
		},
	}

	return map[string][]tf.Resource{
		"toolshed": {
			role,
			readMetastorePolicy,
			readStepFunctions,
			startStepFunctionPolicy,
			deleteDynamoBackfillStateEntriesPolicy,
			s3DeleteNodeDataPolicy,
		},
	}
}

func MixpanelExportResources(c dataplatformconfig.DatabricksConfig) map[string][]tf.Resource {
	maxSessionDuration := 12 * 60 * 60

	mixpanelExportRole := &awsresource.IAMRole{
		Name: "mixpanel-export",
		AssumeRolePolicy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Action: []string{
						"sts:AssumeRole",
					},
					Principal: map[string]string{
						"AWS": "arn:aws:iam::485438090326:user/mixpanel-export",
					},
					Effect: "Allow",
				},
			},
		},
		MaxSessionDuration: maxSessionDuration,
		Tags: map[string]string{
			"samsara:service":       "mixpanel-export",
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}

	mixpanelExportGluePolicy := &awsresource.IAMRolePolicy{
		Name: "mixpanel-export-glue-policy",
		Role: mixpanelExportRole.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Sid:    "MixpanelGlueAccessStatement",
					Effect: "Allow",
					Action: []string{
						"glue:GetDatabase",
						"glue:CreateTable",
						"glue:GetTables",
						"glue:GetTableVersions",
						"glue:UpdateTable",
						"glue:DeleteTable",
						"glue:GetTable",
						"glue:GetPartition",
						"glue:CreatePartition",
						"glue:DeletePartition",
						"glue:UpdatePartition",
						"glue:BatchCreatePartition",
						"glue:GetPartitions",
						"glue:BatchDeletePartition",
						"glue:BatchGetPartition",
					},
					Resource: []string{
						fmt.Sprintf("arn:aws:glue:%s:%d:database/mixpanel", c.Region, infraconsts.GetDatabricksAccountIdForRegion(c.Region)),
						fmt.Sprintf("arn:aws:glue:%s:%d:table/mixpanel", c.Region, infraconsts.GetDatabricksAccountIdForRegion(c.Region)) + "/*",
						fmt.Sprintf("arn:aws:glue:%s:%d:catalog", c.Region, infraconsts.GetDatabricksAccountIdForRegion(c.Region)),
					},
				},
			},
		},
	}

	mixpanelExportBucket := dataplatformresource.Bucket{
		Name:                             "mixpanel",
		Region:                           c.Region,
		NonCurrentExpirationDaysOverride: 1,
		RnDCostAllocation:                1,
		EnableS3IntelligentTiering:       true,
		LoggingBucket:                    fmt.Sprintf("%sdatabricks-s3-logging", awsregionconsts.RegionPrefix[c.Region]),
	}

	mixpanelExportRoleBucketPolicy := &awsresource.IAMRolePolicy{
		Name: "mixpanel-export-bucket-policy",
		Role: mixpanelExportRole.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Sid:    "MixpanelBucketAccessStatement",
					Effect: "Allow",
					Action: []string{
						"s3:PutObject",
						"s3:GetObject",
						"s3:ListBucket",
						"s3:DeleteObject",
					},
					Resource: []string{
						mixpanelExportBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						mixpanelExportBucket.Bucket().ResourceId().ReferenceAttr("arn") + "/*",
					},
				},
			},
		},
	}

	mixpanelExportBucketPolicy := &awsresource.S3BucketPolicy{
		Bucket: mixpanelExportBucket.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: append([]policy.AWSPolicyStatement{
				{
					Sid:       "RequireTLS",
					Effect:    "Deny",
					Principal: map[string]string{"AWS": "*"},
					Action:    []string{"s3:*"},
					Resource: []string{
						mixpanelExportBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", mixpanelExportBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
					Condition: &policy.AWSPolicyCondition{
						Bool: &policy.AWSPolicyConditionBool{SecureTransportEnabled: pointer.BoolPtr(false)},
					},
				},
			}, s3emitters.RayCrossAccountReadOnlyBucketPolicyStatements(c.Cloud, mixpanelExportBucket.Name)...),
		},
	}

	return map[string][]tf.Resource{
		"role":   []tf.Resource{mixpanelExportRole, mixpanelExportGluePolicy, mixpanelExportRoleBucketPolicy},
		"bucket": append(mixpanelExportBucket.Resources(), mixpanelExportBucketPolicy),
	}
}

// createAuditLogReplicationIAMResources creates the IAM role resources so that our audit log data is replicated
// to an AWS S3 bucket owned by IT which is used by Splunk.
func createAuditLogReplicationIAMResources(c dataplatformconfig.DatabricksConfig, itDestinationBucketName string) []tf.Resource {
	return dataplatformresource.Role{
		Name:        "databricks-audit-log-replicaton-to-it-splunk-bucket",
		Description: "replicate databricks audit log to IT bucket for use in Splunk",
		AssumeRolePolicy: policy.AWSPolicyStatement{
			Action: []string{"sts:AssumeRole"},
			Principal: map[string]string{
				"Service": "s3.amazonaws.com",
			},
			Effect: "Allow",
		},
		InlinePolicies: []policy.AWSPolicyStatement{
			{
				Effect: "Allow",
				Action: []string{
					"s3:Get*",
				},
				Resource: []string{fmt.Sprintf("arn:aws:s3:::%s/*", awsregionconsts.RegionPrefix[c.Region]+"databricks-audit-log")},
			},
			{
				Effect: "Allow",
				Action: []string{
					"s3:GetReplicationConfiguration",
					"s3:ListBucket",
				},
				Resource: []string{fmt.Sprintf("arn:aws:s3:::%s", awsregionconsts.RegionPrefix[c.Region]+"databricks-audit-log")},
			},
			{
				Effect: "Allow",
				Action: []string{
					"s3:ReplicateObject",
					"s3:ReplicateDelete",
					"s3:ReplicateTags",
					"s3:GetObjectVersionTagging",
					"s3:ObjectOwnerOverrideToBucketOwner",
				},
				Resource: []string{fmt.Sprintf("arn:aws:s3:::%s/*", itDestinationBucketName)},
			},
		},
	}.Resources()
}

func ECR(c dataplatformconfig.DatabricksConfig) map[string][]tf.Resource {
	repos := []tf.Resource{
		&awsresource.ECRRepository{
			Name: "samsara-python-standard",
			Tags: []*awsresource.Tag{
				{
					Key:   "samsara:service",
					Value: "python-databricks-image",
				},
				{
					Key:   "samsara:team",
					Value: strings.ToLower(team.DataPlatform.Name()),
				},
				{
					Key:   "samsara:product-group",
					Value: strings.ToLower(team.TeamProductGroup[team.DataPlatform.Name()]),
				},
			},
			BaseResource: tf.BaseResource{
				MetaParameters: tf.MetaParameters{
					Lifecycle: tf.Lifecycle{
						PreventDestroy: true,
					},
				},
			},
		},
		&awsresource.ECRRepository{
			Name: "samsara-python-firmware",
			Tags: []*awsresource.Tag{
				{
					Key:   "samsara:service",
					Value: "python-databricks-image",
				},
				{
					Key:   "samsara:team",
					Value: strings.ToLower(team.Firmware.Name()),
				},
				{
					Key:   "samsara:product-group",
					Value: strings.ToLower(team.TeamProductGroup[team.Firmware.Name()]),
				},
			},
			BaseResource: tf.BaseResource{
				MetaParameters: tf.MetaParameters{
					Lifecycle: tf.Lifecycle{
						PreventDestroy: true,
					},
				},
			},
		},
	}

	policies := []tf.Resource{}
	for _, repo := range repos {
		repoPolicy := &awsresource.ECRLifecyclePolicy{
			Repository: repo.ResourceId(),
			Policy: policy.AWSECRLifecyclePolicy{
				Rules: []policy.AWSECRLifecycleRule{
					{
						RulePriority: 1,
						Description:  "Keep last 24 images",
						Selection: policy.AWSECRLifecycleSelection{
							TagStatus:   components.AWSLifecycleTagStatusAny,
							CountType:   components.AWSLifecycleCountTypeMoreThan,
							CountNumber: 24,
						},
						Action: map[string]string{
							"type": "expire",
						},
					},
				},
			},
		}

		repoPerm := &awsresource.ECRRepositoryPolicy{
			Repository: repo.ResourceId(),
			Policy: policy.AWSPolicy{
				Version: policy.AWSPolicyVersion,
				Statement: []policy.AWSPolicyStatement{
					{
						Effect: "Allow",
						Principal: map[string]string{
							"AWS": fmt.Sprintf("arn:aws:iam::%d:root", infraconsts.SamsaraAWSBuildkiteAccountID),
						},
						Action: []string{
							"ecr:GetDownloadUrlForLayer",
							"ecr:BatchGetImage",
							"ecr:BatchCheckLayerAvailability",
							"ecr:PutImage",
							"ecr:InitiateLayerUpload",
							"ecr:UploadLayerPart",
							"ecr:CompleteLayerUpload",
						},
					},
					{
						Effect: "Allow",
						Principal: map[string]string{
							"AWS": fmt.Sprintf("arn:aws:iam::%d:root", infraconsts.GetAccountIdForRegion(c.Region)),
						},
						Action: []string{
							"ecr:DescribeImages",
						},
					},
				},
			},
		}

		policies = append(policies, repoPolicy)
		policies = append(policies, repoPerm)
	}

	clusterPerm := &awsresource.IAMPolicy{
		ResourceName: "pull-ecr-docker-image",
		Name:         "pull-ecr-docker-image",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				policy.AWSPolicyStatement{
					Effect: "Allow",
					Action: []string{
						"ecr:GetAuthorizationToken",
					},
					Resource: []string{"*"},
				},
				policy.AWSPolicyStatement{
					Effect: "Allow",
					Action: []string{
						"ecr:BatchCheckLayerAvailability",
						"ecr:GetDownloadUrlForLayer",
						"ecr:GetRepositoryPolicy",
						"ecr:DescribeRepositories",
						"ecr:ListImages",
						"ecr:DescribeImages",
						"ecr:BatchGetImage",
					},
					Resource: []string{
						fmt.Sprintf("arn:aws:ecr:%s:%d:repository/samsara*", c.Region, infraconsts.GetDatabricksAccountIdForRegion(c.Region)),
						fmt.Sprintf("arn:aws:ecr:%s:%d:repository/samsara*", infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSPlaygroundAccountID),
					},
				},
			},
		},
	}

	return map[string][]tf.Resource{
		"repo":        repos,
		"policies":    policies,
		"clusterperm": []tf.Resource{clusterPerm},
	}
}

func DataScience(c dataplatformconfig.DatabricksConfig) map[string][]tf.Resource {
	repo := &awsresource.ECRRepository{
		Name: "samsara-python-datascience",
		Tags: []*awsresource.Tag{
			{
				Key:   "samsara:service",
				Value: "python-databricks-image",
			},
			{
				Key:   "samsara:team",
				Value: strings.ToLower(team.DataScience.Name()),
			},
			{
				Key:   "samsara:product-group",
				Value: strings.ToLower(team.TeamProductGroup[team.DataScience.Name()]),
			},
		},
		BaseResource: tf.BaseResource{
			MetaParameters: tf.MetaParameters{
				Lifecycle: tf.Lifecycle{
					PreventDestroy: true,
				},
			},
		},
	}

	repoPolicy := &awsresource.ECRLifecyclePolicy{
		Repository: repo.ResourceId(),
		Policy: policy.AWSECRLifecyclePolicy{
			Rules: []policy.AWSECRLifecycleRule{
				{
					RulePriority: 1,
					Description:  "Keep last 1000 images",
					Selection: policy.AWSECRLifecycleSelection{
						TagStatus:   components.AWSLifecycleTagStatusAny,
						CountType:   components.AWSLifecycleCountTypeMoreThan,
						CountNumber: 1000,
					},
					Action: map[string]string{
						"type": "expire",
					},
				},
			},
		},
	}

	repoPerm := &awsresource.ECRRepositoryPolicy{
		Repository: repo.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Principal: map[string]string{
						"AWS": fmt.Sprintf("arn:aws:iam::%d:root", infraconsts.GetAccountIdForRegion(c.Region)),
					},
					Action: []string{
						"ecr:GetDownloadUrlForLayer",
						"ecr:BatchGetImage",
						"ecr:BatchCheckLayerAvailability",
						"ecr:PutImage",
						"ecr:InitiateLayerUpload",
						"ecr:UploadLayerPart",
						"ecr:CompleteLayerUpload",
						"ecr:DescribeImages",
					},
				},
			},
		},
	}

	return map[string][]tf.Resource{
		"repo":     []tf.Resource{repo},
		"policies": []tf.Resource{repoPolicy, repoPerm},
	}
}

// getSharedBiztechGlueARNs returns the glue read-write ARNs for the biztech_s3_bucket_read_write managed policy.
// This contains the shared read/write databases between the biztechenterprisedata-cluster and biztech-prod-cluster profiles.
// We separate these out in order to stay within the 10240 aggregate byte limit for inline policies.
func getSharedBiztechGlueARNs(c dataplatformconfig.DatabricksConfig, glueBaseArn string, glueBase string) (glueReadWriteARNs []string, err error) {
	glueReadWriteARNs = []string{
		glueBaseArn,
	}
	_, _, readWriteDbsIntersection, err := findIntersectionOfBiztechenterprisedataAndBiztechProdReadWriteDbs(c)
	if err != nil {
		return nil, oops.Wrapf(err, "Error computing the readwriteDatabases intersection between biztech-prod and biztechenterprisedata roles")
	}
	for _, db := range readWriteDbsIntersection {
		glueReadWriteARNs = append(glueReadWriteARNs,
			fmt.Sprintf("%s:database/%s", glueBase, db),
			fmt.Sprintf("%s:table/%s/*", glueBase, db),
		)
	}
	return glueReadWriteARNs, nil
}

// getSharedBiztechGlueARNs returns the S3 read-write ARNs for the biztech_glue_database_read_write managed policy.
// This contains the shared read/write databases between the biztechenterprisedata-cluster and biztech-prod-cluster profiles.
// We separate these out in order to stay within the 10240 aggregate byte limit for inline policies.
func getSharedBiztechS3ARNs(c dataplatformconfig.DatabricksConfig) (s3ReadWriteARNs []string, err error) {
	_, _, readWriteDbsIntersection, err := findIntersectionOfBiztechenterprisedataAndBiztechProdReadWriteDbs(c)
	if err != nil {
		return nil, oops.Wrapf(err, "Error computing the readwriteDatabases intersection between biztech-prod and biztechenterprisedata roles")
	}

	for _, bucket := range databaseregistries.GetBucketsForDatabasesInRegion(c, readWriteDbsIntersection, databaseregistries.ReadWrite, databaseregistries.LegacyOnlyDatabases) {
		arn := fmt.Sprintf("arn:aws:s3:::%s", bucket)
		s3ReadWriteARNs = append(s3ReadWriteARNs,
			arn,
			arn+"/*",
		)
	}
	return s3ReadWriteARNs, nil
}
