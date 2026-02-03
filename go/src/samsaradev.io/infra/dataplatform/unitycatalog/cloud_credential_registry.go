package unitycatalog

import (
	"fmt"
	"strings"

	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/emrreplicationproject"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/dataplatform/unitycatalog/databaseregistries"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

// Abstraction for s3 permissions within a cloud credential
type CloudCredentialS3Permission struct {
	ReadWrite            bool
	WriteOnly            bool
	DisallowedRegions    []string
	UnprefixedBucketName string
	AllowedPrefixes      []string
}

type CloudCredentialDynamoDBPermission struct {
	Arns              []string
	DisallowedRegions []string
	ReadWrite         bool
	CanScan           bool
}

type CloudCredential struct {
	Name                       string
	S3Permissions              []CloudCredentialS3Permission
	SSMParameters              []dataplatformconsts.Parameter
	SqsConsumerQueues          []string
	SqsProducerQueues          []string
	BatchJobQueues             []string
	ReadKinesisStreamArns      []string // Kinesis stream ARNs for enhanced fan-out subscription
	SnsTopicPublishArns        []string // SNS topic ARNs for publishing messages
	SESAccess                  bool
	DynamoDBPermissions        []CloudCredentialDynamoDBPermission
	EnableReadFromCodeArtifact bool

	TeamsWithAccess []components.TeamInfo

	// The IAM roles we create need to be self-assuming, which isn't possible to do in terraform
	// in one-shot, since it creates a circular reference. So to get around this, we create credentials
	// first with `NeedsCreation_DONOTUSE` as true, which won't add the self-assuming portion, and then follow-up
	// in another PR to remove this field after the role has been created so that the self-assuming trust
	// policy can be added.
	NeedsCreation_DONOTUSE bool

	// A flag to indicate if the cloud credential can be created with self-assuming
	// role and policy in one step.
	// NOTE: This does not need to be explicitly set as it will be manually set as part of
	// the AllCloudCredentials() function.
	SelfAssumingRole bool
}

func AllCloudCredentials(config dataplatformconfig.DatabricksConfig) []CloudCredential {
	newSelfAssumingCloudCredentials := make([]CloudCredential, len(selfAssumingCloudCredentials))
	for i, cred := range selfAssumingCloudCredentials {
		cred.SelfAssumingRole = true
		newSelfAssumingCloudCredentials[i] = cred
	}
	allCloudCredentials := append(legacyCloudCredentialRegistry_DONOTADD, getAllTeamCloudCredentials()...)
	allCloudCredentials = append(allCloudCredentials, newSelfAssumingCloudCredentials...)
	allCloudCredentials = append(allCloudCredentials, getEmrReplicationCloudCredential(config))
	allCloudCredentials = append(allCloudCredentials, getEcoDrivingEfficiencyCloudCredentials(config)...)
	return allCloudCredentials
}

// This is a legacy registry of cloud credentials that should not be added to. It is here for legacy credentials
// that are still in use and have not been migrated. Use the below registry to add in order to not
// need to follow a 2-step process for adding new credentials via NeedsCreation_DONOTUSE flag.
var legacyCloudCredentialRegistry_DONOTADD = []CloudCredential{
	{
		Name: "test-databricks-workspace-read",
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.DatabricksWorkspaceBucket,
			},
		},
		TeamsWithAccess: []components.TeamInfo{team.DataPlatform},
	},
	{
		Name: "standard-read-parameters-ssm",
		SSMParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterSlackAppToken,
			dataplatformconsts.ParameterDatadogAppToken,
			dataplatformconsts.ParameterDatadogApiToken,
			dataplatformconsts.ParameterGoogleMapsApiToken,
		},
		TeamsWithAccess: clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name: "marketing-data-analytics-ssm",
		SSMParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterMarketingDataAnalyticsSegmentDevWriteKey,
			dataplatformconsts.ParameterMarketingDataAnalyticsSegmentProdWriteKey,
			dataplatformconsts.ParameterMarketingDataAnalyticsBigQueryDevPrivateId,
			dataplatformconsts.ParameterMarketingDataAnalyticsBigQueryProdPrivateId,
			dataplatformconsts.ParameterMarketingDataAnalyticsBigQueryDevPrivateKey,
			dataplatformconsts.ParameterMarketingDataAnalyticsBigQueryProdPrivateKey,
			dataplatformconsts.ParameterMarketingDataAnalyticsMixpanelDevProjectToken,
			dataplatformconsts.ParameterMarketingDataAnalyticsMixpanelProdProjectToken,
			dataplatformconsts.ParameterMarketingDataAnalyticsMixpanelDevAPISecret,
			dataplatformconsts.ParameterMarketingDataAnalyticsMixpanelProdAPISecret,
			dataplatformconsts.ParameterMarketingDataAnalyticsOn24DevAccessTokenKey,
			dataplatformconsts.ParameterMarketingDataAnalyticsOn24ProdAccessTokenKey,
			dataplatformconsts.ParameterMarketingDataAnalyticsOn24DevAccessTokenSecret,
			dataplatformconsts.ParameterMarketingDataAnalyticsOn24ProdAccessTokenSecret,
			dataplatformconsts.ParameterMarketingDataAnalyticsSalesforceDevUsername,
			dataplatformconsts.ParameterMarketingDataAnalyticsSalesforceDevPassword,
			dataplatformconsts.ParameterMarketingDataAnalyticsSalesforceDevSecurityToken,
			dataplatformconsts.ParameterMarketingDataAnalyticsIterableDevAPIToken,
			dataplatformconsts.ParameterMarketingDataAnalyticsIterableProdAPIToken,
		},
		TeamsWithAccess: []components.TeamInfo{team.MarketingDataAnalytics},
	},
	{
		Name: "here-maps-api-token-ssm",
		SSMParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterHereMapsApiToken,
		},
		TeamsWithAccess: []components.TeamInfo{team.Routing, team.SafetyPlatform},
	},
	{
		Name: "jdm-ftp-ssh-private-key-ssm",
		SSMParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterJdmFtpSshPrivateKey,
		},
		TeamsWithAccess: []components.TeamInfo{team.CoreServices},
	},
	{
		Name: "datahub-ssm",
		SSMParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterDatahubToken,
			dataplatformconsts.ParameterDatahubServer,
			dataplatformconsts.ParameterDatahubDbtApiToken,
		},
		TeamsWithAccess: []components.TeamInfo{
			team.DataEngineering,
			team.DataAnalytics,
			team.Firmware,
			team.MarketingDataAnalytics,
		},
	},
	{
		Name: "dmv2-rollout-ld-key-ssm",
		SSMParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterDMV2RolloutLDKey,
		},
		TeamsWithAccess: []components.TeamInfo{team.Compliance},
	},
	{
		Name: "greenhouse-api-token-ssm",
		SSMParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterGreenhouseApiToken,
		},
		TeamsWithAccess: []components.TeamInfo{
			team.DataScience,
			team.DataEngineering,
			team.DataAnalytics,
			team.Firmware,
			team.MarketingDataAnalytics,
		},
	},
	{
		Name: "tomtom-api-token-ssm",
		SSMParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterTomTomApiToken,
		},
		TeamsWithAccess: []components.TeamInfo{team.SafetyPlatform, team.SafetyEventIngestionAndFiltering},
	},
	{
		Name: "databricks-eu-token-ssm",
		SSMParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterDatabricksEuToken,
		},
		TeamsWithAccess: []components.TeamInfo{
			team.DataEngineering,
			team.DataAnalytics,
			team.Firmware,
			team.MarketingDataAnalytics,
		},
	},
	{
		Name: "dagster-slack-bot-token-ssm",
		SSMParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterDagsterSlackBotToken,
		},
		TeamsWithAccess: []components.TeamInfo{
			team.DataEngineering,
			team.DataAnalytics,
			team.Firmware,
			team.MarketingDataAnalytics,
		},
	},
	{
		Name: "netsuiteprocessor-sqs",
		SqsProducerQueues: []string{
			"samsara_netsuiteinvoiceworker_*",
			"samsara_eu_netsuiteinvoiceworker_*",
			"samsara_licenseingestionworker_*",
			"samsara_eu_licenseingestionworker_*",
			"samsara_exchangeingestionworker_*",
			"samsara_eu_exchangeingestionworker_*",
			"samsara_netsuite_record_delete_*",
		},
		TeamsWithAccess: []components.TeamInfo{team.PlatformOperations},
	},
	{
		Name: "data-collection-sqs",
		SqsProducerQueues: []string{
			"samsara_data_collection_create_internal_retrieval_*",
			"samsara_eu_data_collection_create_internal_retrieval_*",
		},
		TeamsWithAccess: []components.TeamInfo{team.PlatformOperations, team.MLCV},
	},
	{
		Name: "samsara-safety-map-data-sources-readwrite",
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.SafetyMapDataSourcesBucket,
			},
		},
		TeamsWithAccess: []components.TeamInfo{team.SafetyPlatform, team.SafetyEventIngestionAndFiltering},
	},
	{
		Name: "samsara-maps-readwrite",
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.MapsBucket,
			},
		},
		TeamsWithAccess: []components.TeamInfo{team.Maps},
	},
	{
		Name: "samsara-workforce-data-read",
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.WorkforceDataBucket,
			},
		},
		TeamsWithAccess: []components.TeamInfo{team.DataEngineering},
	},
	{
		Name: "samsara-benchmarking-metrics-read",
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.BenchmarkingMetricsBucket,
			},
		},
		TeamsWithAccess: []components.TeamInfo{team.DataScience},
	},
	{
		Name: "samsara-databricks-playground-read",
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.LegacyDatabricksPlaygroundBucket_DONOTUSE,
			},
		},
		TeamsWithAccess: clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name: "samsara-firmware-test-automation-read",
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.FirmwareTestAutomationBucket,
			},
		},
		TeamsWithAccess: []components.TeamInfo{team.Hardware},
	},
	{
		Name: "samsara-s3-inventory-read",
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.S3InventoryBucket,
			},
		},
		TeamsWithAccess: []components.TeamInfo{team.SafetyCameraServices},
	},
	{
		Name: "samsara-att-sftp-read",
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.AttSftpBucket,
			},
		},
		TeamsWithAccess: []components.TeamInfo{team.CoreServices},
	},
	{
		Name: "samsara-com-nav-daily-driver-session-count-read",
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.CommercialNavigationDailyDriverSessionCountBucket,
			},
		},
		TeamsWithAccess: []components.TeamInfo{team.Navigation},
	},
	{
		Name: "samsara-detailed-ifta-reports-readwrite",
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.DetailedIftaReportsBucket,
			},
		},
		TeamsWithAccess: []components.TeamInfo{team.IftaCore},
	},
	{
		Name: "samsara-databricks-workspace-readwrite",
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.DatabricksWorkspaceBucket,
			},
		},
		TeamsWithAccess: []components.TeamInfo{team.IftaCore},
	},
	{
		Name: "samsara-dss-ml-models-readwrite",
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.DssMlModelsBucket,
			},
		},
		TeamsWithAccess: []components.TeamInfo{team.DecisionScience, team.DataPlatform, team.DataEngineering, team.DataTools, team.DataAnalytics, team.DataScience},
	},
	{
		Name: "samsara-datahub-metadata-readwrite",
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.DataHubMetadataBucket,
			},
		},
		TeamsWithAccess: []components.TeamInfo{team.DecisionScience, team.DataPlatform, team.DataEngineering, team.DataTools, team.DataAnalytics, team.DataScience, team.ProductManagement},
	},
	{
		Name: "samsara-jasper-rate-plan-optimization-readwrite",
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.JasperRatePlanOptimizationBucket,
			},
		},
		TeamsWithAccess: []components.TeamInfo{team.CoreServices},
	},
	{
		Name: "samsara-thor-variants-readwrite",
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.ThorVariantsBucket,
			},
		},
		TeamsWithAccess: []components.TeamInfo{team.CoreServices},
	},
	{
		Name: "samsara-awsbilluploader-readwrite",
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.AwsbilluploaderBucket,
			},
		},
		TeamsWithAccess: []components.TeamInfo{team.OPX},
	},
	{
		Name: "samsara-netsuite-invoice-exports-readwrite",
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.NetsuiteInvoiceExportsBucket,
			},
		},
		TeamsWithAccess: []components.TeamInfo{team.PlatformOperations},
	},
	{
		Name: "samsara-platops-databricks-metadata-readwrite",
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.PlatopsDatabricksMetadataBucket,
			},
		},
		TeamsWithAccess: []components.TeamInfo{team.PlatformOperations},
	},
	{
		Name: "samsara-databricks-netsuite-invoice-output-readwrite",
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.DatabricksNetsuiteInvoiceOutputBucket,
			},
		},
		TeamsWithAccess: []components.TeamInfo{team.PlatformOperations},
	},
	{
		Name:            "ses-send-email",
		SESAccess:       true,
		TeamsWithAccess: clusterhelpers.DatabricksClusterTeams(),
	},
}

// Add new cloud credentials here. These credentials should be self-assuming.
var selfAssumingCloudCredentials = []CloudCredential{
	{
		Name: "rds-replication",
		SSMParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterDatadogAppToken,
			dataplatformconsts.ParameterDatadogApiToken,
		},
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.RdsExportBucket,
			},
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.DataplatformDeployedArtifactsBucket,
			},
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.ProdAppConfigsBucket,
			},
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.RdsDeltaLakeBucket,
			},
		},
	},
	{
		Name: "dynamodb-replication",
		SSMParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterDatadogAppToken,
			dataplatformconsts.ParameterDatadogApiToken,
		},
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.DynamoDbExportBucket,
			},
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.DynamoDbDeltaLakeBucket,
			},
		},
	},
	{
		Name: "kinesisstats-replication",
		SSMParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterDatadogAppToken,
			dataplatformconsts.ParameterDatadogApiToken,
		},
		SqsConsumerQueues: []string{
			"samsara_delta_lake_merge_ks_*",
		},
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.KinesisstatsJsonExportBucket,
			},
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.DataplatformDeployedArtifactsBucket,
			},
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.KinesisstatsDeltaLakeBucket,
			},
		},
	},
	{
		Name: "s3bigstats-replication",
		SSMParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterDatadogAppToken,
			dataplatformconsts.ParameterDatadogApiToken,
		},
		SqsConsumerQueues: []string{
			"samsara_delta_lake_merge_s3_big_stat_*",
		},
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.S3BigStatsJsonExportBucket,
			},
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.DataplatformDeployedArtifactsBucket,
			},
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.S3BigStatsDeltaLakeBucket,
			},
		},
	},
	{
		Name: "data-pipelines-replication",
		SSMParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterDatadogAppToken,
			dataplatformconsts.ParameterDatadogApiToken,
		},
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.DataPipelinesDeltaLakeBucket,
			},
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.DatabricksWarehouseBucket,
			},
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.ZendeskBucket,
			},
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.LegacyDatabricksPlaygroundBucket_DONOTUSE,
			},
			// Unsure what this is for. Enable later if needed. Otherwise, remove.
			// {
			// 	ReadWrite:            true,
			// 	UnprefixedBucketName: databaseregistries.DatabricksJobResourcesBucket,
			// },
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.DataPipelinesDeltaLakeFromEuWest1Bucket,
				DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			},
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.DataplatformDeployedArtifactsBucket,
			},
		},
	},
	{
		Name: "dataplatform-deployed-artifacts-read",
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.DataplatformDeployedArtifactsBucket,
			},
		},
	},
	{
		Name: "amundsen-metadata-readwrite",
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.AmundsenMetadataBucket,
			},
		},
	},
	{
		Name: "report-aggregator-s3",
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.DataplatformDeployedArtifactsBucket,
			},
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.PartialReportAggregationBucket,
			},
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.ReportStagingTablesBucket,
			},
		},
	},
	{
		Name: "s3-inventory-read",
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.S3InventoryBucket,
			},
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.DatabricksS3InventoryBucket,
			},
		},
	},
	{
		Name: "ks-diff-tool",
		SSMParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterDatadogAppToken,
			dataplatformconsts.ParameterDatadogApiToken,
		},
		TeamsWithAccess: []components.TeamInfo{team.DataPlatform},
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.DatabricksKinesisstatsDiffsBucket,
			},
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.DatabricksWorkspaceBucket,
			},
		},
	},
	{
		Name: "dojo-samsara-365k-read",
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.DojoSamsara365KBucket,
				DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			},
		},
		TeamsWithAccess: []components.TeamInfo{team.MLCV, team.MLDeviceFarm, team.MLInfra},
	},
	{
		Name: "ml-infra-spark-streaming",
		SSMParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterDatadogAppToken,
			dataplatformconsts.ParameterDatadogApiToken,
		},
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.DataplatformDeployedArtifactsBucket,
			},
		},
		TeamsWithAccess:            []components.TeamInfo{team.MLInfra, team.MLInfraAdmin, team.MLScience},
		EnableReadFromCodeArtifact: true,
		SnsTopicPublishArns: []string{
			"arn:aws:sns:us-west-2:175636180121:dpf-failure-stream-data-topic",
			"arn:aws:sns:eu-west-1:120551827380:dpf-failure-stream-data-topic",
			"arn:aws:sns:us-west-2:175636180121:fuel-denoising-stream-data-topic.fifo",
			"arn:aws:sns:eu-west-1:120551827380:fuel-denoising-stream-data-topic.fifo",
		},
		ReadKinesisStreamArns: []string{
			"arn:aws:kinesis:us-west-2:781204942244:stream/*dpf_failure_stream",
		},
	},
	{
		Name: "samsara-query-agents-read",
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.QueryAgentsBucket,
			},
		},
		TeamsWithAccess: []components.TeamInfo{team.DataPlatform, team.DataEngineering, team.DataTools},
	},
	{
		Name: "samsara-query-agents-write",
		S3Permissions: []CloudCredentialS3Permission{
			{
				WriteOnly:            true,
				UnprefixedBucketName: databaseregistries.QueryAgentsBucket,
			},
		},
		TeamsWithAccess: clusterhelpers.DatabricksClusterTeams(),
	},
}

func getEmrReplicationCloudCredential(config dataplatformconfig.DatabricksConfig) CloudCredential {
	credential := CloudCredential{
		Name: "emr-replication",
		SSMParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterDatadogAppToken,
			dataplatformconsts.ParameterDatadogApiToken,
		},
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.DataplatformDeployedArtifactsBucket,
			},
		},
		SelfAssumingRole: true,
	}
	allEmrReplicationDeltaLakeBucketNames := emrreplicationproject.GetAllEmrReplicationDeltaLakeBucketNames(config)
	for _, bucketName := range allEmrReplicationDeltaLakeBucketNames {
		credential.S3Permissions = append(credential.S3Permissions, CloudCredentialS3Permission{
			ReadWrite:            true,
			UnprefixedBucketName: bucketName,
		})
	}

	allEmrReplicationExportBucketNames := emrreplicationproject.GetAllEmrReplicationExportBucketNames(config)
	for _, bucketName := range allEmrReplicationExportBucketNames {
		credential.S3Permissions = append(credential.S3Permissions, CloudCredentialS3Permission{
			ReadWrite:            false,
			UnprefixedBucketName: bucketName,
		})
	}

	return credential
}

func getEcoDrivingEfficiencyCloudCredentials(config dataplatformconfig.DatabricksConfig) []CloudCredential {
	credentials := []CloudCredential{}

	for _, env := range []string{"dev", "prod"} {

		var accountId string

		switch config.Region {
		case infraconsts.SamsaraAWSDefaultRegion:
			if env == "prod" {
				accountId = infraconsts.SamsaraAWSDataScienceProdAccountID
			} else {
				accountId = infraconsts.SamsaraAWSDataScienceAccountID
			}
		case infraconsts.SamsaraAWSEURegion:
			if env == "prod" {
				accountId = infraconsts.SamsaraAWSEUDataScienceProdAccountID
			} else {
				accountId = infraconsts.SamsaraAWSEUDataScienceAccountID
			}
		case infraconsts.SamsaraAWSCARegion:
			if env == "prod" {
				accountId = infraconsts.SamsaraAWSCADataScienceProdAccountID
			} else {
				accountId = infraconsts.SamsaraAWSCADataScienceAccountID
			}
		}

		credentials = append(credentials, CloudCredential{
			Name:             fmt.Sprintf("ecodriving-efficiency-%s", env),
			TeamsWithAccess:  []components.TeamInfo{team.Sustainability, team.DataEngineering},
			SelfAssumingRole: true,
			DynamoDBPermissions: []CloudCredentialDynamoDBPermission{
				{
					Arns: []string{
						fmt.Sprintf("arn:aws:dynamodb:%s:%s:table/ecodriving-efficiency-%s", config.Region, accountId, env),
						fmt.Sprintf("arn:aws:dynamodb:%s:%s:table/ecodriving-efficiency-%s/index/*", config.Region, accountId, env),
					},
					ReadWrite: true,
					CanScan:   true,
				},
			},
		})

	}
	return credentials
}

var standardDevReadBuckets = []string{ // lint: +sorted
	databaseregistries.AttSftpBucket,
	databaseregistries.BenchmarkingMetricsBucket,
	databaseregistries.DatabricksBillingBucket,
	databaseregistries.DatabricksS3InventoryBucket,
	databaseregistries.DatabricksSqlQueryHistoryBucket,
	databaseregistries.DatabricksWarehouseBucket,
	databaseregistries.DatabricksWorkspaceBucket,
	databaseregistries.DataEngMappingTablesBucket,
	databaseregistries.DataInsightsBucket,
	databaseregistries.DataPipelinesDeltaLakeBucket,
	databaseregistries.DataScienceExploratoryBucket,
	databaseregistries.DatastreamLakeBucket,
	databaseregistries.DatastreamsDeltaLakeBucket,
	databaseregistries.DevmetricsBucket,
	databaseregistries.DynamoDbDeltaLakeBucket,
	databaseregistries.DynamoDbExportBucket,
	databaseregistries.FivetranIncidentioDeltaTablesBucket,
	databaseregistries.FivetranPagerDutyDeltaTablesBucket,
	databaseregistries.KinesisstatsDeltaLakeBucket,
	databaseregistries.KinesisstatsJsonExportBucket,
	databaseregistries.MixpanelBucket,
	databaseregistries.PartialReportAggregationBucket,
	databaseregistries.ProdAppConfigsBucket,
	databaseregistries.RdsDeltaLakeBucket,
	databaseregistries.RdsExportBucket,
	databaseregistries.ReportStagingTablesBucket,
	databaseregistries.S3BigStatsDeltaLakeBucket,
	databaseregistries.S3BigStatsJsonExportBucket,
	databaseregistries.S3InventoryBucket,
	databaseregistries.SparkPlaygroundBucket,
	databaseregistries.YearReviewMetricsBucket,
	databaseregistries.ZendeskBucket,
}

var standardDevReadWriteBuckets = []string{ // lint: +sorted
	databaseregistries.LegacyDatabricksPlaygroundBucket_DONOTUSE,
}

var standardReadParameters = []dataplatformconsts.Parameter{
	dataplatformconsts.ParameterSlackAppToken,
	dataplatformconsts.ParameterDatadogAppToken,
	dataplatformconsts.ParameterDatadogApiToken,
	dataplatformconsts.ParameterGoogleMapsApiToken,
}

var sensitiveReadBuckets = []CloudCredentialS3Permission{
	{
		ReadWrite:            false,
		UnprefixedBucketName: databaseregistries.SamsaraCvDataBucket,
	},
	{
		ReadWrite:            false,
		UnprefixedBucketName: databaseregistries.SamsaraDashcamVideosBucket,
	},
	{
		ReadWrite:            false,
		UnprefixedBucketName: databaseregistries.SamsaraMlModelsBucket,
	},
	{
		ReadWrite:            false,
		UnprefixedBucketName: databaseregistries.SamsaraWorkforceVideoAssetsBucket,
	},
}

var sensitiveReadWriteBuckets = []CloudCredentialS3Permission{ // lint: +sorted
	{
		ReadWrite:            true,
		UnprefixedBucketName: databaseregistries.BigdataDatashareBucket,
	},
	{
		ReadWrite:            true,
		UnprefixedBucketName: databaseregistries.DatasetsBucket,
	},
	{
		ReadWrite:            true,
		UnprefixedBucketName: databaseregistries.WorkforceDataBucket,
	},
}

func getAllTeamCloudCredentials() []CloudCredential {
	var credentials []CloudCredential
	for _, t := range clusterhelpers.DatabricksClusterTeams() {
		// Skip creating cloud credential for teams to be migrated to biztech workspace.
		if _, ok := dataplatformresource.DONOTMIGRATE_Clusters[strings.ToLower(t.TeamName)]; ok {
			continue
		}

		credentials = append(credentials, makeTeamCloudCredentials(&t))
	}
	return credentials
}

func makeTeamCloudCredentials(team *components.TeamInfo) CloudCredential {
	// Default credentials for a team.
	teamCredential := CloudCredential{
		Name:             fmt.Sprintf("team-%s", strings.ToLower(team.TeamName)),
		TeamsWithAccess:  []components.TeamInfo{*team},
		SelfAssumingRole: true,
		SESAccess:        true,
		SSMParameters:    standardReadParameters,
	}

	for _, bucket := range standardDevReadBuckets {
		teamCredential.S3Permissions = append(teamCredential.S3Permissions, CloudCredentialS3Permission{
			ReadWrite:            false,
			UnprefixedBucketName: bucket,
		})
	}

	for _, bucket := range standardDevReadWriteBuckets {
		teamCredential.S3Permissions = append(teamCredential.S3Permissions, CloudCredentialS3Permission{
			ReadWrite:            true,
			UnprefixedBucketName: bucket,
		})
	}

	teamCredential.S3Permissions = append(teamCredential.S3Permissions, CloudCredentialS3Permission{
		ReadWrite:            true,
		UnprefixedBucketName: "databricks-workspace",
		AllowedPrefixes:      []string{strings.ToLower(team.TeamName)},
	})

	// Apply overrides for specific teams.
	overrides := CloudCredential{}
	if entry, ok := teamCloudCredentialOverrides[team.TeamName]; ok {
		overrides = entry
	}
	teamCredential.S3Permissions = append(teamCredential.S3Permissions, overrides.S3Permissions...)
	teamCredential.SSMParameters = append(teamCredential.SSMParameters, overrides.SSMParameters...)
	teamCredential.SqsConsumerQueues = append(teamCredential.SqsConsumerQueues, overrides.SqsConsumerQueues...)
	teamCredential.SqsProducerQueues = append(teamCredential.SqsProducerQueues, overrides.SqsProducerQueues...)
	teamCredential.BatchJobQueues = append(teamCredential.BatchJobQueues, overrides.BatchJobQueues...)
	teamCredential.SESAccess = overrides.SESAccess
	teamCredential.EnableReadFromCodeArtifact = overrides.EnableReadFromCodeArtifact
	teamCredential.ReadKinesisStreamArns = append(teamCredential.ReadKinesisStreamArns, overrides.ReadKinesisStreamArns...)
	teamCredential.SnsTopicPublishArns = append(teamCredential.SnsTopicPublishArns, overrides.SnsTopicPublishArns...)

	return teamCredential
}

var teamCloudCredentialOverrides = map[string]CloudCredential{ // lint: +sorted
	team.Cloud.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.GqlMappingBucket,
			},
		},
	},
	team.Compliance.TeamName: {
		SSMParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterDMV2RolloutLDKey,
		},
	},
	team.ConnectedEquipment.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.NetworkCoverageBucket,
			},
		},
	},
	team.ConnectedWorker.TeamName: {
		S3Permissions: append(sensitiveReadBuckets, sensitiveReadWriteBuckets...),
	},
	team.CoreServices.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.ProdAppLogsBucket,
			},
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.VodafoneReportsBucket,
			},
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.ThorVariantsBucket,
			},
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.GqlMappingBucket,
			},
		},
		SSMParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterJdmFtpSshPrivateKey,
		},
	},
	team.DataAnalytics.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.BenchmarkingMetricsBucket,
			},
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.DataInsightsBucket,
			},
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.YearReviewMetricsBucket,
			},
		},
	},
	team.DataEngineering.TeamName: {
		S3Permissions: append([]CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.BigdataDatashareBucket,
			},
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.DatasetsBucket,
			},
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.LegacyDatabricksPlaygroundBucket_DONOTUSE,
			},
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.YearReviewMetricsBucket,
			},
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.DatabricksKinesisstatsDiffsBucket,
			},
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.AiDatasetsBucket,
			},
		}, sensitiveReadBuckets...),
	},
	team.DataPlatform.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.DatabricksKinesisstatsDiffsBucket,
			},
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.DatabricksRdsDiffsBucket,
			},
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.RdsDeltaLakeBucket,
			},
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.YearReviewMetricsBucket,
			},
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.DataPlatformFivetranBronzeBucket,
			},
		},
	},
	team.DataProducts.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.BenchmarkingMetricsBucket,
			},
		},
	},
	team.DataScience.TeamName: {
		S3Permissions: append(append([]CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.BenchmarkingMetricsBucket,
			},
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.BigdataDatashareBucket,
			},
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.DataInsightsBucket,
			},
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.DojoDataExchangeBucket,
			},
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.YearReviewMetricsBucket,
			},
		}, sensitiveReadWriteBuckets...), sensitiveReadBuckets...),
	},
	team.DecisionScience.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.BiztechEdwSilverBucket,
			},
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.BiztechEdwGoldBucket,
			},
		},
	},
	team.Firmware.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.SamsaraDashcamVideosBucket,
			},
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.SensorReplayBucket,
			},
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.AiDatasetsBucket,
			},
		},
	},
	team.FirmwareVdp.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.SamsaraDashcamVideosBucket,
			},
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.SensorReplayBucket,
			},
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.AiDatasetsBucket,
			},
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.SafetyMapDataSourcesBucket,
			},
		},
	},
	team.FuelServices.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.BenchmarkingMetricsBucket,
			},
		},
	},
	team.Hardware.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.FirmwareTestAutomationBucket,
			},
		},
	},
	team.Maps.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.SafetyMapDataSourcesBucket,
			},
		},
	},
	team.MediaExperience.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.MediaExperienceProdBucket,
			},
		},
	},
	team.MLCV.TeamName: {
		S3Permissions: append(append([]CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.DojoDataExchangeBucket,
			},
		}, sensitiveReadBuckets...), sensitiveReadWriteBuckets...),
		SqsProducerQueues: []string{
			"samsara_data_collection_create_internal_retrieval_*",
			"samsara_eu_data_collection_create_internal_retrieval_*",
		},
	},
	team.Navigation.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.CommercialNavigationDailyDriverSessionCountBucket,
			},
		},
	},
	team.PlatformAlertsWorkflows.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.LegacyDatabricksPlaygroundBucket_DONOTUSE,
			},
			// I don't think it's right to give access to the entire workspace bucket.
			// {
			// 	ReadWrite:            true,
			// 	UnprefixedBucketName: databaseregistries.DatabricksWorkspaceBucket,
			// },
		},
	},
	team.PlatformOperations.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.NetsuiteDeltaTablesBucket,
			},
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.NetsuiteSamReportsBucket,
			},
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.DatabricksNetsuiteInvoiceOutputBucket,
			},
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.PlatopsDatabricksMetadataBucket,
			},
		},
		SqsProducerQueues: []string{
			"samsara_netsuiteinvoiceworker_*",
			"samsara_eu_netsuiteinvoiceworker_*",
			"samsara_licenseingestionworker_*",
			"samsara_eu_licenseingestionworker_*",
		},
	},
	team.PlatformReports.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.YearReviewMetricsBucket,
			},
		},
	},
	team.QualityAssured.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.FirmwareTestAutomationBucket,
			},
		},
	},
	team.Routing.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.SafetyMapDataSourcesBucket,
			},
		},
		SSMParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterHereMapsApiToken,
		},
	},
	team.SafetyADAS.TeamName: {
		S3Permissions: append(sensitiveReadBuckets, sensitiveReadWriteBuckets...),
	},
	team.SafetyCameraServices.TeamName: {
		S3Permissions: sensitiveReadBuckets,
	},
	team.SafetyEventIngestionAndFiltering.TeamName: {
		S3Permissions: append([]CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.MapTilesBucket,
			},
		}, sensitiveReadWriteBuckets...),
		SSMParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterTomTomApiToken,
		},
	},
	team.SafetyFirmware.TeamName: {
		S3Permissions: append(append([]CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.BigdataDatashareBucket,
			},
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.AiDatasetsBucket,
			},
		}, sensitiveReadWriteBuckets...), sensitiveReadBuckets...),
	},
	team.SafetyPlatform.TeamName: {
		S3Permissions: append(append([]CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.MapTilesBucket,
			},
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.SafetyMapDataSourcesBucket,
			},
		}, sensitiveReadWriteBuckets...), append([]CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.AiDatasetsBucket,
			},
		}, sensitiveReadBuckets...)...),
		SSMParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterTomTomApiToken,
			dataplatformconsts.ParameterHereMapsApiToken,
		},
	},
	team.Security.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.LoadBalancerAccesLogsBucket,
			},
		},
	},
	team.SeOps.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.BiztechEdwGoldBucket,
			},
		},
	},
	team.SmartMaps.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.SmartMapsBucket,
			},
		},
	},
	team.SRE.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.EC2LogsBucket,
			},
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.AWSReportsBucket,
			},
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.OrgWideStorageLensExportBucket,
			},
		},
	},
	team.STCEUnpowered.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.NetworkCoverageBucket,
			},
		},
	},
	team.SupplyChain.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.BiztechEdwGoldBucket,
			},
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.BiztechEdwBronzeBucket,
			},
		},
	},
	team.Support.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.SupportCloudwatchLogsBucket,
			},
		},
	},
	team.SupportOps.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.BiztechEdwSilverBucket,
			},
		},
	},
	team.Sustainability.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.BenchmarkingMetricsBucket,
			},
			{
				ReadWrite:            true,
				UnprefixedBucketName: databaseregistries.DatabricksWorkspaceBucket,
				AllowedPrefixes:      []string{"fuelservices"},
			},
		},
	},
	team.TestAutomation.TeamName: {
		S3Permissions: []CloudCredentialS3Permission{
			{
				ReadWrite:            false,
				UnprefixedBucketName: databaseregistries.FirmwareTestAutomationBucket,
			},
		},
	},
}
