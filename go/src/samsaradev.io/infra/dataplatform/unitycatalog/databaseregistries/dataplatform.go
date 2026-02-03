package databaseregistries

import (
	"fmt"
	"strings"

	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/emrreplicationproject"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var DataPlatformDatabaseRegistry = []SamsaraDB{ // lint: +sorted
	{
		Name:          "apollo",
		Bucket:        LegacyDatabricksPlaygroundBucket_DONOTUSE,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: PlaygroundDatabaseGroup_LEGACY,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:          "appconfigs",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:          "auditlog",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.DataPlatform,
			team.DataTools,
			team.FirmwareVdp,
			team.DataAnalytics,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers":   {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":        {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "bigquery",
		Bucket:        LegacyDatabricksPlaygroundBucket_DONOTUSE,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: PlaygroundDatabaseGroup_LEGACY,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-epofinance":             {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "billing",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.DataPlatform,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "cloudtrail",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:          "cm3xinvestigation",
		Bucket:        LegacyDatabricksPlaygroundBucket_DONOTUSE,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: PlaygroundDatabaseGroup_LEGACY,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:          "dataplat_ucx",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.DataPlatform,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "dataplatform",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:          "dataplatform_beta_report",
		Bucket:        DataPipelinesDeltaLakeBucket,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		SkipGlueCatalog: true,
		CanReadGroups:   clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:          "dataplatform_dev",
		Bucket:        DatabricksWorkspaceBucket,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: TeamDevDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.DataPlatform,
			team.DataTools,
			team.FirmwareVdp,
			team.DataAnalytics,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-techsupport": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "dataplatform_stable_report",
		Bucket:        DataPipelinesDeltaLakeBucket,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		SkipGlueCatalog: true,
		CanReadGroups:   clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:          "datastreams",
		Bucket:        DatastreamsDeltaLakeBucket,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		SkipGlueCatalog: true,
		CanReadGroups:   clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.DataPlatform,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers":   {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-csops-developers":         {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":              {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":        {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "datastreams_errors",
		Bucket:        DatastreamLakeBucket,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:          "datastreams_history",
		Bucket:        DatastreamsDeltaLakeBucket,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		SkipGlueCatalog: true,
		CanReadGroups:   clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers":   {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":              {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":        {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "datastreams_schema",
		Bucket:        DatastreamLakeBucket,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		SkipGlueCatalog: true,
		CanReadGroups:   clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:          "dynamodb",
		Bucket:        DynamoDbDeltaLakeBucket,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		SkipGlueCatalog: true,
		CanReadGroups:   clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers":   {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-csops-developers":         {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":              {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":        {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "emr",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: EmrDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:          "helpers",
		Bucket:        LegacyDatabricksPlaygroundBucket_DONOTUSE,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: PlaygroundDatabaseGroup_LEGACY,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		SkipGlueCatalog: true,
		CanReadGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataApplication,
		},
	},
	{
		Name:               "kinesisstats",
		Bucket:             KinesisstatsDeltaLakeBucket,
		LegacyGlueS3Bucket: DatabricksWarehouseBucket,
		OwnerTeam:          team.DataPlatform,
		DatabaseGroup:      ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-csops-developers":         {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-supplychain-ops":          {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":              {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":        {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:               "kinesisstats_diff",
		Bucket:             DatabricksWarehouseBucket,
		LegacyGlueS3Bucket: DatabricksWarehouseBucket,
		OwnerTeam:          team.DataPlatform,
		DatabaseGroup:      ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:               "kinesisstats_history",
		Bucket:             KinesisstatsDeltaLakeBucket,
		LegacyGlueS3Bucket: DatabricksWarehouseBucket,
		OwnerTeam:          team.DataPlatform,
		DatabaseGroup:      ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-supplychain-ops":        {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":            {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "kinesisstats_window",
		Bucket:        DataPipelinesDeltaLakeBucket,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		SkipGlueCatalog: true,
		CanReadGroups:   clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-techsupport":       {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "ksdifftool",
		Bucket:        LegacyDatabricksPlaygroundBucket_DONOTUSE,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: PlaygroundDatabaseGroup_LEGACY,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:          "messagesdb_hashed",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: []components.TeamInfo{
			team.MLScience,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.DataPlatform,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "multicam",
		Bucket:        LegacyDatabricksPlaygroundBucket_DONOTUSE,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: PlaygroundDatabaseGroup_LEGACY,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:          "objectstat_diffs",
		Bucket:        DataPipelinesDeltaLakeBucket,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		SkipGlueCatalog: true,
		CanReadGroups:   clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		// Database for query agent logging. All granted teams (CursorQueryAgentTeams)
		// have READWRITE access, but column masking on the payload column prevents
		// most teams from reading sensitive query data. Only DataPlatform/DataTools/
		// DataEngineering can read the full payload for analysis.
		Name:          "query_agents",
		Bucket:        QueryAgentsBucket,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		SkipGlueCatalog: true,
		// All CursorQueryAgentTeams have READWRITE access (includes DataPlatform,
		// DataTools, DataEngineering). Column masking on payload protects sensitive data.
		CanReadWriteGroups: dataplatformterraformconsts.CursorQueryAgentTeams(),
	},
	{
		Name:          "report_staging",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":              {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":        {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:               "s3bigstats",
		Bucket:             S3BigStatsDeltaLakeBucket,
		LegacyGlueS3Bucket: DatabricksWarehouseBucket,
		OwnerTeam:          team.DataPlatform,
		DatabaseGroup:      ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-techsupport": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:               "s3bigstats_history",
		Bucket:             S3BigStatsDeltaLakeBucket,
		LegacyGlueS3Bucket: DatabricksWarehouseBucket,
		OwnerTeam:          team.DataPlatform,
		DatabaseGroup:      ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "s3inventories",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:          "s3inventories_delta",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "s3inventory",
		Bucket:        S3InventoryBucket,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
		},
		SkipGlueCatalog: true,
		CanReadGroups:   clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers":   {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":        {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "s3logs",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "stateramp",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.DataPlatform,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers":   {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
}

func GetEmrDeltaLakeDbs(config dataplatformconfig.DatabricksConfig) []SamsaraDB {
	var emrDbs []SamsaraDB

	for _, bucketName := range emrreplicationproject.GetAllEmrReplicationDeltaLakeBucketNames(config) {
		// get the cell name which is the last token of the bucket name after the last hyphen
		bucketNameParts := strings.Split(bucketName, "-")
		cellName := bucketNameParts[len(bucketNameParts)-1]
		emrDbs = append(emrDbs, SamsaraDB{
			Name:          fmt.Sprintf("emr_%s", cellName),
			Bucket:        bucketName,
			OwnerTeam:     team.DataPlatform,
			DatabaseGroup: EmrDatabaseGroup,
			AWSAccountIDs: []string{
				DatabricksAWSAccountUS,
				DatabricksAWSAccountEU,
			},
			SkipGlueCatalog: true,
			CanReadGroups:   clusterhelpers.DatabricksClusterTeams(),
			CanReadBiztechGroups: map[string][]string{
				"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
			},
		})
	}

	return emrDbs
}

func DataPlatformDatabases(config dataplatformconfig.DatabricksConfig) []SamsaraDB {
	return append(DataPlatformDatabaseRegistry, GetEmrDeltaLakeDbs(config)...)
}
