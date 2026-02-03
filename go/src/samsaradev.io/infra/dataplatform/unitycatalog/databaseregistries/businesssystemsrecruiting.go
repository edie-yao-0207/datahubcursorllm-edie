package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var BusinessSystemsRecruitingDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "_fivetran_setup_test",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.BusinessSystemsRecruiting,
		DatabaseGroup: FivetranDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataApplication,
			team.BizTechEnterpriseDataSensitive,
			team.BusinessSystems,
			team.BusinessSystemsRecruiting,
			team.DataScience,
			team.DevEcosystem,
			team.OPX,
			team.PlatformOperations,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-techsupport": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "_fivetran_staging",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.BusinessSystemsRecruiting,
		DatabaseGroup: FivetranDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataApplication,
			team.BizTechEnterpriseDataSensitive,
			team.BusinessSystems,
			team.BusinessSystemsRecruiting,
			team.DataScience,
			team.DevEcosystem,
			team.OPX,
			team.PlatformOperations,
		},
	},
	{
		Name:          "fivetran_log",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.BusinessSystemsRecruiting,
		DatabaseGroup: FivetranDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataApplication,
			team.BizTechEnterpriseDataSensitive,
			team.BusinessSystems,
			team.BusinessSystemsRecruiting,
			team.DataScience,
			team.DevEcosystem,
			team.OPX,
			team.PlatformOperations,
		},
	},
	{
		Name:          "people_analytics",
		Bucket:        FivetranGreenhouseRecruitingDeltaTablesBucket,
		OwnerTeam:     team.BusinessSystemsRecruiting,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BusinessSystemsRecruiting,
		},
	},
}
