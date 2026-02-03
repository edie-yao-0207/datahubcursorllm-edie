package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var EpoFinanceDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "epofinance_dev",
		Bucket:        DatabricksWorkspaceBucket,
		OwnerTeam:     team.EpoFinance,
		DatabaseGroup: TeamDevDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
			team.EpoFinance,
		},
	},
	{
		Name:          "epofinance_prod",
		Bucket:        EpofinanceProdBucket,
		OwnerTeam:     team.EpoFinance,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
			team.EpoFinance,
		},
	},
}
