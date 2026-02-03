package dataplatformconfig

import (
	"github.com/samsarahq/go/oops"

	"samsaradev.io/libs/ni/infraconsts"
)

const DatabricksAccountId = "f8e9c6b3-6083-4e24-bf92-19bbd19235e3"
const DatabricksTestAccountId = "c1048651-c62c-4662-89dd-dc8fb3d1cb00"
const DatabricksAWSAccount = "414351767826"

// The virtual source address of our custom databricks provider which is used as a prefix for local filesystem installation
// Internal version number for the databricks provider
// See tf 0.13+ documentation: https://developer.hashicorp.com/terraform/language/v1.1.x/providers/requirements#in-house-providers
const SamsaraDatabricksProviderSource = "registry.samsara.com/samsara/databricks"
const SamsaraDatabricksProviderVersion = "1.0.0"

// The source address of the official databricks terraform provider and its version.
// See official reference documentation: https://registry.terraform.io/providers/databricks/databricks/latest/docs
const OfficialDatabricksProviderSource = "databricks/databricks"
const OfficialDatabricksProviderVersion = "1.84.0"

// We authenticate Databricks providers differently depending on whether they need workspace or account level access.
const DatabricksWorkspaceProviderAuthTypeToken = "pat"
const DatabricksAccountProviderAuthTypeOAuth = "oauth-m2m"

// Note: Infra Plat has a slice called DONOTUSE_AllRegions defined in
// go/src/samsaradev.io/libs/ni/infraconsts/cells.go, but it does not contain the
// Canada region yet since it is not productionized. Once it is fully rolled
// out, we should deprecate this slice in favor of what is defined in the
// infraconsts package.
var AllRegions = []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion}

type MachineUserConfig struct {
	Name  string
	Email string
}

type DatabricksMachineUserType string

const (
	DatabricksUsCIMachineUser = "dev-databricks-ci@samsara.com"
	DatabricksEuCIMachineUser = "dev-databricks-eu-ci@samsara.com"
	DatabricksCaCIMachineUser = "dev-databricks-ca-ci@samsara.com"

	DatabricksUsDataPipelinesLambdaMachineUser = "dev-databricks-datapipelines-lambda@samsara.com"
	DatabricksEuDataPipelinesLambdaMachineUser = "dev-databricks-datapipelines-eu-lambda@samsara.com"
	DatabricksCaDataPipelinesLambdaMachineUser = "dev-databricks-datapipelines-ca-lambda@samsara.com"

	DatabricksUsMetricsCIMachineUser = "dev-databricks-metrics@samsara.com"
	DatabricksEuMetricsCIMachineUser = "dev-databricks-eu-metrics@samsara.com"
	DatabricksCaMetricsCIMachineUser = "dev-databricks-ca-metrics@samsara.com"

	DatabricksUsCIServicePrincipalAppId = "790f7c2e-de70-456f-bca8-9f8276555a38"
	DatabricksEuCIServicePrincipalAppId = "bc1bfa85-1b1e-4990-b63c-1a49fb9053a0"
	DatabricksCaCIServicePrincipalAppId = "9ac897a0-c4dc-4d46-919f-9bb66ff1915f"

	DatabricksUsDataPipelinesServicePrincipalAppId = "94266402-8dc8-44d7-8a4c-1439cb051aac"
	DatabricksEuDataPipelinesServicePrincipalAppId = "bea7b88b-63d5-4721-aa4e-2b33425560a8"
	DatabricksCaDataPipelinesServicePrincipalAppId = "4c7141eb-d153-4670-b06e-186b94d53c03"

	DatabricksDevUsProviderGroup = "databricks-dev-us"
	DatabricksDevEuProviderGroup = "databricks-dev-eu"
	DatabricksDevCaProviderGroup = "databricks-dev-ca"

	DatabricksMachineUserDagster             DatabricksMachineUserType = "DAGSTER"
	DatabricksMachineUserDbtProd             DatabricksMachineUserType = "DBT_PROD"
	DatabricksMachineUserDbtCi               DatabricksMachineUserType = "DBT_CI"
	DatabricksMachineUserSiem                DatabricksMachineUserType = "SIEM"
	DatabricksMachineUserCi                  DatabricksMachineUserType = "CI"
	DatabricksMachineUserDatapipelinesLambda DatabricksMachineUserType = "DATAPIPELINES_LAMBDA"
	DatabricksMachineUserMetrics             DatabricksMachineUserType = "METRICS"
	DatabricksMachineUserProdGql             DatabricksMachineUserType = "PROD_GQL"
	DatabricksMachineUserMatik               DatabricksMachineUserType = "MATIK"
	DatabricksMachineUserBiztech             DatabricksMachineUserType = "BIZTECH"
	DatabricksMachineUserSafetyAirflow       DatabricksMachineUserType = "SAFETY_AIRFLOW_DATABRICKS"
)

type DatabricksMachineUsers struct {
	CI                 MachineUserConfig
	DataPipelineLambda MachineUserConfig
	Metrics            MachineUserConfig
	Fivetran           MachineUserConfig
	ProdGQL            MachineUserConfig
	DataStreams        MachineUserConfig
	Matik              MachineUserConfig
	BizTech            MachineUserConfig
	DbtProd            MachineUserConfig
	DbtCi              MachineUserConfig
	Dagster            MachineUserConfig
	Siem               MachineUserConfig
}

type DatabricksConfig struct {
	DatabricksProviderGroup string
	AWSProviderGroup        string
	ExternalId              string
	Hostname                string
	DatabricksVPCId         string
	ProdVPCId               string
	AWSAccountId            int
	DatabricksAWSAccountId  int
	RDSClusterIdentifier    string
	Region                  string
	MachineUsers            DatabricksMachineUsers
	LegacyWorkspace         bool
	Cloud                   infraconsts.SamsaraCloud // TODO: replace legacy fields (account ids, region) by referencing the cloud struct.
}

var allConfigs = []DatabricksConfig{
	DatabricksConfig{
		DatabricksProviderGroup: "databricks-us",
		AWSProviderGroup:        "aws-us",
		ExternalId:              "761c0344-da85-4184-9309-0232f2526573",
		Hostname:                "https://samsara.cloud.databricks.com",
		DatabricksVPCId:         "vpc-0709ec4d1c1173f18",
		// VPC Ids are created by databricks inside of our AWS account and can
		// be read via the AWS console.
		ProdVPCId:              infraconsts.USProdVPCID,
		AWSAccountId:           infraconsts.SamsaraAWSAccountID,
		DatabricksAWSAccountId: infraconsts.SamsaraAWSDatabricksAccountID,
		RDSClusterIdentifier:   "cluster-c7ihh7zjlmiq",
		Region:                 infraconsts.SamsaraAWSDefaultRegion,
		MachineUsers: DatabricksMachineUsers{
			CI: MachineUserConfig{
				Name:  "CI",
				Email: DatabricksUsCIMachineUser,
			},
			DataPipelineLambda: MachineUserConfig{
				Name:  "Data Pipeline Lambdas",
				Email: "dev-databricks-datapipelines-lambda@samsara.com",
			},
			Metrics: MachineUserConfig{
				Name:  "Metrics",
				Email: "dev-databricks-metrics@samsara.com",
			},
		},
		LegacyWorkspace: true,
		Cloud:           infraconsts.SamsaraClouds.USProd,
	},
	DatabricksConfig{
		DatabricksProviderGroup: "databricks-eu",
		AWSProviderGroup:        "aws-eu",
		ExternalId:              "e946a037-2055-4b60-ae3b-02a68de38bf2",
		Hostname:                "https://samsara-eu.cloud.databricks.com",
		DatabricksVPCId:         "vpc-09decf8c9b3c36479",
		// VPC Ids are created by databricks inside of our AWS account and can
		// be read via the AWS console.
		ProdVPCId:              "vpc-0e8a900150d6621be",
		AWSAccountId:           infraconsts.SamsaraAWSEUAccountID,
		DatabricksAWSAccountId: infraconsts.SamsaraAWSEUDatabricksAccountID,
		RDSClusterIdentifier:   "cluster-coowdxddbjcc",
		Region:                 infraconsts.SamsaraAWSEURegion,
		MachineUsers: DatabricksMachineUsers{
			CI: MachineUserConfig{
				Name:  "CI",
				Email: DatabricksEuCIMachineUser,
			},
			DataPipelineLambda: MachineUserConfig{
				Name:  "Data Pipeline Lambdas",
				Email: "dev-databricks-datapipelines-eu-lambda@samsara.com",
			},
			Metrics: MachineUserConfig{
				Name:  "Metrics",
				Email: "dev-databricks-eu-metrics@samsara.com",
			},
		},
		LegacyWorkspace: true,
		Cloud:           infraconsts.SamsaraClouds.EUProd,
	},
	DatabricksConfig{
		DatabricksProviderGroup: "databricks-ca",
		AWSProviderGroup:        "aws-ca",
		ExternalId:              "f8e9c6b3-6083-4e24-bf92-19bbd19235e3",
		Hostname:                infraconsts.SamsaraCADevDatabricksWorkspaceURLBase,
		// VPC Ids are created by databricks inside of our AWS account and can
		// be read via the AWS console.
		ProdVPCId:              "vpc-01dada81f5c7efb54",
		AWSAccountId:           infraconsts.SamsaraAWSCAAccountID,
		DatabricksAWSAccountId: infraconsts.SamsaraAWSCADatabricksAccountID,
		RDSClusterIdentifier:   "cluster-cnyka2s60k85",
		Region:                 infraconsts.SamsaraAWSCARegion,
		MachineUsers: DatabricksMachineUsers{
			CI: MachineUserConfig{
				Name:  "CI",
				Email: DatabricksCaCIMachineUser,
			},
			DataPipelineLambda: MachineUserConfig{
				Name:  "Data Pipeline Lambdas",
				Email: "dev-databricks-datapipelines-ca-lambda@samsara.com",
			},
			Metrics: MachineUserConfig{
				Name:  "Metrics",
				Email: "dev-databricks-ca-metrics@samsara.com",
			},
		},
		LegacyWorkspace: true,
		Cloud:           infraconsts.SamsaraClouds.CAProd,
	},
	DatabricksConfig{
		DatabricksProviderGroup: DatabricksDevUsProviderGroup,
		AWSProviderGroup:        "aws-us",
		Hostname:                infraconsts.SamsaraDevDatabricksWorkspaceURLBase,
		DatabricksVPCId:         "vpc-01320310ca6e595d7",
		// VPC Ids are created by databricks inside of our AWS account and can
		// be read via the AWS console.
		AWSAccountId:           infraconsts.SamsaraAWSAccountID,
		DatabricksAWSAccountId: infraconsts.SamsaraAWSDatabricksAccountID,
		RDSClusterIdentifier:   "cluster-c7ihh7zjlmiq",
		Region:                 infraconsts.SamsaraAWSDefaultRegion,
		MachineUsers: DatabricksMachineUsers{
			CI: MachineUserConfig{
				Name:  "CI",
				Email: DatabricksUsCIMachineUser,
			},
			DataPipelineLambda: MachineUserConfig{
				Name:  "Data Pipeline Lambdas",
				Email: "dev-databricks-datapipelines-lambda@samsara.com",
			},
			Metrics: MachineUserConfig{
				Name:  "Metrics",
				Email: "dev-databricks-metrics@samsara.com",
			},
			Fivetran: MachineUserConfig{
				Name:  "Fivetran",
				Email: "dev-databricks-fivetran@samsara.com",
			},
			// Used to talk to databricks from GQL in the Samsara prod AWS account
			ProdGQL: MachineUserConfig{
				Name:  "Prod GQL",
				Email: "dev-databricks-prod-gql@samsara.com",
			},
			Matik: MachineUserConfig{
				Name:  "Matik",
				Email: "dev-databricks-matik@samsara.com",
			},
			BizTech: MachineUserConfig{
				Name:  "BizTech",
				Email: "dev-databricks-bted-prod@samsara.com",
			},
			DbtCi: MachineUserConfig{
				Name:  "DBT CI",
				Email: "dev-databricks-dbt-ci@samsara.com",
			},
			DbtProd: MachineUserConfig{
				Name:  "DBT Prod",
				Email: "dev-databricks-dbt-prod@samsara.com",
			},
			Siem: MachineUserConfig{
				Name:  "Siem User",
				Email: "dev-databricks-siem@samsara.com",
			},
		},
		Cloud: infraconsts.SamsaraClouds.USProd,
	},
	DatabricksConfig{
		DatabricksProviderGroup: DatabricksDevEuProviderGroup,
		AWSProviderGroup:        "aws-eu",
		Hostname:                infraconsts.SamsaraEUDevDatabricksWorkspaceURLBase,
		DatabricksVPCId:         "vpc-05dded5d60496a993",
		// VPC Ids are created by databricks inside of our AWS account and can
		// be read via the AWS console.
		AWSAccountId:           infraconsts.SamsaraAWSEUAccountID,
		DatabricksAWSAccountId: infraconsts.SamsaraAWSEUDatabricksAccountID,
		RDSClusterIdentifier:   "cluster-coowdxddbjcc",
		Region:                 infraconsts.SamsaraAWSEURegion,
		MachineUsers: DatabricksMachineUsers{
			CI: MachineUserConfig{
				Name:  "CI",
				Email: DatabricksEuCIMachineUser,
			},
			DataPipelineLambda: MachineUserConfig{
				Name:  "Data Pipeline Lambdas",
				Email: "dev-databricks-datapipelines-eu-lambda@samsara.com",
			},
			Metrics: MachineUserConfig{
				Name:  "Metrics",
				Email: "dev-databricks-eu-metrics@samsara.com",
			},
			// Used to talk to databricks from GQL in the Samsara prod EU AWS account
			ProdGQL: MachineUserConfig{
				Name:  "Prod GQL",
				Email: "dev-databricks-eu-prod-gql@samsara.com",
			},
			BizTech: MachineUserConfig{
				Name:  "BizTech",
				Email: "dev-databricks-eu-bted-prod@samsara.com",
			},
			DbtCi: MachineUserConfig{
				Name:  "dbt-ci",
				Email: "dev-databricks-eu-dbt-ci@samsara.com",
			},
			DbtProd: MachineUserConfig{
				Name:  "dbt-prod",
				Email: "dev-databricks-eu-dbt-prod@samsara.com",
			},
			Siem: MachineUserConfig{
				Name:  "Siem User",
				Email: "dev-databricks-eu-siem@samsara.com",
			},
		},
		Cloud: infraconsts.SamsaraClouds.EUProd,
	},
	DatabricksConfig{
		DatabricksProviderGroup: DatabricksDevCaProviderGroup,
		AWSProviderGroup:        "aws-ca",
		Hostname:                infraconsts.SamsaraCADevDatabricksWorkspaceURLBase,
		DatabricksVPCId:         "vpc-01dada81f5c7efb54",
		// VPC Ids are created by databricks inside of our AWS account and can
		// be read via the AWS console.
		AWSAccountId:           infraconsts.SamsaraAWSCAAccountID,
		DatabricksAWSAccountId: infraconsts.SamsaraAWSCADatabricksAccountID,
		RDSClusterIdentifier:   "cluster-cnyka2s60k85",
		Region:                 infraconsts.SamsaraAWSCARegion,
		MachineUsers: DatabricksMachineUsers{
			CI: MachineUserConfig{
				Name:  "CI",
				Email: DatabricksCaCIMachineUser,
			},
			DataPipelineLambda: MachineUserConfig{
				Name:  "Data Pipeline Lambdas",
				Email: "dev-databricks-datapipelines-ca-lambda@samsara.com",
			},
			Metrics: MachineUserConfig{
				Name:  "Metrics",
				Email: "dev-databricks-ca-metrics@samsara.com",
			},
			// Used to talk to databricks from GQL in the Samsara prod EU AWS account
			ProdGQL: MachineUserConfig{
				Name:  "Prod GQL",
				Email: "dev-databricks-ca-prod-gql@samsara.com",
			},
			BizTech: MachineUserConfig{
				Name:  "BizTech",
				Email: "dev-databricks-ca-bted-prod@samsara.com",
			},
			DbtCi: MachineUserConfig{
				Name:  "dbt-ci",
				Email: "dev-databricks-ca-dbt-ci@samsara.com",
			},
			DbtProd: MachineUserConfig{
				Name:  "dbt-prod",
				Email: "dev-databricks-ca-dbt-prod@samsara.com",
			},
			Siem: MachineUserConfig{
				Name:  "Siem User",
				Email: "dev-databricks-ca-siem@samsara.com",
			},
		},
		Cloud: infraconsts.SamsaraClouds.CAProd,
	},
	DatabricksConfig{
		DatabricksProviderGroup: "databricks-prod-us",
		AWSProviderGroup:        "aws-us",
		Hostname:                infraconsts.SamsaraUSProdDatabricksWorkspaceURLBase,
		AWSAccountId:            infraconsts.SamsaraAWSAccountID,
		DatabricksAWSAccountId:  infraconsts.SamsaraAWSDatabricksAccountID,
		Region:                  infraconsts.SamsaraAWSDefaultRegion,
		MachineUsers: DatabricksMachineUsers{
			CI: MachineUserConfig{
				Name:  "CI",
				Email: "prod-databricks-ci@samsara.com",
			},
		},
		Cloud: infraconsts.SamsaraClouds.USProd,
	},
	DatabricksConfig{
		DatabricksProviderGroup: "databricks-account",
		Hostname:                "https://accounts.cloud.databricks.com",
		Region:                  infraconsts.SamsaraAWSDefaultRegion,
	},
}

var providerGroupConfigs map[string]DatabricksConfig

func init() {
	providerGroupConfigs = make(map[string]DatabricksConfig)
	for _, config := range allConfigs {
		providerGroupConfigs[config.DatabricksProviderGroup] = config
	}
}

func DatabricksProviderConfig(providerGroup string) (DatabricksConfig, error) {
	config, ok := providerGroupConfigs[providerGroup]
	if !ok {
		return DatabricksConfig{}, oops.Errorf("Could not find config for provider group %s", providerGroup)
	}
	return config, nil
}

func GetCIUserByRegion(region string) (string, error) {
	switch region {
	case infraconsts.SamsaraAWSDefaultRegion:
		return DatabricksUsCIMachineUser, nil
	case infraconsts.SamsaraAWSEURegion:
		return DatabricksEuCIMachineUser, nil
	case infraconsts.SamsaraAWSCARegion:
		return DatabricksCaCIMachineUser, nil
	}
	return "", oops.Errorf("no job owner for region %s", region)
}

func GetCIServicePrincipalAppIdByRegion(region string) (string, error) {
	switch region {
	case infraconsts.SamsaraAWSDefaultRegion:
		return DatabricksUsCIServicePrincipalAppId, nil
	case infraconsts.SamsaraAWSEURegion:
		return DatabricksEuCIServicePrincipalAppId, nil
	case infraconsts.SamsaraAWSCARegion:
		return DatabricksCaCIServicePrincipalAppId, nil
	}
	return "", oops.Errorf("no CI service principal app id for region %s", region)
}

func GetMetricsCIUserByRegion(region string) (string, error) {
	switch region {
	case infraconsts.SamsaraAWSDefaultRegion:
		return DatabricksUsMetricsCIMachineUser, nil
	case infraconsts.SamsaraAWSEURegion:
		return DatabricksEuMetricsCIMachineUser, nil
	case infraconsts.SamsaraAWSCARegion:
		return DatabricksCaMetricsCIMachineUser, nil
	}
	return "", oops.Errorf("no job owner for region %s", region)
}

func GetDataPipelineLambdaUserByRegion(region string) (string, error) {
	switch region {
	case infraconsts.SamsaraAWSDefaultRegion:
		return DatabricksUsDataPipelinesLambdaMachineUser, nil
	case infraconsts.SamsaraAWSEURegion:
		return DatabricksEuDataPipelinesLambdaMachineUser, nil
	case infraconsts.SamsaraAWSCARegion:
		return DatabricksCaDataPipelinesLambdaMachineUser, nil
	}
	return "", oops.Errorf("no data pipeline lambda user for region %s", region)
}

func GetDataPipelineServicePrincipalAppIdByRegion(region string) (string, error) {
	switch region {
	case infraconsts.SamsaraAWSDefaultRegion:
		return DatabricksUsDataPipelinesServicePrincipalAppId, nil
	case infraconsts.SamsaraAWSEURegion:
		return DatabricksEuDataPipelinesServicePrincipalAppId, nil
	case infraconsts.SamsaraAWSCARegion:
		return DatabricksCaDataPipelinesServicePrincipalAppId, nil
	}
	return "", oops.Errorf("no data pipeline service principal app id for region %s", region)
}

func DatabricksE2WorkspacesMachineUsersOfType(machineUserType DatabricksMachineUserType) (machineUsers []MachineUserConfig) {
	for _, c := range databricksE2WorkspaceProviderConfigs() {
		if machineUserType == DatabricksMachineUserDagster && c.MachineUsers.Dagster.Email != "" {
			machineUsers = append(machineUsers, c.MachineUsers.Dagster)
		} else if machineUserType == DatabricksMachineUserDbtCi && c.MachineUsers.DbtCi.Email != "" {
			machineUsers = append(machineUsers, c.MachineUsers.DbtCi)
		} else if machineUserType == DatabricksMachineUserDbtProd && c.MachineUsers.DbtProd.Email != "" {
			machineUsers = append(machineUsers, c.MachineUsers.DbtProd)
		} else if machineUserType == DatabricksMachineUserSiem && c.MachineUsers.Siem.Email != "" {
			machineUsers = append(machineUsers, c.MachineUsers.Siem)
		} else if machineUserType == DatabricksMachineUserCi && c.MachineUsers.CI.Email != "" {
			machineUsers = append(machineUsers, c.MachineUsers.CI)
		} else if machineUserType == DatabricksMachineUserDatapipelinesLambda && c.MachineUsers.DataPipelineLambda.Email != "" {
			machineUsers = append(machineUsers, c.MachineUsers.DataPipelineLambda)
		} else if machineUserType == DatabricksMachineUserMetrics && c.MachineUsers.Metrics.Email != "" {
			machineUsers = append(machineUsers, c.MachineUsers.Metrics)
		} else if machineUserType == DatabricksMachineUserProdGql && c.MachineUsers.ProdGQL.Email != "" {
			machineUsers = append(machineUsers, c.MachineUsers.ProdGQL)
		} else if machineUserType == DatabricksMachineUserMatik && c.MachineUsers.Matik.Email != "" {
			machineUsers = append(machineUsers, c.MachineUsers.Matik)
		} else if machineUserType == DatabricksMachineUserBiztech && c.MachineUsers.BizTech.Email != "" {
			machineUsers = append(machineUsers, c.MachineUsers.BizTech)
		}
	}
	return machineUsers
}

func databricksE2WorkspaceProviderConfigs() (providerConfigs []DatabricksConfig) {
	for _, c := range allConfigs {
		if c.DatabricksProviderGroup == DatabricksDevUsProviderGroup ||
			c.DatabricksProviderGroup == DatabricksDevEuProviderGroup ||
			c.DatabricksProviderGroup == DatabricksDevCaProviderGroup {
			providerConfigs = append(providerConfigs, c)
		}
	}
	return providerConfigs
}

func IsE2WorkspaceProviderGroup(providerGroup string) bool {
	return providerGroup == DatabricksDevUsProviderGroup ||
		providerGroup == DatabricksDevEuProviderGroup ||
		providerGroup == DatabricksDevCaProviderGroup
}
