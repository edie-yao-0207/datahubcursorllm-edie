package unitycatalog

import (
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	teamComponents "samsaradev.io/team/components"
)

const (
	EuMetastoreId = "aws:eu-west-1:fecf0cfb-bf1e-43a3-8803-b536ba03a2ca"
	UsMetastoreId = "aws:us-west-2:a4ada860-2122-418f-ae38-e374e756bc04"
	CaMetastoreId = "aws:ca-central-1:175be941-32cd-46ef-9229-1f986aefe4b1"
)

type Recipient struct {
	Name   string
	Region string

	Owner teamComponents.TeamInfo

	AuthenticationType             string
	DataRecipientGlobalMetastoreId string
	PropertiesKvPairs              []RecipientProperties
}

type RecipientProperties struct {
	Properties map[string]string
}

func (r Recipient) InRegion(region string) bool {
	return r.Region == region
}

var RecipientRegistry = []Recipient{
	{
		Name:                           "databricks_eu",
		Region:                         infraconsts.SamsaraAWSDefaultRegion,
		Owner:                          team.DataPlatform,
		AuthenticationType:             "DATABRICKS",
		DataRecipientGlobalMetastoreId: EuMetastoreId,
	},
	{
		Name:                           "databricks_us",
		Region:                         infraconsts.SamsaraAWSEURegion,
		Owner:                          team.DataPlatform,
		AuthenticationType:             "DATABRICKS",
		DataRecipientGlobalMetastoreId: UsMetastoreId,
	},
	{
		Name:                           "databricks_ca",
		Region:                         infraconsts.SamsaraAWSDefaultRegion,
		Owner:                          team.DataPlatform,
		AuthenticationType:             "DATABRICKS",
		DataRecipientGlobalMetastoreId: CaMetastoreId,
	},
	{
		Name:                           "databricks_ca_us",
		Region:                         infraconsts.SamsaraAWSCARegion,
		Owner:                          team.DataPlatform,
		AuthenticationType:             "DATABRICKS",
		DataRecipientGlobalMetastoreId: UsMetastoreId,
	},
}
