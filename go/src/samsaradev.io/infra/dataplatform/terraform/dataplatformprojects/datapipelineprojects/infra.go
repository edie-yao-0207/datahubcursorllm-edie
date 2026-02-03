package datapipelineprojects

import (
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/definitions/vpc"
	"samsaradev.io/infra/dataplatform/dataplatformhelpers"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
)

func dataPipelinesVPC(region string) (*vpc.VPCResources, error) {

	az := []string{}
	var err error

	if region == infraconsts.SamsaraAWSCARegion {

		azs := infraconsts.GetDatabricksAvailabilityZones(region)
		if len(azs) == 0 {
			return nil, oops.Errorf("%s", "No Databricks AZ found for region: "+region)
		}
		az, err = dataplatformhelpers.GetSupportedAzs(azs)

		if err != nil {
			return nil, oops.Errorf("Error getting Az for region %s", region)
		}
	}

	pipelineLambdaVPC, err := vpc.DONOTUSE_DeprecatedDefine(vpc.VPCConfig{
		Environment:          "datapipelines-lambdas",
		Region:               region,
		Cidr16:               16,
		CreatePrivateSubnets: true,
		Tags:                 vpcTags(),
		Azs:                  az,
	})
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return pipelineLambdaVPC, nil
}

func vpcTags() []*awsresource.Tag {
	return []*awsresource.Tag{
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
			Value: "data-pipeline-lambdas-vpc",
		},
	}
}
