package dynamodbdeltalake

import (
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/dynamodbdeltalake"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformprojecthelpers"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
)

func dynamodVacuumPipeline(config dataplatformconfig.DatabricksConfig) ([]tf.Resource, error) {
	tables := dynamodbdeltalake.AllTables()
	var s3Paths []string
	for _, table := range tables {
		s3Paths = append(s3Paths, dynamoDbTableDeltaTablePath(awsregionconsts.RegionPrefix[config.Region], table.TableName))
	}

	retentionDays := int64(7)
	timeoutHours := int(1)

	az := dataplatformresource.JobNameToAZ(config.Region, "dynamodb-vacuum-job")
	driverPoolIdentifier := getPoolIdentifiers(config.Region, false)[az].DriverPoolIdentifier
	resources, err := dataplatformprojecthelpers.CreateVacuumResources(
		s3Paths, config, "dynamodb-vacuum",
		dataplatformprojecthelpers.PoolArguments{
			DriverPool: dataplatformprojecthelpers.IndividualPoolArguments{
				PoolName: driverPoolIdentifier,
			},
		},
		dataplatformconsts.DynamoDbDeltaLakeIngestionVacuum, retentionDays, timeoutHours)
	if err != nil {
		return nil, oops.Wrapf(err, "error creating vacuum jobs")
	}

	return resources, nil
}
