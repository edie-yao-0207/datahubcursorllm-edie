package ksdeltalake

import (
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/ksdeltalake"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformprojecthelpers"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
)

func kinesisStatsVacuumPipeline(config dataplatformconfig.DatabricksConfig) ([]tf.Resource, error) {
	tables := ksdeltalake.AllTables()
	var s3Paths []string
	for _, table := range tables {
		s3Paths = append(s3Paths, ksdeltalake.TableLocation(config.Region, ksdeltalake.TableTypeDeduplicated, table.Name))
	}
	if len(s3Paths) == 0 {
		return nil, nil
	}

	retentionDays := int64(30)
	timeoutHours := int(1)

	az := dataplatformresource.JobNameToAZ(config.Region, "kinesisstats-vacuum-job")
	driverPoolIdentifier := getPoolIdentifiers(config.Region, false)[az].DriverPoolIdentifier
	resources, err := dataplatformprojecthelpers.CreateVacuumResources(
		s3Paths, config, "kinesisstats-vacuum",
		dataplatformprojecthelpers.PoolArguments{
			DriverPool: dataplatformprojecthelpers.IndividualPoolArguments{
				PoolName: driverPoolIdentifier,
			},
		},
		dataplatformconsts.KinesisStatsDeltaLakeIngestionVacuum, retentionDays, timeoutHours)
	if err != nil {
		return nil, oops.Wrapf(err, "error creating vacuum jobs")
	}

	return resources, nil
}
