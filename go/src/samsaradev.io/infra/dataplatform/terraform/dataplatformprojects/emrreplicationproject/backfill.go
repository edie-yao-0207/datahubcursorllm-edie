package emrreplicationproject

import (
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/emrreplication/emrhelpers"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/team"
)

const EmrReplicationBackfillKinesisDataStreams = "emr_replication_backfill_kinesis_data_streams"

var backfillTags = map[string]string{
	"samsara:service":       "emr_replication_backfill",
	"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
	"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
}

// Generate KMS key for backfill kinesis data streams - shared across all cells.
var BackfillKinesisKey = &awsresource.KMSKey{
	ResourceName:      KmsKeyName(EmrReplicationBackfillKinesisDataStreams),
	Description:       "KMS key for EMR replication backfill kinesis data streams",
	EnableKeyRotation: true,
	Tags:              backfillTags,
}

func emrBackfillProject(providerGroup string) (*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	resourceGroups := make(map[string][]tf.Resource)

	cells, err := emrhelpers.GetAllCellsPerRegion(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "getting cells for region %s", config.Region)
	}

	// Create Kinesis streams for each cell.
	kinesisDataStreamsResources, err := createKinesisDataStreamsForStreamType(config, cells, streamTypeBackfill)
	if err != nil {
		return nil, oops.Wrapf(err, "Failed to generate resources for Kinesis Data Streams")
	}
	resourceGroups[EmrReplicationBackfillKinesisDataStreams] = kinesisDataStreamsResources

	// Create Firehose delivery streams for each cell.
	firehoseResources, err := createKinesisFirehoseDeliveryForStreamType(config, cells, streamTypeBackfill)
	if err != nil {
		return nil, oops.Wrapf(err, "Failed to generate resources for Firehose Delivery Streams")
	}
	resourceGroups[EmrReplicationBackfillKinesisDataStreams+"_firehose_delivery"] = firehoseResources

	return &project.Project{
		RootTeam:        dataplatformterraformconsts.DataPlatformEmrReplicationProjectPipeline,
		Provider:        config.AWSProviderGroup,
		Class:           "emr_replication",
		Name:            "emr_replication_backfill",
		GenerateOutputs: true,
		ResourceGroups:  resourceGroups,
	}, nil
}
