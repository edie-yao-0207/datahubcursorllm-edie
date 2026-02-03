package rdslakeprojects

import (
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/rdsdeltalake"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/service/dbregistry"
	"samsaradev.io/team"
)

const KinesisStreamNamePrefix = "rds-kinesis-stream"
const KinesisStreamKeyNamePrefix = "rds-kinesis-kms-key"

// KinesisStreamName returns the name of the Kinesis Data Stream for a database
func KinesisStreamName(dbName string) string {
	return fmt.Sprintf("%s-%s", KinesisStreamNamePrefix, dbName)
}

// createKinesisStreamForDatabase creates a single Kinesis Data Stream for all shards of a database
func createKinesisStreamForDatabase(config dataplatformconfig.DatabricksConfig, db rdsdeltalake.RegistryDatabase) ([]tf.Resource, *awsresource.KinesisStream, error) {
	var resources []tf.Resource

	// Use first shard to determine metadata (all shards belong to same database)
	shards := db.RegionToShards[config.Region]
	if len(shards) == 0 {
		return nil, nil, oops.Errorf("database %s has no shards in region %s", db.MySqlDb, config.Region)
	}

	firstShard := shards[0]
	dbName := getDbName(db.MySqlDb, firstShard)
	teamOwner, ok := dbregistry.DbToTeam[strings.TrimSuffix(dbName, "db")]
	if !ok {
		return nil, nil, oops.Errorf("Cannot find owner of this database: %s", dbName)
	}

	hasProduction := db.HasProductionTable()
	rndAllocation := "1"
	if hasProduction {
		rndAllocation = "0"
	}

	serviceTag := fmt.Sprintf("rds-kinesis-stream-%s", dbName)

	tags := map[string]string{
		"samsara:service":        serviceTag,
		"samsara:team":           strings.ToLower(teamOwner.Name()),
		"samsara:product-group":  strings.ToLower(team.TeamProductGroup[teamOwner.Name()]),
		"samsara:rnd-allocation": rndAllocation,
		"samsara:production":     fmt.Sprintf("%v", hasProduction),
		"owner":                  strings.ToLower(team.DataPlatform.TeamName),
	}

	// Create a KMS key for the Kinesis stream
	kmsKey := &awsresource.KMSKey{
		ResourceName:      fmt.Sprintf("%s-%s", KinesisStreamKeyNamePrefix, dbName),
		Description:       fmt.Sprintf("CMK used for encrypting Kinesis Data Stream for RDS replication of %s (all shards)", dbName),
		EnableKeyRotation: true,
		Tags:              tags,
	}
	resources = append(resources, kmsKey)

	// Create KMS alias for easier reference
	kmsAlias := &awsresource.KMSAlias{
		Name:        fmt.Sprintf("alias/%s-%s", KinesisStreamKeyNamePrefix, dbName),
		TargetKeyId: kmsKey.ResourceId().ReferenceAttr("key_id"),
	}
	resources = append(resources, kmsAlias)

	// Create Kinesis Data Stream
	kinesisStreamName := KinesisStreamName(dbName)
	kinesisStream := &awsresource.KinesisStream{
		Name:            kinesisStreamName,
		RetentionPeriod: 168, // 7 days in hours
		EncryptionType:  "KMS",
		KMSKey:          kmsKey.ResourceId(),
		ShardLevelMetrics: []string{
			"IncomingBytes",
			"IncomingRecords",
			"OutgoingBytes",
			"OutgoingRecords",
			"WriteProvisionedThroughputExceeded",
			"ReadProvisionedThroughputExceeded",
		},
		StreamModeDetails: &awsresource.StreamModeDetails{
			StreamMode: "ON_DEMAND", // Use on-demand mode for automatic scaling
		},
		Tags: tags,
		BaseResource: tf.BaseResource{
			MetaParameters: tf.MetaParameters{
				Lifecycle: tf.Lifecycle{
					PreventDestroy: true, // Protect stream from accidental deletion
				},
			},
		},
	}
	resources = append(resources, kinesisStream)

	return resources, kinesisStream, nil
}
