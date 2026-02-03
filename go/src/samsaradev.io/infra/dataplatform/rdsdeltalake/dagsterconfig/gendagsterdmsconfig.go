// Package dagsterconfig generates Dagster configuration files for RDS replication.
//
// To regenerate the Dagster configs, run from the backend root:
//
//	# Generate both Parquet and Kinesis DMS configs (default)
//	go run go/src/samsaradev.io/infra/dataplatform/cmd/gendagsterdmsconfig/main.go
//
//	# Generate only Parquet DMS configs
//	go run go/src/samsaradev.io/infra/dataplatform/cmd/gendagsterdmsconfig/main.go -target=parquet
//
//	# Generate only Kinesis DMS configs
//	go run go/src/samsaradev.io/infra/dataplatform/cmd/gendagsterdmsconfig/main.go -target=kinesis
//
// The generated configs are written to:
//   - Parquet: go/src/samsaradev.io/infra/dataplatform/rdsdeltalake/dagsterconfig/generated/{region}/dagster_config.json
//   - Kinesis: go/src/samsaradev.io/infra/dataplatform/rdsdeltalake/dagsterconfig/generated/{region}/kinesis_dms_dagster_config.json
//
// Note: Kinesis configs are only generated for databases with EnableKinesisStreamDestination enabled.
package dagsterconfig

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/databricksjobnameshelpers"
	"samsaradev.io/infra/dataplatform/rdsdeltalake"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/rdslakeprojects"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/service/dbregistry"
)

type dagsterConfig struct {
	Databases []databaseConfig `json:"databases"`
}

type databaseConfig struct {
	Name   string        `json:"name"`
	Shards []shardConfig `json:"shards"`
}

type s3Output struct {
	Bucket string `json:"bucket"`
	Prefix string `json:"prefix"`
}

type shardConfig struct {
	Name                      string      `json:"name"`
	CdcTaskName               string      `json:"cdc_task_name"`
	LoadTaskName              string      `json:"load_task_name"`
	DmsS3Location             *s3Output   `json:"dms_s3_location,omitempty"`
	KinesisStreamName         string      `json:"kinesis_stream_name,omitempty"`
	DeltaTableS3Location      s3Output    `json:"delta_table_s3_location"`
	CheckpointsS3Location     s3Output    `json:"checkpoints_s3_location"`
	SslCertificateArnLocation s3Output    `json:"ssl_certificate_arn_location"`
	Tables                    []tableSpec `json:"tables"`
}

type tableSpec struct {
	Name string `json:"name"`
	// CdcVersion should never change because the same CDC task is used when tables are reloaded via table version bumps.
	CdcVersion rdsdeltalake.TableVersion `json:"cdc_version"`
	// LoadVersion gets increased every time a table needs to be reloaded via a table version bump.
	LoadVersion rdsdeltalake.TableVersion `json:"load_version"`
	JobName     string                    `json:"job_name"`
}

// Kinesis DMS specific functions
func KinesisDmsMergeBucket(region string) string {
	return awsregionconsts.RegionPrefix[region] + "kinesis-dms-delta-lake"
}

func KinesisDmsCheckpointPrefix(dbname string) string {
	return fmt.Sprintf("checkpoint/%s", dbname)
}

// DmsTargetType represents the type of DMS target (Parquet or Kinesis)
type DmsTargetType int

const (
	ParquetTarget DmsTargetType = iota
	KinesisTarget
)

func GenerateParquetDagsterConfig() error {
	return generateDagsterConfigs(ParquetTarget)
}

func GenerateKinesisDagsterConfig() error {
	return generateDagsterConfigs(KinesisTarget)
}

// GenerateDagsterConfig is kept for backward compatibility - generates Parquet configs
func GenerateDagsterConfig() error {
	return GenerateParquetDagsterConfig()
}

func generateDagsterConfigs(targetType DmsTargetType) error {
	folder := filepath.Join(filepathhelpers.BackendRoot, "go/src/samsaradev.io/infra/dataplatform/rdsdeltalake/dagsterconfig/generated")

	for _, region := range []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion} {
		// Ensure the directory exists before starting.
		err := os.MkdirAll(filepath.Join(folder, region), 0777)
		if err != nil {
			return oops.Wrapf(err, "failed to create directories")
		}

		// Loop through every database, collecting the necessary config and write it out to a file.
		var config dagsterConfig
		for _, db := range rdsdeltalake.AllDatabases() {
			if !db.IsInRegion(region) {
				continue
			}

			// For Kinesis target, only generate configs for databases with EnableKinesisStreamDestination enabled
			if targetType == KinesisTarget && !db.InternalOverrides.EnableKinesisStreamDestination {
				continue
			}

			// Get shard configs and sort by name for stability in output.
			var shards []shardConfig
			for _, shard := range db.RegionToShards[region] {
				// Get table specs and lets always sort by name for stability in output.
				var tables []tableSpec
				for _, table := range db.Tables {
					tables = append(tables, tableSpec{
						Name:        table.TableName,
						CdcVersion:  table.CdcVersion_DO_NOT_SET,
						LoadVersion: table.VersionInfo.DMSOutputVersionParquet(),
						JobName: databricksjobnameshelpers.GetRDSParquetMergeJobName(databricksjobnameshelpers.GetRDSMergeJobNameInput{
							Shard:        shard,
							MySqlDb:      db.MySqlDb,
							TableName:    table.TableName,
							TableVersion: int(table.VersionInfo.DMSOutputVersionParquet()),
							Region:       region,
						}),
					})
				}
				sort.Slice(tables, func(i, j int) bool {
					return tables[i].Name < tables[j].Name
				})

				var shardCfg shardConfig
				if targetType == KinesisTarget {
					// All shards in a database use the same unified Kinesis stream (database-level)
					// DMS tasks are still per-shard, but they all write to the same stream
					shardCfg = shardConfig{
						Name:              shard,
						CdcTaskName:       rdslakeprojects.KinesisStreamCdcTaskName(shard),
						LoadTaskName:      rdslakeprojects.KinesisStreamLoadTaskName(shard),
						Tables:            tables,
						KinesisStreamName: rdslakeprojects.KinesisStreamName(db.MySqlDb),
						DeltaTableS3Location: s3Output{
							Bucket: KinesisDmsMergeBucket(region),
							Prefix: fmt.Sprintf("%s/%s", rdslakeprojects.TablePrefix, db.DatabaseS3Path(shard)),
						},
						CheckpointsS3Location: s3Output{
							Bucket: KinesisDmsMergeBucket(region),
							Prefix: fmt.Sprintf("%s/%s", KinesisDmsCheckpointPrefix(shard), db.DatabaseS3Path(shard)),
						},
						SslCertificateArnLocation: s3Output{
							Bucket: KinesisDmsMergeBucket(region),
							Prefix: fmt.Sprintf("ssl_certificate_arn/%s", db.DatabaseS3Path(shard)),
						},
					}
				} else {
					shardCfg = shardConfig{
						Name:         shard,
						CdcTaskName:  rdslakeprojects.ParquetCdcTaskName(shard),
						LoadTaskName: rdslakeprojects.ParquetLoadTaskName(shard),
						Tables:       tables,
						DmsS3Location: &s3Output{
							Bucket: rdslakeprojects.RdsExportBucket(region),
							Prefix: rdslakeprojects.DmsOutputPrefix(shard),
						},
						DeltaTableS3Location: s3Output{
							Bucket: rdslakeprojects.ParquetMergeBucket(region),
							Prefix: fmt.Sprintf("%s/%s", rdslakeprojects.TablePrefix, db.DatabaseS3Path(shard)),
						},
						CheckpointsS3Location: s3Output{
							Bucket: rdslakeprojects.ParquetMergeBucket(region),
							Prefix: fmt.Sprintf("%s/%s", rdslakeprojects.CheckpointPrefix, db.DatabaseS3Path(shard)),
						},
						SslCertificateArnLocation: s3Output{
							Bucket: rdslakeprojects.ParquetMergeBucket(region),
							Prefix: fmt.Sprintf("ssl_certificate_arn/%s", db.DatabaseS3Path(shard)),
						},
					}
				}
				shards = append(shards, shardCfg)
			}
			sort.Slice(shards, func(i, j int) bool {
				return shards[i].Name < shards[j].Name
			})

			// Alertsdb has a different name in the US than in EU and CA. It's
			// slightly hacky to do this here, but modifying the registry to become
			// aware of region-level mysqldb overrides would require more work and it
			// would be easiest to do that after the rds replication project is
			// completely done. In the short term this will fix alertsdb in the EU and
			// CA regions.
			mysqlDb := db.MySqlDb
			if db.RegistryName == dbregistry.AlertsDB && region != infraconsts.SamsaraAWSDefaultRegion {
				mysqlDb = "alertsdb"
			}

			config.Databases = append(config.Databases, databaseConfig{
				// We want to make sure to use the name of the mysql db here as thats what dagster needs when working with DMS tasks
				Name:   mysqlDb,
				Shards: shards,
			})
		}

		// Sort database names for stability in output.
		sort.Slice(config.Databases, func(i, j int) bool {
			return config.Databases[i].Name < config.Databases[j].Name
		})

		// Write generated config file.
		output, err := json.MarshalIndent(config, "", "  ")
		if err != nil {
			return oops.Wrapf(err, "failed to marshal dagster configuration")
		}

		var filename string
		if targetType == KinesisTarget {
			filename = "kinesis_dms_dagster_config.json"
		} else {
			filename = "dagster_config.json"
		}

		fp := filepath.Join(folder, region, filename)
		f, err := os.Create(fp)
		if err != nil {
			return oops.Wrapf(err, "failed to open dagster config file for writing")
		}
		defer f.Close()
		_, err = f.Write(output)
		if err != nil {
			return oops.Wrapf(err, "failed to write to dagster config")
		}

		// Theres no newline at the end of the file, and backend ci fails and tries to patch it.
		// I'm not sure why that happens, but lets add it explicitly.
		_, err = f.WriteString("\n")
		if err != nil {
			return oops.Wrapf(err, "failed to write to dagster config")
		}
	}

	return nil
}
