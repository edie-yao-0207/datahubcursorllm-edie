package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"go.uber.org/fx"

	"samsaradev.io/helpers/monitoring"
	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dataplatform/emrworker"
	"samsaradev.io/infra/samsaraaws"
	"samsaradev.io/infra/samsaraaws/awshelpers/awssessions"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/system"
)

func getS3BucketName(cell string) (string, error) {
	prefix := ""
	switch samsaraaws.GetAWSRegion() {
	case infraconsts.SamsaraAWSDefaultRegion:
		prefix = infraconsts.SamsaraClouds.USProd.S3BucketPrefix
	case infraconsts.SamsaraAWSEURegion:
		prefix = infraconsts.SamsaraClouds.EUProd.S3BucketPrefix
	case infraconsts.SamsaraAWSCARegion:
		prefix = infraconsts.SamsaraClouds.CAProd.S3BucketPrefix
	default:
		return "", fmt.Errorf("unknown region: %s", samsaraaws.GetAWSRegion())
	}
	return fmt.Sprintf("%semr-replication-export-%s", prefix, cell), nil
}

func main() {
	system.NewFx(
		&config.ConfigParams{},
		fx.Provide(
			// Provide instrumented AWS session
			func() *session.Session {
				return awssessions.NewInstrumentedAWSSession()
			},
			func(appConfig *config.AppConfig) emrworker.EmrSqsWorkerConfig {
				// During initialization checks (like fxinitchecker), we'll use a dummy entity name
				// In production, this will be overridden by the environment variable in the worker
				entityName := "speedingintervalsbytrip"
				if env, ok := os.LookupEnv("EMR_REPLICATION_ENTITY_NAME"); ok {
					monitoring.AggregatedDatadog.Incr(
						"entity.emrreplicationsqsworker.env_entity_name_check_main_function.count",
						"entity_name:"+env,
					)
					entityName = env
				}

				// Derive the stream name from the current cell name
				cell := samsaraaws.GetECSClusterName()
				streamName := fmt.Sprintf("emr_replication_kinesis_data_streams_%s", cell)
				s3Bucket, err := getS3BucketName(cell)
				if err != nil {
					log.Fatalf("Failed to get S3 bucket name: %v", err)
				}

				return emrworker.EmrSqsWorkerConfig{
					// Set Arbitrarily to 10 minutes for now.
					ProcessMessageTimeout:    10 * time.Minute,
					VisibilityTimeoutSeconds: int64((10 * time.Minute).Seconds()),
					EntityName:               entityName,
					StreamName:               streamName,
					Concurrency:              50,
					S3Bucket:                 s3Bucket,
				}
			},
		),
		fx.Invoke(func(*emrworker.EmrSqsWorker) {}),
	).Run()
}
