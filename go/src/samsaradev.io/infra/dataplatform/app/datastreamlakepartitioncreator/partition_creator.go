package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/dataplatform/datastreamlake"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/monitoring/datadogutils"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/samsaraaws/glueiface"
	"samsaradev.io/libs/ni/infraconsts"
)

type PartitionCreator struct {
	glueClient glueiface.GlueAPI
}

func newPartitionCreator(glueClient glueiface.GlueAPI) (*PartitionCreator, error) {
	return &PartitionCreator{
		glueClient: glueClient,
	}, nil
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(newPartitionCreator)
}

func (p *PartitionCreator) CreatePartitions(ctx context.Context, dataStreamDatabaseName string, date string) error {
	sess := session.Must(session.NewSession())
	if sess.Config.Region == nil {
		return oops.Errorf("Cannot determine current AWS region")
	}

	region := *sess.Config.Region
	s3Prefix := awsregionconsts.RegionPrefix[region]

	ctx = slog.With(ctx, "region", region, "date", date)

	for _, entry := range datastreamlake.Registry {
		_, err := p.glueClient.CreatePartitionWithContext(ctx, &glue.CreatePartitionInput{
			CatalogId:    aws.String(strconv.Itoa(infraconsts.GetDatabricksAccountIdForRegion(region))),
			DatabaseName: aws.String(dataStreamDatabaseName),
			TableName:    aws.String(entry.StreamName),
			PartitionInput: &glue.PartitionInput{
				Values: []*string{aws.String(date)},
				StorageDescriptor: &glue.StorageDescriptor{
					InputFormat:  aws.String("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
					OutputFormat: aws.String("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
					SerdeInfo: &glue.SerDeInfo{
						Name:                 aws.String("parquet"),
						SerializationLibrary: aws.String("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"),
					},
					Location: aws.String(fmt.Sprintf("s3://%sdata-stream-lake/%s/data/date=%s/", s3Prefix, entry.StreamName, date)),
				},
			},
		})
		validError := err
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok && aerr.Code() == glue.ErrCodeAlreadyExistsException {
				slog.Infow(ctx, "partition already exists", "database", dataStreamDatabaseName, "table", entry.StreamName)
				validError = nil
			} else {
				// slog error and continue because if one table partition is unable to be created
				// it should not affected other tables partition creation
				slog.Errorw(ctx, err, slog.Tag{
					"database": dataStreamDatabaseName,
					"table":    entry.StreamName,
				})
			}
		}
		datadogutils.IncrResult(validError, "firehose.partition_creation",
			fmt.Sprintf("database:%s", dataStreamDatabaseName),
			fmt.Sprintf("deliverystream:%s", entry.StreamName),
			fmt.Sprintf("region:%s", region),
			fmt.Sprintf("date:%s", date),
		)
	}

	return nil
}
