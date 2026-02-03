package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/samsarahq/go/oops"
	"go.uber.org/fx"

	"samsaradev.io/helpers/samtime"
	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dataplatform/databricks"
	_ "samsaradev.io/infra/dataplatform/databricksoauthfx"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/samsaraaws"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/samsaraaws/s3iface"
	"samsaradev.io/infra/serviceorchestration/leaderelection"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/system"
)

type QueryHistoryLogger struct {
	s3API            s3iface.S3API
	databricksClient databricks.API
	workspaceId      int
}

func New(s3API s3iface.S3API, databricksClient databricks.API) (*QueryHistoryLogger, error) {
	// Map region to workspaceId, so that if we replicate query history from other
	// regions they won't collide in S3. Ideally we can grab the workspaceId with
	// an API call, but no such API exists.
	region := samsaraaws.GetAWSRegion()
	var workspaceId int
	switch region {
	case infraconsts.SamsaraAWSEURegion:
		workspaceId = dataplatformconsts.DatabricksDevEUWorkspaceId
	case infraconsts.SamsaraAWSCARegion:
		workspaceId = dataplatformconsts.DatabricksDevCAWorkspaceId
	case infraconsts.SamsaraAWSDefaultRegion:
		workspaceId = dataplatformconsts.DatabricksDevUSWorkspaceId
	default:
		return nil, oops.Errorf("bad region: %s", region)
	}

	return &QueryHistoryLogger{
		s3API:            s3API,
		databricksClient: databricksClient,
		workspaceId:      workspaceId,
	}, nil
}

// Run stores queries between `startDate` and `endDate` (inclusive on both sides) to S3 bucket `bucket`.
func (l *QueryHistoryLogger) Run(ctx context.Context, bucket string, startDate, endDate time.Time) error {
	input := &databricks.ListSqlQueryHistoryInput{
		FilterBy: &databricks.QueryFilter{
			QueryStartTimeRange: databricks.TimeRange{
				StartTimeMs: samtime.TimeToMs(startDate),
				EndTimeMs:   samtime.TimeToMs(endDate.AddDate(0, 0, 1)),
			},
		},
	}

	historyByDate := make(map[time.Time][]*databricks.QueryInfo)

	for {
		output, err := l.databricksClient.ListSqlQueryHistory(ctx, input)
		if err != nil {
			return oops.Wrapf(err, "")
		}
		for _, query := range output.Results {
			date := samtime.MsToTime(query.QueryStartTimeMs).Truncate(24 * time.Hour)
			historyByDate[date] = append(historyByDate[date], query)
		}
		if !output.HasNextPage {
			break
		}
		input = &databricks.ListSqlQueryHistoryInput{
			PageToken: output.NextPageToken,
		}
	}

	for date := startDate; !date.After(endDate); date = date.AddDate(0, 0, 1) {
		queries := historyByDate[date]
		sort.Slice(queries, func(i, j int) bool {
			return queries[i].QueryStartTimeMs < queries[j].QueryStartTimeMs
		})

		var buf bytes.Buffer
		encoder := json.NewEncoder(&buf)
		for _, query := range queries {
			if err := encoder.Encode(query); err != nil {
				return oops.Wrapf(err, "")
			}
		}

		key := fmt.Sprintf("workspaceId=%d/date=%s/queries.jsonl", l.workspaceId, date.Format("2006-01-02"))
		_, err := l.s3API.PutObjectWithContext(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(buf.Bytes()),
			ACL:    aws.String(s3.ObjectCannedACLBucketOwnerFullControl),
		})
		if err != nil {
			return oops.Wrapf(err, "put s3://%s/%s", bucket, key)
		}
		slog.Infow(ctx, "wrote queries", "date", date.Format("2006-01-02"), "bucket", bucket, "key", key, "count", len(queries))
	}

	return nil
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(New)
}

func main() {
	var logger *QueryHistoryLogger
	var waitForLeaderHelper leaderelection.WaitForLeaderLifecycleHookHelper
	app := system.NewFx(&config.ConfigParams{}, fx.Populate(&logger, &waitForLeaderHelper))
	if err := app.Start(context.Background()); err != nil {
		slog.Fatalw(context.Background(), "failed to start", "error", err)
	}
	defer app.Stop(context.Background())

	if err := waitForLeaderHelper.BlockUntilLeader(context.Background()); err != nil {
		slog.Fatalw(context.Background(), "failed to become leader", "error", err)
	}

	region := samsaraaws.GetAWSRegion()
	if region == "" {
		region = infraconsts.SamsaraAWSDefaultRegion
	}
	bucket := awsregionconsts.RegionPrefix[region] + "databricks-sql-query-history"
	endDate := time.Now().UTC().Truncate(24 * time.Hour)
	startDate := endDate.AddDate(0, 0, -7)

	if err := logger.Run(context.Background(), bucket, startDate, endDate); err != nil {
		slog.Errorw(context.Background(), err, nil)
	}
}
