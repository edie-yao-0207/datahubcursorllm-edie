package main

import (
	"context"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"samsaradev.io/helpers/samtime"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/databricks/mock_databricks"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/samsaraaws/aws_testutil"
	"samsaradev.io/infra/testloader"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/vendormocks/mock_s3iface"
)

func init() {
	fxregistry.MustRegisterDefaultTestConstructor(mock_s3iface.NewMockS3API)
	fxregistry.MustRegisterDefaultTestConstructor(mock_databricks.NewMockAPI)
}

func TestRun(t *testing.T) {
	t.Setenv("AWS_REGION", infraconsts.SamsaraAWSDefaultRegion)

	var env struct {
		MockS3         *aws_testutil.TestS3Client
		MockDatabricks *mock_databricks.MockAPI
		Logger         *QueryHistoryLogger
	}
	testloader.MustStart(t, &env)

	env.MockDatabricks.EXPECT().ListSqlQueryHistory(gomock.Any(), gomock.Any()).Return(&databricks.ListSqlQueryHistoryOutput{
		Results: []*databricks.QueryInfo{
			{
				QueryId:          "foo",
				QueryStartTimeMs: samtime.TimeToMs(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)),
			},
		},
	}, nil)
	require.NoError(t, env.Logger.Run(context.Background(), "foo", time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)))

	out, err := env.MockS3.GetObject(&s3.GetObjectInput{
		Bucket: aws.String("foo"),
		Key:    aws.String("workspaceId=5924096274798303/date=2021-01-01/queries.jsonl"),
	})
	require.NoError(t, err)
	b, err := ioutil.ReadAll(out.Body)
	require.NoError(t, err)
	require.Equal(t, `{"query_id":"foo","status":"","query_text":"","query_start_time_ms":1609459200000,"execution_end_time_ms":0,"query_end_time_ms":0,"user_id":0,"user_name":"","spark_ui_url":"","endpoint_id":"","error_message":"","rows_produced":0}`, strings.TrimSpace(string(b)))
}
