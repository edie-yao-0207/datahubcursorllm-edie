package updateclusters

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ecr"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/helpers/samtime"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/databricks/mock_databricks"
	"samsaradev.io/infra/testloader"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/vendormocks/mock_ecriface"
)

// End to end test that function runs with no errors to ensure that future additions do not break function
func TestRun(t *testing.T) {
	ctx := context.Background()

	var env struct {
		Task             *ClusterUpdater
		DatabricksClient *mock_databricks.MockAPI
		ECRClient        *mock_ecriface.MockECRAPI
	}
	testloader.MustStart(t, &env)

	// Mock out dbx list clusters call
	// Return clusters that would enter the if statement and cause the main body to run (set tags and state properly)
	// Return multiple clusters to make sure function can run for more than one cluster returned from the list endpoint
	env.DatabricksClient.EXPECT().ListClusters(ctx, &databricks.ListClustersInput{}).Return(&databricks.ListClustersOutput{
		Clusters: []databricks.ClusterSpec{
			{
				ClusterId:   "1",
				ClusterName: "test",
				CustomTags:  map[string]string{"samsara:dataplatform-job-type": "interactive_cluster"},
				State:       "TERMINATED",
			},
			{
				ClusterId:   "2",
				ClusterName: "test2",
				CustomTags:  map[string]string{"samsara:dataplatform-job-type": "interactive_cluster"},
				State:       "TERMINATED",
			},
		},
	}, nil).Times(1)

	// Mock out ECR describe images call
	// Make sure cache works--should not have to describe images more than once if the same repo name is used
	identifiers := []*ecr.ImageIdentifier{{ImageTag: aws.String("latest")}}
	testDate := time.Date(2019, 06, 12, 13, 00, 00, 00, samtime.PacificTimeLocation) // June 12th, 2019
	defaultRepo := "samsara-python-standard"
	latestCommitHashTag := "1"
	env.ECRClient.EXPECT().DescribeImages(&ecr.DescribeImagesInput{
		RepositoryName: aws.String(defaultRepo),
		ImageIds:       identifiers,
		RegistryId:     aws.String(strconv.Itoa(infraconsts.GetDatabricksAccountIdForRegion(env.Task.region))),
	}).Return(
		&ecr.DescribeImagesOutput{
			ImageDetails: []*ecr.ImageDetail{
				{
					ImagePushedAt: &testDate,
					ImageTags:     []*string{aws.String("latest"), aws.String(latestCommitHashTag)},
				},
			},
		},
		nil,
	).Times(0)

	// Mock out dbx edit cluster call
	env.DatabricksClient.EXPECT().EditCluster(ctx, gomock.Any()).Times(0)

	require.NoError(t, env.Task.Run())
}

func TestFindLatestECRTag(t *testing.T) {
	ctx := context.Background()

	var env struct {
		Task      *ClusterUpdater
		ECRClient *mock_ecriface.MockECRAPI
	}
	testloader.MustStart(t, &env)

	// Set expected results from describe images ECR call
	identifiers := []*ecr.ImageIdentifier{{ImageTag: aws.String("latest")}}
	testDate := time.Date(2019, 06, 12, 13, 00, 00, 00, samtime.PacificTimeLocation)        // June 12th, 2019
	testDateEarlier := time.Date(2019, 05, 12, 13, 00, 00, 00, samtime.PacificTimeLocation) // May 12th, 2019
	latestCommitHashTag := "1"
	env.ECRClient.EXPECT().DescribeImages(&ecr.DescribeImagesInput{
		RepositoryName: aws.String("test-repo"),
		ImageIds:       identifiers,
		RegistryId:     aws.String(strconv.Itoa(infraconsts.GetDatabricksAccountIdForRegion(env.Task.region))),
	}).Return(
		&ecr.DescribeImagesOutput{
			ImageDetails: []*ecr.ImageDetail{
				{
					ImagePushedAt: &testDate,
					ImageTags:     []*string{aws.String("latest"), aws.String(latestCommitHashTag)},
				},
				{
					ImagePushedAt: &testDateEarlier,
					ImageTags:     []*string{aws.String("2")},
				},
			},
		},
		nil,
	).Times(1)

	latestImage, _, err := env.Task.findLatestECRImageTag(ctx, "test-repo")
	require.NoError(t, err)
	// Test that output image tag is equal to most recent image by PushedAt date
	assert.Equal(t, latestImage, latestCommitHashTag)
}
