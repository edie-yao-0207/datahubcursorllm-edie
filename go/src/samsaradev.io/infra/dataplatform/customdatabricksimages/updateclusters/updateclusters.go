package updateclusters

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ecr"
	"github.com/aws/aws-sdk-go/service/ecr/ecriface"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/monitoring"
	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dataplatform/databricks"
	_ "samsaradev.io/infra/dataplatform/databricksoauthfx"
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/samsaraaws/awshelpers/awssessions"
	"samsaradev.io/infra/serviceorchestration/leaderelection"
	"samsaradev.io/libs/ni/infraconsts"
)

type ClusterUpdater struct {
	region string

	waitForLeaderHelper leaderelection.WaitForLeaderLifecycleHookHelper
	databricksClient    databricks.API
	ecrClient           ecriface.ECRAPI
}

func New(appConfig *config.AppConfig, waitForLeaderHelper leaderelection.WaitForLeaderLifecycleHookHelper, databricksClient databricks.API, ecrClient ecriface.ECRAPI) (*ClusterUpdater, error) {
	sess := awssessions.NewInstrumentedAWSSession()
	region := aws.StringValue(sess.Config.Region)

	c := &ClusterUpdater{
		region:              region,
		waitForLeaderHelper: waitForLeaderHelper,
		databricksClient:    databricksClient,
		ecrClient:           ecrClient,
	}

	return c, nil
}

// This function does the heavy lifting--essentially putting together the custom docker ECR url with the latest
// image tag and updating all the clusters with it
func (clusterUpdater *ClusterUpdater) Run() error {
	ctx := context.Background()

	err := clusterUpdater.waitForLeaderHelper.BlockUntilLeader(ctx)
	if err != nil {
		return oops.Wrapf(err, "Could not block until leader")
	}

	slog.Infow(ctx, "Now updating databricks clusters", "region", clusterUpdater.region)

	// Databricks list endpoint returns 150 clusters, in order of priority where pinned clusters take priority
	// Thus, to get all interactive clusters in this endpoint, we have pinned every interactive cluster
	// We currently have about 90 clusters, if this ever exceeds 150, we need to ask Databricks to up the limit
	clusters, err := clusterUpdater.databricksClient.ListClusters(ctx, &databricks.ListClustersInput{})
	if err != nil {
		return oops.Wrapf(err, "Unable to list clusters")
	}

	clusterToRepo := getRepoFromDatabricksClusterName()
	repoToLatestTag := make(map[string]string)

	accId := infraconsts.GetDatabricksAccountIdForRegion(clusterUpdater.region)
	for _, cluster := range clusters.Clusters {
		if cluster.CustomTags["samsara:dataplatform-job-type"] == "interactive_cluster" && cluster.State == "TERMINATED" {
			slog.Infow(ctx, "Now editing cluster", "cluster_name", cluster.ClusterName)

			// Find repo that cluster should pull from, if not found then default to standard 7.x image with added python repo
			var latestTag string
			repo, ok := clusterToRepo[cluster.ClusterName]
			if !ok {
				slog.Errorw(ctx, oops.Errorf("%s", fmt.Sprintf("Could not find cluster %s", cluster.ClusterName)), nil)
			}

			if repo == "" || repo == "none" {
				// This is an opt-in process so if the repo is not specified, we don't update
				slog.Infow(ctx, "Repo not specified, skip update")
				continue
			}

			// If another cluster has used this repo, the latest image tag should be cached in the map
			// Else, find latest tag from helper function
			var latestTagTime time.Time
			if tag, ok := repoToLatestTag[repo]; ok {
				latestTag = tag
				slog.Infow(ctx, "Latest tag found in cache", "latest tag", latestTag)
			} else {
				latestTag, latestTagTime, err = clusterUpdater.findLatestECRImageTag(ctx, repo)
				if err != nil {
					slog.Errorw(ctx, oops.Wrapf(err, ""), nil)
				} else {
					repoToLatestTag[repo] = latestTag
					slog.Infow(ctx, "Latest tag found", "latest tag", latestTag)
				}
			}

			timeSinceLatestDockerImage := time.Since(latestTagTime) * time.Millisecond
			monitoring.AggregatedDatadog.HistogramValue(float64(timeSinceLatestDockerImage), "customdatabricksimpages.updatecluster.timesincelatestdockerimage")

			// Build docker image URL and call edit endpoint with new image URL
			dockerImageUrl := fmt.Sprintf("%d.dkr.ecr.%s.amazonaws.com/%s:%s", accId, clusterUpdater.region, repo, latestTag)
			dockerImage := databricks.DockerImage{URL: dockerImageUrl}
			cluster.CustomDockerImage = &dockerImage
			inp := databricks.EditClusterInput{
				ClusterSpec: cluster,
				ClusterId:   cluster.ClusterId,
			}
			_, err := clusterUpdater.databricksClient.EditCluster(ctx, &inp)
			if err != nil {
				slog.Errorw(ctx, oops.Wrapf(err, "Unable to edit interactive cluster: %s", cluster.ClusterName), nil)
			}
		}
	}
	slog.Infow(ctx, "Updating clusters finished")
	return nil
}

// Helper function that queries the ECR api and finds the latest image tag based on a region and a repository name
func (clusterUpdater *ClusterUpdater) findLatestECRImageTag(ctx context.Context, repo string) (string, time.Time, error) {
	slog.Infow(ctx, "Now pulling ECR to find latest commit hash")

	// In order to get ALL images in a repo (which we have up to 1000 of) we would have to use the paginated endpoint
	// go through every page, add them all to a list and sort
	// However, since we push "latest" as a tag along with the commit hash for the most recent image, we can just use
	// and identifier to find it! The endpoint will return all tags associated with this image so latest and the commit hash
	identifiers := []*ecr.ImageIdentifier{{ImageTag: aws.String("latest")}}
	images, err := clusterUpdater.ecrClient.DescribeImages(&ecr.DescribeImagesInput{
		RepositoryName: aws.String(repo),
		ImageIds:       identifiers,
		RegistryId:     aws.String(strconv.Itoa(infraconsts.GetDatabricksAccountIdForRegion(clusterUpdater.region))),
	})

	if err != nil {
		return "", time.Time{}, oops.Wrapf(err, "Unable to describe images to find latest pushed image for repo: %s", repo)
	}

	imageDetails := images.ImageDetails
	if len(imageDetails) == 0 {
		return "", time.Time{}, oops.Errorf("Repo %s has no images available", repo)
	}

	sort.Slice(imageDetails, func(i, j int) bool {
		left := aws.TimeValue(imageDetails[i].ImagePushedAt)
		right := aws.TimeValue(imageDetails[j].ImagePushedAt)
		return left.After(right)
	})

	latestImage := imageDetails[0]
	for _, tag := range latestImage.ImageTags {
		tagString := aws.StringValue(tag)
		if tagString != "latest" {
			return tagString, *latestImage.ImagePushedAt, nil
		}
	}
	return "", time.Time{}, oops.Errorf("Cannot find tag with commit hash")
}

// Helper function that maps cluster names to the repo that the cluster wants to refer to
func getRepoFromDatabricksClusterName() map[string]string {
	teamMap := make(map[string]string)
	for _, t := range clusterhelpers.DatabricksClusterTeams() {
		name := strings.ToLower(t.Name())
		teamMap[name] = string(t.DatabricksInteractiveClusterOverrides.CustomRepoName)
		for _, clusterConfig := range t.DatabricksInteractiveClusters {
			clusterName := fmt.Sprintf("%s-%s", name, clusterConfig.Name)
			teamMap[clusterName] = string(clusterConfig.CustomRepoName)
		}
	}
	return teamMap
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(New)
}
