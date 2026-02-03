package allserieslister

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/samsarahq/go/oops"
	"google.golang.org/grpc"

	"samsaradev.io/helpers/errgroup"
	"samsaradev.io/infra/config"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/grpc/grpchelpers/grpcclient"
	"samsaradev.io/infra/releasemanagement/launchdarkly"
	"samsaradev.io/infra/samsaraaws/awshelpers/awssessions"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/stats/kinesisstats/kinesisstatsproto"
	"samsaradev.io/stats/kinesisstats/serieslisterpaginatedclient"
)

// TODO: put this in the kinesisstats package, and make this implementation implement the kinesisstatsproto.SeriesListerClient interface
//
//	rather than our bespoke one.
type AllSeriesListerClient interface {
	GetSeriesWithTimeMs(ctx context.Context, in *kinesisstatsproto.GetSeriesRequest, opts ...grpc.CallOption) (*kinesisstatsproto.GetSeriesWithTimeMsResponse, error)
}

type AllSeriesListerImpl struct {
	region             string
	appConfig          *config.AppConfig
	grpcDial           grpcclient.DialFunc
	launchDarklyBridge *launchdarkly.SDKBridge
}

// Dials every KS cluster in the region, and appends the results of the query together.
func (a AllSeriesListerImpl) GetSeriesWithTimeMs(ctx context.Context, in *kinesisstatsproto.GetSeriesRequest, opts ...grpc.CallOption) (*kinesisstatsproto.GetSeriesWithTimeMsResponse, error) {
	clusters, err := infraconsts.RegionKinesisStatsClusters(a.region)
	if err != nil {
		return nil, oops.Wrapf(err, "couldn't get all ks clusters for the region")
	}

	var mu sync.Mutex
	eg, ctx := errgroup.WithContext(ctx)
	var seriesList kinesisstatsproto.SeriesWithTimeMsList

	// Dial every ks cluster for the region, appending the results together.
	// We exclude ks0 since it has mostly non-customer information, and has
	// some orgs (e.g. the "every device heartbeat org") which cause problems
	// when attempting to list series.
	for _, c := range clusters {
		if c == infraconsts.KinesisStatsCluster0ID {
			continue
		}
		cluster := c
		eg.Go(func() error {
			seriesListerUrl := a.appConfig.KinesisStatsServiceConfigByCluster[cluster].SerieslisterUrlBase
			seriesListerConn, err := a.grpcDial(seriesListerUrl, grpcclient.DialFuncOptions{})
			if err != nil {
				return oops.Wrapf(err, "failed to connect to ks cluster %s", cluster)
			}
			seriesLister := kinesisstatsproto.NewSeriesListerServiceClient(seriesListerConn)
			seriesListerClient := serieslisterpaginatedclient.NewSeriesListerPaginatedClient(seriesLister, a.launchDarklyBridge)
			output, err := seriesListerClient.GetSeriesWithTimeMs(ctx, in, opts...)
			if err != nil {
				return oops.Wrapf(err, "failed to list for series in cluster %s", cluster)
			}
			if output.SeriesList == nil {
				return oops.Errorf("GetSeriesWithTimeMs returned unexpected nil SeriesList")
			}

			mu.Lock()
			seriesList.List = append(seriesList.List, output.SeriesList.List...)
			mu.Unlock()

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, oops.Wrapf(err, "error listing from all ks clusters for region %s", a.region)
	}

	return &kinesisstatsproto.GetSeriesWithTimeMsResponse{SeriesList: &seriesList}, nil
}

func NewAllSeriesLister(appConfig *config.AppConfig, grpcDial grpcclient.DialFunc, launchDarklyBridge *launchdarkly.SDKBridge) (AllSeriesListerClient, error) {
	sess := awssessions.NewInstrumentedAWSSession()
	region := aws.StringValue(sess.Config.Region)
	return &AllSeriesListerImpl{region: region, appConfig: appConfig, grpcDial: grpcDial, launchDarklyBridge: launchDarklyBridge}, nil
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(NewAllSeriesLister)
}

var _ AllSeriesListerClient = &AllSeriesListerImpl{}
