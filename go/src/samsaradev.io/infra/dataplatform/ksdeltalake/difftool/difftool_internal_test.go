package difftool

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/samsarahq/go/snapshotter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/gqlschema/gqlobjstat"
	"samsaradev.io/helpers/samtime"
	"samsaradev.io/hubproto/objectstatproto"
	"samsaradev.io/hubproto/vgmcuobjectstatproto"
	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dataplatform/ksdeltalake/difftool/allserieslister/mock_allserieslister"
	"samsaradev.io/infra/testloader"
	"samsaradev.io/infra/thunder/batch"
	"samsaradev.io/libs/ni/pointer"
	"samsaradev.io/models"
	"samsaradev.io/products"
	"samsaradev.io/stats/kinesisstats/jsonexporter/cf2jsonlschema"
	"samsaradev.io/stats/kinesisstats/kinesisstatsproto"
	"samsaradev.io/stats/kinesisstats/objectstatsaccessor"
	"samsaradev.io/stats/kinesisstats/objectstatsaccessor/mock_objectstatsaccessor"
	"samsaradev.io/stats/s3stats"
	"samsaradev.io/vendormocks/mock_s3iface"
)

type fixtures struct {
	orgA *models.Organization
	orgB *models.Organization
	orgC *models.Organization

	device1 *models.Device
	device2 *models.Device
	device3 *models.Device
	device4 *models.Device
}

func setupDb(ctx context.Context, t *testing.T, appModels models.PrimaryAppModelsInput) fixtures {
	// Set up a couple orgs/devices
	// org_a -> has device1 and device2
	// org_b -> has device3
	// org_c (non customer) -> has device 4
	orgA := appModels.Models.Organization.CreateWithTxForTest("org_a")
	groupA := appModels.Models.CreateGroup("group_a", orgA.Id)

	orgB := appModels.Models.Organization.CreateWithTxForTest("org_b")
	groupB := appModels.Models.CreateGroup("group_b", orgB.Id)

	orgC := appModels.Models.Organization.CreateWithTxForTest("org_c")
	groupC := appModels.Models.CreateGroup("group_c", orgC.Id)
	orgC.InternalType = models.OrgTypeGenericInternal
	require.NoError(t, appModels.Models.OrgUpdate(orgC))

	device1Id, err := appModels.Models.CreateDeviceOnly(ctx, nil, products.ProductVg34)
	require.NoError(t, err)
	device2Id, err := appModels.Models.CreateDeviceOnly(ctx, nil, products.ProductVg34)
	require.NoError(t, err)
	device3Id, err := appModels.Models.CreateDeviceOnly(ctx, nil, products.ProductVg34)
	require.NoError(t, err)
	device4Id, err := appModels.Models.CreateDeviceOnly(ctx, nil, products.ProductVg34)
	require.NoError(t, err)

	require.NoError(t, appModels.Models.AddDeviceToGroup(nil, device1Id, groupA.Id))
	require.NoError(t, appModels.Models.AddDeviceToGroup(nil, device2Id, groupA.Id))
	require.NoError(t, appModels.Models.AddDeviceToGroup(nil, device3Id, groupB.Id))
	require.NoError(t, appModels.Models.AddDeviceToGroup(nil, device4Id, groupC.Id))

	device1, err := appModels.Models.Device.ById(ctx, device1Id)
	require.NoError(t, err)
	device2, err := appModels.Models.Device.ById(ctx, device2Id)
	require.NoError(t, err)
	device3, err := appModels.Models.Device.ById(ctx, device3Id)
	require.NoError(t, err)
	device4, err := appModels.Models.Device.ById(ctx, device4Id)
	require.NoError(t, err)

	return fixtures{
		orgA: orgA,
		orgB: orgB,
		orgC: orgC,

		device1: device1,
		device2: device2,
		device3: device3,
		device4: device4,
	}
}

func makeSeries(device *models.Device, timeMs int64) *kinesisstatsproto.SeriesWithTimeMs {
	return &kinesisstatsproto.SeriesWithTimeMs{
		Series: fmt.Sprintf("objectstat#object=%d#objecttype=1#organization=%d#stattype=184", device.Id, device.OrgId),
		TimeMs: timeMs,
	}
}

func TestGetDevicesForStat(t *testing.T) {
	var env struct {
		Client   *mock_allserieslister.MockAllSeriesListerClient
		DiffTool *DiffTool
		Models   models.PrimaryAppModelsInput
	}
	testloader.MustStart(t, &env)
	ctx := context.Background()

	startDate := samtime.MsToTime(10)
	fixtures := setupDb(ctx, t, env.Models)

	seriesList := []*kinesisstatsproto.SeriesWithTimeMs{
		makeSeries(fixtures.device1, 11),
		makeSeries(fixtures.device2, 11),
		makeSeries(fixtures.device3, 11),
		makeSeries(fixtures.device4, 11),
	}

	testCases := []struct {
		description       string
		randomSampleCount int64
		orgIds            []int64
		expectedSeries    []*models.Device
		expectedLength    *int
	}{
		{
			description: "zero for everything returns empty",
		},
		{
			description:    "by org id works",
			orgIds:         []int64{fixtures.orgB.Id},
			expectedSeries: []*models.Device{fixtures.device3},
		},
		{
			description:       "random sample works",
			randomSampleCount: 3,
			expectedLength:    pointer.IntPtr(3),
		},
	}

	env.Client.EXPECT().GetSeriesWithTimeMs(
		ctx,
		&kinesisstatsproto.GetSeriesRequest{
			Kinds: []kinesisstatsproto.SeriesKind{kinesisstatsproto.SeriesKind_ObjectStat},
			TagFilters: map[string]*kinesisstatsproto.GetSeriesRequest_TagValues{
				string(s3stats.StatTypeTagKey): {Values: []string{strconv.FormatInt(int64(objectstatproto.ObjectStatEnum_osDAccelerometer), 10)}},
			},
		},
	).Return(&kinesisstatsproto.GetSeriesWithTimeMsResponse{SeriesList: &kinesisstatsproto.SeriesWithTimeMsList{List: seriesList}}, nil).Times(len(testCases) + 1)

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			deviceMap, err := env.DiffTool.getCustomerAndSpecifiedDevices(ctx, tc.orgIds)
			require.NoError(t, err)
			result, err := env.DiffTool.getDevicesForStat(ctx, deviceMap, objectstatproto.ObjectStatEnum_osDAccelerometer, startDate, tc.randomSampleCount, tc.orgIds)
			require.NoError(t, err)

			if tc.expectedLength != nil {
				assert.Equal(t, *tc.expectedLength, len(result))
			} else {
				assert.Equal(t, tc.expectedSeries, result)
			}
		})
	}

	// Add one extra test case where we randomly sample AND have some orgs specified
	deviceMap, err := env.DiffTool.getCustomerAndSpecifiedDevices(ctx, []int64{fixtures.orgA.Id})
	require.NoError(t, err)
	series, err := env.DiffTool.getDevicesForStat(ctx, deviceMap, objectstatproto.ObjectStatEnum_osDAccelerometer, startDate, 1, []int64{fixtures.orgA.Id})
	require.NoError(t, err)
	assert.Equal(t, 3, len(series))
	for _, elem := range series[0:2] {
		assert.Equal(t, elem.OrgId, int64(fixtures.orgA.Id))
	}
	for _, elem := range series[2:] {
		assert.NotEqual(t, elem.OrgId, int64(fixtures.orgA.Id))
	}

}

func TestGetCustomerOrgs(t *testing.T) {
	var env struct {
		Models   models.PrimaryAppModelsInput
		DiffTool *DiffTool
	}
	testloader.MustStart(t, &env)
	ctx := context.Background()

	fixtures := setupDb(ctx, t, env.Models)

	// Without any specific orgs, only include orgs a+b
	result, err := env.DiffTool.getCustomerAndSpecifiedDevices(ctx, []int64{})
	require.NoError(t, err)
	assert.Equal(t, orgDeviceMap{
		fixtures.orgA.Id: map[int64]*models.Device{
			fixtures.device1.Id: fixtures.device1,
			fixtures.device2.Id: fixtures.device2,
		},
		fixtures.orgB.Id: map[int64]*models.Device{
			fixtures.device3.Id: fixtures.device3,
		},
	}, result)

	// Specifying non-customer orgs should work, and customer orgs shouldn't overlap
	result, err = env.DiffTool.getCustomerAndSpecifiedDevices(ctx, []int64{fixtures.orgC.Id, fixtures.orgA.Id})
	require.NoError(t, err)
	assert.Equal(t, orgDeviceMap{
		fixtures.orgA.Id: map[int64]*models.Device{
			fixtures.device1.Id: fixtures.device1,
			fixtures.device2.Id: fixtures.device2,
		},
		fixtures.orgB.Id: map[int64]*models.Device{
			fixtures.device3.Id: fixtures.device3,
		},
		fixtures.orgC.Id: map[int64]*models.Device{
			fixtures.device4.Id: fixtures.device4,
		},
	}, result)
}

func TestListAndParseSeries(t *testing.T) {
	var env struct {
		Client   *mock_allserieslister.MockAllSeriesListerClient
		DiffTool *DiffTool
	}
	testloader.MustStart(t, &env)
	ctx := context.Background()
	series := []*kinesisstatsproto.SeriesWithTimeMs{
		{Series: "objectstat#object=1#objecttype=1#organization=728#stattype=184", TimeMs: 10},
		{Series: "objectstat#object=2#objecttype=1#organization=728#stattype=184", TimeMs: 20},
		{Series: "objectstat#object=3#objecttype=1#organization=9086#stattype=184", TimeMs: 30},
		{Series: "objectstat#object=4#objecttype=1#organization=9086#stattype=184", TimeMs: 40},
	}

	expected := []parsedSeries{
		{objectId: 2, orgId: 728},
		{objectId: 3, orgId: 9086},
		{objectId: 4, orgId: 9086},
	}
	env.Client.EXPECT().GetSeriesWithTimeMs(
		ctx,
		&kinesisstatsproto.GetSeriesRequest{
			Kinds: []kinesisstatsproto.SeriesKind{kinesisstatsproto.SeriesKind_ObjectStat},
			TagFilters: map[string]*kinesisstatsproto.GetSeriesRequest_TagValues{
				string(s3stats.StatTypeTagKey): {Values: []string{strconv.FormatInt(int64(objectstatproto.ObjectStatEnum_osDAccelerometer), 10)}},
			},
		},
	).Return(&kinesisstatsproto.GetSeriesWithTimeMsResponse{SeriesList: &kinesisstatsproto.SeriesWithTimeMsList{List: series}}, nil)

	result, err := env.DiffTool.listAndParseSeries(ctx, objectstatproto.ObjectStatEnum_osDAccelerometer, samtime.MsToTime(15))
	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestFetchAndWriteStat(t *testing.T) {
	var env struct {
		Models    models.PrimaryAppModelsInput
		DiffTool  *DiffTool
		AppConfig *config.AppConfig
		S3        *mock_s3iface.MockS3API
		Accessor  *mock_objectstatsaccessor.MockAccessor
		Client    *mock_allserieslister.MockAllSeriesListerClient
	}
	testloader.MustStart(t, &env)
	ctx := context.Background()

	fixtures := setupDb(ctx, t, env.Models)

	// Set up parameters
	devices := []*models.Device{fixtures.device1, fixtures.device2, fixtures.device3}
	stat := objectstatproto.ObjectStatEnum_osDAccelerometer

	startDate := time.Date(2021, 9, 2, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2021, 9, 4, 0, 0, 0, 0, time.UTC)
	bucket := "s3bucket"
	prefix := "ksprefix"
	exportId := "exportId"

	// We need to mock out all the calls to KS and to S3
	durationMs := pointer.Int64Ptr(24 * 60 * 60 * 1000)

	// Mock out 2 days worth of KS calls per org, returning at least 1 point so it doesn't get ignored.
	for _, endMs := range []int64{samtime.TimeToMs(startDate.Add(time.Hour * 24)), samtime.TimeToMs(startDate.Add(time.Hour * 48))} {
		env.Accessor.EXPECT().GetDeviceBatchObjectStats(ctx, models.DeviceBatchFromSlice(devices), objectstatsaccessor.ObjectStatArgs{
			StatTypeEnum: &stat,
			EndTime:      pointer.Int64Ptr(endMs),
			Duration:     durationMs,
		}).Return(map[batch.Index][]*gqlobjstat.ObjStatDatapoint{}, nil)
	}

	// Mock out list series call
	timeMs := samtime.TimeToMs(startDate) + 100
	series := []*kinesisstatsproto.SeriesWithTimeMs{
		makeSeries(fixtures.device1, timeMs),
		makeSeries(fixtures.device2, timeMs),
		makeSeries(fixtures.device3, timeMs),
		makeSeries(fixtures.device4, timeMs),
	}
	env.Client.EXPECT().GetSeriesWithTimeMs(
		ctx,
		&kinesisstatsproto.GetSeriesRequest{
			Kinds: []kinesisstatsproto.SeriesKind{kinesisstatsproto.SeriesKind_ObjectStat},
			TagFilters: map[string]*kinesisstatsproto.GetSeriesRequest_TagValues{
				string(s3stats.StatTypeTagKey): {Values: []string{strconv.FormatInt(int64(objectstatproto.ObjectStatEnum_osDAccelerometer), 10)}},
			},
		},
	).Return(&kinesisstatsproto.GetSeriesWithTimeMsResponse{SeriesList: &kinesisstatsproto.SeriesWithTimeMsList{List: series}}, nil)

	// To actually assert we are writing to the correct place in s3, we need to write a custom matcher
	// that will only check the Bucket/Key part of the s3 input args and not the input bytes. I don't want
	// to do this right now so just asserting that the correct number of files are written.
	env.S3.EXPECT().PutObjectWithContext(gomock.Any(), gomock.Any()).Times(2)

	orgDeviceMap, err := env.DiffTool.getCustomerAndSpecifiedDevices(ctx, []int64{})
	require.NoError(t, err)
	orgDevices, err := env.DiffTool.fetchAndWriteStat(ctx, time.Now(), stat, orgDeviceMap, bucket, prefix, exportId, startDate, endDate, 3, []int64{})
	assert.NoError(t, err)
	assert.Equal(t, 3, len(orgDevices))
}

func TestChunkDevices(t *testing.T) {
	devices := []*models.Device{
		{Name: "1"},
		{Name: "2"},
		{Name: "3"},
		{Name: "4"},
		{Name: "5"},
	}

	testCases := []struct {
		description string
		devices     []*models.Device
		expected    [][]*models.Device
		chunksize   int
	}{
		{
			description: "nil",
			chunksize:   2,
		},
		{
			description: "empty",
			devices:     []*models.Device{},
			chunksize:   2,
		},
		{
			description: "smaller than 1 chunk",
			devices:     devices[0:1],
			expected:    [][]*models.Device{devices[0:1]},
			chunksize:   2,
		},
		{
			description: "exactly 1 chunk",
			devices:     devices[0:2],
			expected:    [][]*models.Device{devices[0:2]},
			chunksize:   2,
		},
		{
			description: "many chunks",
			devices:     devices,
			expected: [][]*models.Device{
				devices[0:2],
				devices[2:4],
				devices[4:5],
			},
			chunksize: 2,
		},
		{
			description: "many chunks, odd chunksize",
			devices:     devices,
			expected: [][]*models.Device{
				devices[0:2],
				devices[2:4],
				devices[4:5],
			},
			chunksize: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			assert.Equal(t, tc.expected, models.ChunkDevices(tc.devices, tc.chunksize))
		})
	}
}

func TestReplaceValueWithString(t *testing.T) {
	snapshotter := snapshotter.New(t)
	defer snapshotter.Verify()

	output := cf2jsonlschema.ObjectStatOutput{
		Date:       "",
		StatType:   1,
		OrgId:      2,
		ObjectType: 3,
		ObjectId:   4,
		Time:       5,
		Value: &cf2jsonlschema.ObjectStatValue{
			Date:        "",
			Time:        5,
			IsStart:     false,
			IsEnd:       false,
			IsDataBreak: false,
			IntValue:    1,
			DoubleValue: 0,
			ProtoValue: &vgmcuobjectstatproto.VgMcuMetrics{
				MetricsMcuTimeMs:   100,
				SerialNumberString: "hello",
			},
		},
	}

	resultMap, err := replaceValueWithString(output)
	require.NoError(t, err)
	outputString, err := json.Marshal(resultMap)
	require.NoError(t, err)
	snapshotter.Snapshot("replacing entire value with stringified value", fmt.Sprintf("%s", outputString))
}
