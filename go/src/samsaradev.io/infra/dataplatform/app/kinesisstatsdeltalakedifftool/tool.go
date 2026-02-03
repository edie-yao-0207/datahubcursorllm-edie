package kinesisstatsdeltalakedifftool

import (
	"context"
	"flag"
	"strconv"
	"strings"
	"time"

	"github.com/samsarahq/go/oops"
	"go.uber.org/fx"

	"samsaradev.io/helpers/slog"
	"samsaradev.io/hubproto/objectstatproto"
	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dataplatform/ksdeltalake"
	"samsaradev.io/infra/dataplatform/ksdeltalake/difftool"
	"samsaradev.io/infra/serviceorchestration/leaderelection"
	"samsaradev.io/system"
)

func buildDefaultParams() difftool.DiffToolParams {
	statMap := make(map[objectstatproto.ObjectStatEnum]struct{})

	// Add all production stats
	ksProdTables := ksdeltalake.AllProductionTables()
	for _, table := range ksProdTables {
		// Skip over location (i.e non object stats) since we can't fetch those from KS normally
		if table.StatKind != ksdeltalake.StatKindObjectStat {
			continue
		}
		statMap[table.StatType] = struct{}{}
	}

	// Append extra stats we want to track for various reasons
	extraStats := []objectstatproto.ObjectStatEnum{
		// Stats with known uint64 issues
		objectstatproto.ObjectStatEnum_osDAccelerometer,
		objectstatproto.ObjectStatEnum_osDModiDeviceInfo,
		objectstatproto.ObjectStatEnum_osDDashcamDriverObstruction,
		objectstatproto.ObjectStatEnum_osDDashcamSeatbelt,
		objectstatproto.ObjectStatEnum_osDNVR10CameraInfo,
		objectstatproto.ObjectStatEnum_osDVgMcuMetrics,

		// Stats that have likely issues with overwrites
		objectstatproto.ObjectStatEnum_osDDerivedGpsDistance,
		objectstatproto.ObjectStatEnum_osDHubServerDeviceHeartbeat,

		// Stat we are starting to roll out overwrites code changes to
		objectstatproto.ObjectStatEnum_osDVehicleCurrentGear,

		// Stats we are using the new s3filestable for
		objectstatproto.ObjectStatEnum_osTDataPlatformInternalTestStat,
		objectstatproto.ObjectStatEnum_osDCableUpgrade,
		objectstatproto.ObjectStatEnum_osDCanConnected,
		objectstatproto.ObjectStatEnum_osDGatewayPlugged,
		objectstatproto.ObjectStatEnum_osDMarathonEvent,
		objectstatproto.ObjectStatEnum_osDOctoDisplayConnected,
		objectstatproto.ObjectStatEnum_osDRawVins,
		objectstatproto.ObjectStatEnum_osDTireConditionData,
		objectstatproto.ObjectStatEnum_osDWgfiConnected,
		objectstatproto.ObjectStatEnum_osDEngineState,

		// Stats relevant to ELD and VDP coverage data
		objectstatproto.ObjectStatEnum_osDSeatbeltDriver,

		// Stats we've seen discrepancies with
		objectstatproto.ObjectStatEnum_osDDerivedTotalAssetEngineMs,
	}

	for _, stat := range extraStats {
		statMap[stat] = struct{}{}
	}

	stats := make([]objectstatproto.ObjectStatEnum, 0, len(statMap))
	for stat := range statMap {
		stats = append(stats, stat)
	}

	return difftool.DiffToolParams{
		// Run this diff over data from a couple days ago. This likely gives us a good window to account for
		// late data, failures, etc.
		StartDate:               time.Now().UTC().Truncate(24 * time.Hour).Add(-1 * 2 * 24 * time.Hour),
		EndDate:                 time.Now().UTC().Truncate(24 * time.Hour).Add(-1 * 1 * 24 * time.Hour),
		DeviceRandomSampleCount: 10000,
		Stats:                   stats,
		ExportPrefix:            "dataplatcron",
	}
}

var defaultParams = buildDefaultParams()

func DiffToolAppMain(useLeaderElection bool) {
	ctx := context.Background()

	// We allow passing command line arguments to this app, and otherwise use defaults. This lets us run the app as a
	// cron (w/o arguments since the cron setup has no place for arguments to be provided) and also manually kick off
	// ecs tasks with custom arguments.
	var startDate, endDate, prefix, stats, orgIds string
	var randomSampleCount int

	flag.StringVar(&startDate, "startDate", "", "start date to diff from")
	flag.StringVar(&endDate, "endDate", "", "start date to diff from")
	flag.StringVar(&prefix, "prefix", "", "prefix")
	flag.StringVar(&stats, "stats", "", "comma separated list of stats")
	flag.StringVar(&orgIds, "orgIds", "", "comma separated list of orgIds")
	flag.IntVar(&randomSampleCount, "randomSampleCount", 0, "")
	flag.Parse()

	args, err := buildParams(startDate, endDate, prefix, stats, orgIds, randomSampleCount)
	if err != nil {
		slog.Fatalw(ctx, "invalid parameters", "error", err)
	}

	var tool *difftool.DiffTool
	var appConfig *config.AppConfig

	var app *fx.App
	if useLeaderElection {
		var helper leaderelection.WaitForLeaderLifecycleHookHelper
		app = system.NewFx(&config.ConfigParams{}, fx.Populate(&tool, &appConfig, &helper))
		if err := app.Start(ctx); err != nil {
			slog.Fatalw(ctx, "failed to start application", "error", err)
		}

		err = helper.BlockUntilLeader(ctx)
		if err != nil {
			slog.Fatalw(ctx, "failed to become leader", "error", err)
		}
	} else {
		app = system.NewFx(&config.ConfigParams{}, fx.Populate(&tool, &appConfig))
		if err := app.Start(ctx); err != nil {
			slog.Fatalw(ctx, "failed to start application", "error", err)
		}
	}

	if appConfig.IsDevEnv() {
		args.Local = true
		args.ExportPrefix = "local"
	}

	if err = tool.Run(*args); err != nil {
		slog.Fatalw(ctx, "failed to run application", "error", err)
	}

	if err = app.Stop(ctx); err != nil {
		slog.Fatalw(ctx, "failed to stop application", "error", err)
	}
}

// Tries to build params from args, or provides the defaults.
// This function errors when the command line params given are not correct.
func buildParams(startDate string, endDate string, prefix string, stats string, orgIds string, randomSampleCount int) (*difftool.DiffToolParams, error) {
	// When no args have been passed, return our default
	if startDate == "" && endDate == "" && prefix == "" && stats == "" {
		if orgIds != "" || randomSampleCount != 0 {
			return nil, oops.Errorf("Cannot pass only orgIds or randomSampleCount, must pass startDate, endDate, prefix, and stats.")
		}

		return &defaultParams, nil
	}

	// Check that all required args have been passed.
	if startDate == "" || endDate == "" || prefix == "" || stats == "" {
		return nil, oops.Errorf("Must pass all of startDate, endDate, prefix, and stats (and optionally orgIds)")
	}

	// At least one of `randomSampleCount` and `orgIds` must be provided
	if randomSampleCount == 0 && orgIds == "" {
		return nil, oops.Errorf("Must pass at least one of randomSampleCount and orgIds")
	}

	layout := "2006-01-02"
	startTime, err := time.Parse(layout, startDate)
	if err != nil {
		return nil, err
	}
	endTime, err := time.Parse(layout, endDate)
	if err != nil {
		return nil, err
	}

	var statEnums []objectstatproto.ObjectStatEnum
	statsArr := strings.Split(stats, ",")
	for _, stat := range statsArr {
		statVal, ok := objectstatproto.ObjectStatEnum_value[stat]
		if !ok {
			return nil, oops.Errorf("Could not find stat for %s\n. Please check spelling and capitalization.", stat)
		}
		statEnums = append(statEnums, objectstatproto.ObjectStatEnum(statVal))
	}

	var parsedOrgIds []int64
	if orgIds != "" {
		orgIdsArr := strings.Split(orgIds, ",")
		for _, orgIdString := range orgIdsArr {
			orgId, err := strconv.Atoi(orgIdString)
			if err != nil {
				return nil, oops.Wrapf(err, "could not parse %s as number", orgIdString)
			}
			parsedOrgIds = append(parsedOrgIds, int64(orgId))
		}
	}

	return &difftool.DiffToolParams{
		FullOrgIds:              parsedOrgIds,
		DeviceRandomSampleCount: int64(randomSampleCount),
		StartDate:               startTime,
		EndDate:                 endTime,
		Stats:                   statEnums,
		ExportPrefix:            prefix,
	}, nil
}
