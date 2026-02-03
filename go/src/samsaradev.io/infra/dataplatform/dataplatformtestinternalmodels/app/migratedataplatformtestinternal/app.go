package main

import (
	"go.uber.org/fx"

	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dbtools/dbversionstate"
	"samsaradev.io/infra/releasemanagement/launchdarkly"
	"samsaradev.io/infra/serviceorchestration/leaderelection"
)

// migrateApp defines an app which runs migrations and backfills.
type migrateApp struct {
	Config              *config.AppConfig
	waitForLeaderHelper leaderelection.WaitForLeaderLifecycleHookHelper
	dbState             dbversionstate.DBState
	launchDarklyBridge  *launchdarkly.SDKBridge

	// Add dependencies for running backfills here.
}

type params struct {
	fx.In

	Config              *config.AppConfig
	WaitForLeaderHelper leaderelection.WaitForLeaderLifecycleHookHelper
	DBState             dbversionstate.DBState
	LaunchDarklyBridge  *launchdarkly.SDKBridge
}

func new(p params) *migrateApp {
	return &migrateApp{
		Config:              p.Config,
		waitForLeaderHelper: p.WaitForLeaderHelper,
		dbState:             p.DBState,
		launchDarklyBridge:  p.LaunchDarklyBridge,
	}
}
