package dataplatformmonitors

import (
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/emitters"
	"samsaradev.io/infra/app/generate_terraform/tf"
)

var tableExistenceMonitors = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource
	var tableExistenceMonitorsRunbook = "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4948748/Runbook+Table+Existence+Monitors"

	singleFileTableFilesizeMonitor, err := createDataPlatformMonitor(dataPlatformMonitor{
		Name:              "Table existence monitor: Single file table under the filesize threshold",
		Message:           "File backing a single file table is under the filesize threshold.",
		Query:             "sum(last_2h):sum:dagster_table_monitors.single_file_table.under_filesize_threshold{*} by {database,table,region}.as_count() > 0",
		RunbookLink:       tableExistenceMonitorsRunbook,
		RequireFullWindow: false,
		NotifyNoData:      false,
		Severity:          lowSeverity, // TODO: Update all the table existence monitors to high severity after monitoring for a week.
	})

	if err != nil {
		return nil, oops.Wrapf(err, "error creating singleFileTableFilesizeMonitor")
	}
	monitors = append(monitors, singleFileTableFilesizeMonitor)

	singleFileTableNotFoundMonitor, err := createDataPlatformMonitor(dataPlatformMonitor{
		Name:              "Table existence monitor: Single file table not found",
		Message:           "File backing a single file table was not found.",
		Query:             "sum(last_2h):sum:dagster_table_monitors.single_file_table.not_found{*} by {database,table,region}.as_count() > 0",
		RunbookLink:       tableExistenceMonitorsRunbook,
		RequireFullWindow: false,
		NotifyNoData:      false,
		Severity:          lowSeverity,
	})

	if err != nil {
		return nil, oops.Wrapf(err, "error creating singleFileTableNotFoundMonitor")
	}
	monitors = append(monitors, singleFileTableNotFoundMonitor)

	deltaTableZeroRowsMonitor, err := createDataPlatformMonitor(dataPlatformMonitor{
		Name:              "Table existence monitor: Delta table has zero rows",
		Message:           "Delta table has zero rows but expected >0",
		Query:             "sum(last_2h):sum:dagster_table_monitors.delta_table.zero_rows{*} by {database,table,region}.as_count() > 0",
		RunbookLink:       tableExistenceMonitorsRunbook,
		RequireFullWindow: false,
		NotifyNoData:      false,
		Severity:          lowSeverity,
	})

	if err != nil {
		return nil, oops.Wrapf(err, "error creating deltaTableZeroRowsMonitor")
	}
	monitors = append(monitors, deltaTableZeroRowsMonitor)

	deltaTableNotFoundMonitor, err := createDataPlatformMonitor(dataPlatformMonitor{
		Name:              "Table existence monitor: Delta table not found",
		Message:           "Delta table not found",
		Query:             "sum(last_2h):sum:dagster_table_monitors.delta_table.not_found{*} by {database,table,region}.as_count() > 0",
		RunbookLink:       tableExistenceMonitorsRunbook,
		RequireFullWindow: false,
		NotifyNoData:      false,
		Severity:          lowSeverity,
	})

	if err != nil {
		return nil, oops.Wrapf(err, "error creating deltaTableNotFoundMonitor")
	}
	monitors = append(monitors, deltaTableNotFoundMonitor)

	return monitors, nil
})
