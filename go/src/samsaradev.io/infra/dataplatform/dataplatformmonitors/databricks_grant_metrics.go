package dataplatformmonitors

import (
	"fmt"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/emitters"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/libs/ni/infraconsts"
)

var databricksGrantMetricsMonitorHighSeverity = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	// This alerts if there's a pronounced decrease in grants in the default catalog
	var monitors []tf.Resource
	for _, region := range []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion} {
		monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:                     fmt.Sprintf("[%s] Databricks groups lost access to some or all default catalog schemas", region),
			Message:                  "databricks.grants.groups.num_distinct_schemas_granted has decreased more than 10%. num_distinct_schemas_granted has a group tag to pinpoint the specific group(s) affected. The drop may be (un)intentional. Further investigation is warranted.",
			Query:                    fmt.Sprintf("pct_change(avg(last_240m),last_240m):avg:databricks.grants.groups.num_distinct_schemas_granted{region:%s, catalog:default} < -10", region),
			Severity:                 getSeverityForRegion(region, businessHoursSeverity),
			RunbookLink:              "https://samsaradev.atlassian.net/wiki/spaces/RD/pages/4369481896",
			RequireFullWindow:        false,
			NotifyNoData:             true,
			NoDataTimeframeInMinutes: 180,
		})
		if err != nil {
			return nil, oops.Wrapf(err, "failed to create databricksjobmetric")
		}
		monitors = append(monitors, monitor)
	}

	// This alerts if there's a pronounced decrease in grants in the non_govramp_customer_data catalog
	for _, region := range []string{infraconsts.SamsaraAWSDefaultRegion} {
		monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:                     fmt.Sprintf("[%s] Databricks groups lost access to some or all non_govramp_customer_data catalog tables", region),
			Message:                  "databricks.grants.groups.num_distinct_tables_granted has decreased more than 10%. num_distinct_tables_granted has a group tag to pinpoint the specific group(s) affected. The drop may be (un)intentional. Further investigation is warranted.",
			Query:                    fmt.Sprintf("pct_change(avg(last_240m),last_240m):avg:databricks.grants.groups.num_distinct_tables_granted{region:%s, catalog:non_govramp_customer_data} < -10", region),
			Severity:                 getSeverityForRegion(region, businessHoursSeverity),
			RunbookLink:              "https://samsaradev.atlassian.net/wiki/spaces/RD/pages/4369481896",
			RequireFullWindow:        false,
			NotifyNoData:             true,
			NoDataTimeframeInMinutes: 180,
		})
		if err != nil {
			return nil, oops.Wrapf(err, "failed to create databricksjobmetric")
		}
		monitors = append(monitors, monitor)
	}

	return monitors, nil
})
