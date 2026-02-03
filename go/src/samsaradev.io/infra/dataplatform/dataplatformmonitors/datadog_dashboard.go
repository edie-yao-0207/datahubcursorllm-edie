package dataplatformmonitors

import (
	"fmt"
	"sort"
	"strings"
)

type dataPlatformDatadogDashboard string

const ( // lint: +sorted
	dataplatIngestionDashboard dataPlatformDatadogDashboard = "https://app.datadoghq.com/dashboard/iti-zs8-e2c"
	datastreamsDashboard       dataPlatformDatadogDashboard = "https://app.datadoghq.com/dashboard/2a8-jiq-ykb"
	mergeJobRunsDashboad       dataPlatformDatadogDashboard = "https://app.datadoghq.com/dashboard/yvi-nwc-k6x"
	sparkInfraDashboard        dataPlatformDatadogDashboard = "https://app.datadoghq.com/dashboard/vyb-9y8-jgv"
	dynamodbdeltalakeDashboard dataPlatformDatadogDashboard = "https://app.datadoghq.com/dashboard/9ig-un7-kep"
)

type dataPlatformDatadogDashboardLink struct {
	dashboardLink dataPlatformDatadogDashboard
	variables     map[string]string
}

func (d dataPlatformDatadogDashboardLink) generateDashboardLink() string {
	var variableArgs []string
	for variableKey, variableValue := range d.variables {
		variable := fmt.Sprintf("tpl_var_%s=%s", variableKey, variableValue)
		variableArgs = append(variableArgs, variable)
	}
	// Sort so that variableArgs are deterministic, otherwise Terraform output will differ every time.
	sort.Strings(variableArgs)
	var variablesSuffix string
	if len(variableArgs) > 0 {
		variablesSuffix = fmt.Sprintf("?%s", strings.Join(variableArgs, "&"))
	}
	return fmt.Sprintf("%s%s", string(d.dashboardLink), variablesSuffix)
}
