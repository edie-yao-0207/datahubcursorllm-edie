package dataplatformresource

import (
	"fmt"

	"samsaradev.io/libs/ni/infraconsts"
)

func DatabricksAlertsDomain(region string) string {
	return fmt.Sprintf("%d.databricks-alerts.samsara.com", infraconsts.GetDatabricksAccountIdForRegion(region))
}

func DatabricksAlertsSender(region string) string {
	return fmt.Sprintf("no-reply@%s", DatabricksAlertsDomain(region))
}
