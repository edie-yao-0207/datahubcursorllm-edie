package databricks

import (
	"fmt"
)

// BigQueryCredentialsInitScript_OLD_DO_NOT_USE returns the S3 URL to an init script that
// installs BigQuery API credentials to
// "/databricks/samsara-data-5142c7cd3ba2.json". The init script is not managed
// in Terraform because it contains secret. It's manually uploaded to both
// us-west-2 and eu-west-1 regions. The credential user is
// samsara-bigquery-spark-connect@samsara-data.iam.gserviceaccount.com.
// Likely DO NOT USE THIS FUNCTION: Use BigQueryCredentialsInitScript instead for anything running on UC.
func BigQueryCredentialsInitScript_OLD_DO_NOT_USE(regionPrefix string) string {
	return fmt.Sprintf("s3://%s/init_scripts/load-bigquery-credentials.sh", regionPrefix+"dataplatform-deployed-artifacts")
}

func BigQueryCredentialsLocation() string {
	// Hard-coded in init script.
	return "/databricks/samsara-data-5142c7cd3ba2.json"
}

// For NON-Unity Catalog enabled clusters.
// Likely DO NOT USE THIS FUNCTION: Use GetInitScriptVolumeDestination instead for anything running on UC.
func GetInitScriptS3Destination_OLD_DO_NOT_USE(installer string, regionPrefix string) string {
	return fmt.Sprintf("s3://%s/init_scripts/%s", regionPrefix+"dataplatform-deployed-artifacts", installer)
}
