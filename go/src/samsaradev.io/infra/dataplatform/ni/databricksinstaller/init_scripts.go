package databricksinstaller

import "fmt"

const (
	dataPlatformDeployedArtifactsVolumePath = "/Volumes/s3/dataplatform-deployed-artifacts"

	Spark3RulesInstaller           = "sparkrules_2.12-8e2a48256da32c226f29ddad1e1e9439bf44277a-SNAPSHOT.jar.sh"
	ApacheSedonaInstaller          = "sedona-shaded-1.4.1-init.sh"
	SymlinkTeamWorkspacesInstaller = "symlink-team-workspaces.sh"
	// Only for installing on UC clusters. Init script for non-UC clusters has region-specific name.
	UCSymlinkSharedPythonInstaller = "symlink-shared-python.sh"
	// Init script that adds datadog monitoring to every Databricks cluster on startup. It is created by running
	// this script: https://docs.databricks.com/_static/notebooks/datadog-init-script.html.
	// A copy of this script can be found in this repo in go/src/samsaradev.io/infra/dataplatform/data/datadog/datadog-install-driver-only.sh
	// The script is not managed in Terraform because it contains a secret.
	DatadogInstaller = "datadog-install-driver-only.sh"
	// Init script that installs system packages that are required for R package installations such as 'sf' and 'units':
	// libudunits2-dev, libgdal-dev, libgeos-dev, libproj-dev, libfontconfig1-dev, libcairo2-dev, libprotobuf-dev, libjq-dev, libv8-dev
	RlibInstaller = "rlib-install.sh"
	// Init script that installs gdal.
	GdalInstaller       = "install_gdal.sh"
	TippecanoeInstaller = "install_tippecanoe.sh"

	// Init script that installs Mosaic.
	MosaicInstaller = "mosaic-0.4.0-dependencies-init.sh"
)

func BigQueryCredentialsInitScript() string {
	return fmt.Sprintf("%s/init_scripts/load-bigquery-credentials.sh", dataPlatformDeployedArtifactsVolumePath)
}

// Likely DO NOT USE THIS FUNCTION: Use GetSparkRulesInitScript instead for anything running on UC.
func GetSparkRulesInitScript_OLD_DO_NOT_USE(regionPrefix string) string {
	return fmt.Sprintf("s3://%s/init_scripts/%s", regionPrefix+"dataplatform-deployed-artifacts", Spark3RulesInstaller)
}

func GetSparkRulesInitScript() string {
	return fmt.Sprintf("%s/init_scripts/%s", dataPlatformDeployedArtifactsVolumePath, Spark3RulesInstaller)
}

// For Unity Catalog enabled clusters.
func GetInitScriptVolumeDestination(installer string) string {
	return fmt.Sprintf("%s/init_scripts/%s", dataPlatformDeployedArtifactsVolumePath, installer)
}
