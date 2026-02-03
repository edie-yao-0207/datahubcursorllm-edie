package dataplatformprojects

import (
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/libs/ni/infraconsts"
)

func IPAllowList(providerGroup string) (map[string][]tf.Resource, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	var allowedIPs []string
	switch config.Region {
	case infraconsts.SamsaraAWSDefaultRegion:
		allowedIPs = getUSIPAllowList()
	case infraconsts.SamsaraAWSEURegion:
		allowedIPs = getEUIPAllowList()
	case infraconsts.SamsaraAWSCARegion:
		allowedIPs = getCAIPAllowList()
	default:
		return nil, oops.Errorf("%s region is not supported", config.Region)
	}

	ipAccessListResource := databricksresource.IPAccessList{
		Label:       "samsara-ip-allow-list",
		ListType:    databricks.AllowIPAccess,
		IPAddresses: allowedIPs,
		Enabled:     true,
	}

	return map[string][]tf.Resource{
		"ip_access_list": {&ipAccessListResource},
	}, nil
}

func getUSIPAllowList() []string {
	ips := []string{
		// TODO: Revisit the BizTech System IPs after their migration to biztech workspace.

		// Fivetran US Oregon & US N Virginia
		// Fivetran docs instructions say to whitelist all regions
		// https://fivetran.com/docs/getting-started/ips
		// Fivetran on GCP - TODO: delete once we move all Fivetran configs to use Fivetran on AWS
		"35.227.135.0/29",
		"35.234.176.144/29",
		"52.0.2.4/32",
		// Fivetran on AWS
		"35.80.36.104/29",
		"3.239.194.48/29",
		"52.0.2.4/32",

		// us-west-2 nat gateway elastic ips for sparkreportmetricsworker
		"35.166.166.111", // prod-us-west-2a-nat
		"34.209.175.36",  // prod-us-west-2b-nat
		"52.32.199.170",  // prod-us-west-2c-nat

		"52.24.116.200",  // prod-v2-us-west-2a-nat
		"44.246.241.200", // prod-v2-us-west-2b-nat
		"54.201.176.250", // prod-v2-us-west-2c-nat
		"54.68.79.115",   // prod-v2-us-west-2d-nat

		// New VPC for SCC in us-west-2 for Databricks Clusters
		"52.36.21.170",  // dev-v2-us-west-2a-nat
		"54.244.90.100", // dev-v2-us-west-2b-nat
		"52.12.66.107",  // dev-v2-us-west-2c-nat
		"44.224.70.58",  // dev-v2-us-west-2d-nat

		// new us-west-2 nat gateway entries as of 3/6/2024
		"35.162.92.141",
		"35.165.161.181",
		"44.227.73.184",

		// us-west-2 nat gateway elastic ips for Data Pipelines Lambdas
		"44.238.79.52",  // datapipelines-lambdas-us-west-2a-nat
		"44.236.201.23", // datapipelines-lambdas-us-west-2b-nat
		"44.240.5.244",  // datapipelines-lambdas-us-west-2c-nat

		// Matik
		// A tool that generates customer presentation by pulling data from various sources
		// It needs to access Databricks to be able to read the data in databricks so Matik users at the company can easily generate presentations
		// It makes requests directly to Databricks
		"54.177.196.112",
		"54.219.155.184",

		// Dojo account NAT elastic IPs for ZScaler VPC
		"52.27.40.118", // zscaler-uw2a-nat-ip
		"52.41.8.52",   // zscaler-uw2b-nat-ip
		"52.89.25.127", // zscaler-uw2c-nat-ip

		// Dojo account NAT elastic IPs for ZScaler VPC for EU
		"52.16.203.121",  // ray-zscaler-eu-nat-euw1a
		"54.195.125.209", // ray-zscaler-eu-nat-euw1b

		// airflow-safety NAT elastic IPs
		"35.161.26.135", // airflow-safety-us-west-2a-nat
		"52.36.130.148", // airflow-safety-us-west-2b-nat

		// dbt ips
		"52.45.144.63",
		"54.81.134.249",
		"52.22.161.231",

		// Mulesoft ips
		"3.128.235.211",
		"3.18.216.158",
		"18.118.237.219",
		"18.119.34.182",

		// Dagster EKS VPC Subets
		// Dagster K8s makes requests to databricks api to submit runs and obtain run status
		"35.167.55.1",  // dagster-us-west-2a-eip-nat
		"34.216.88.56", // dagster-us-west-2b-eip-nat
		"52.38.67.177", // dagster-us-west-2c-eip-nat

		// Data Science Prod account NAT gateway elastic IPs for us-west-2
		// These are used by ProductGPT and other services that query Databricks
		"54.186.243.85",  // prod-us-west-2a-nat-eip
		"52.88.194.195",  // prod-us-west-2b-nat-eip
		"54.203.130.117", // prod-us-west-2c-nat-eip
		"52.26.22.23",    // prod-us-west-2d-nat-eip

		//Biztech AWS self hosted runner (Actions) to deploy asset bundles to Databricks
		"34.212.186.28",

		// Allow support automation app to make requests to the Databricks API.
		"52.12.29.125",
		"44.239.40.106",

		// SIEM connection
		"44.230.117.45",     // Splunk HF01
		"52.161.37.112/28",  // Actions runner for samsara-security-detections
		"52.160.167.176/28", // Actions runner for samsara-security-detections

		// Databricks Cluster VPC NATs.
		"44.231.177.174", // dev-staging-us-west-2a.
		"44.228.170.42",  // dev-staging-us-west-2b.
		"54.188.131.234", //dev-staging-us-west-2c.

		// Cloudzero fixed IP for Databricks system table access
		"52.0.118.180",
		"52.0.33.111",

		// R1 Automated Testing Setup - STCE Hardware
		"10.19.5.125",

		// PRODSEC-9656: AWS Elastic Beanstalk - MAAD AI webapp
		"16.58.109.25",

		// TODO Add NAT IP for samsara-dev-us-west-2 once moved to SCC with new VPCs.

	}
	// Add Samsara Office & VPN IPs
	ips = append(ips, infraconsts.OfficeCidrs...)
	ips = append(ips, infraconsts.DevHomeCidrs...)
	// Add Okta IPs which are used for Okta to use SCIM API for managing users
	ips = append(ips, infraconsts.OktaIPv4Cidrs...)
	// Buildkite IP address
	// The databricks-deploy pipeline needs to be able to get information about instance pools/our databricks account to correctly populate
	// the job spec for our databricks jobs
	ips = append(ips, infraconsts.AwsBuildkiteNatGatewayIPs...)
	return ips
}

func getEUIPAllowList() []string {
	ips := []string{
		// Tableau US West Oregon 10az host name
		// https://help.tableau.com/current/pro/desktop/en-us/publish_tableau_online_ip_authorization.htm
		"155.226.128.0/21",

		// No teams are currently doing Fivetran Replication in EU. Add IPs here if
		// teams split out Fivetran replication by region.

		// eu-west-1 nat gateway elastic ips for sparkreportmetricsworker
		"52.215.215.115", // prod-eu-west-1a-nat
		"52.215.210.106", // prod-eu-west-1b-nat
		"34.249.151.126", // prod-eu-west-1c-nat

		// New VPC for SCC in eu-west-1 for Databricks Clusters
		"54.247.85.116",  // dev-v2-eu-west-1a-nat
		"52.211.174.194", // dev-v2-eu-west-1b-nat
		"18.200.226.69",  // dev-v2-eu-west-1c-nat

		// eu-west-1 Databricks AWAS Lambdas VPC for pipeline lambdas
		"54.74.236.106", // datapipelines-lambdas-eu-west-1a-nat
		"54.195.68.109", // datapipelines-lambdas-eu-west-1b-nat
		"52.31.16.64",   // datapipelines-lambdas-eu-west-1c-nat

		// Dojo account NAT elastic IPs for ZScaler VPC
		"52.27.40.118", // zscaler-uw2a-nat-ip
		"52.41.8.52",   // zscaler-uw2b-nat-ip
		"52.89.25.127", // zscaler-uw2c-nat-ip

		// Dojo account NAT elastic IPs for ZScaler VPC for EU
		"52.16.203.121",  // ray-zscaler-eu-nat-euw1a
		"54.195.125.209", // ray-zscaler-eu-nat-euw1b

		// Matik
		// A tool that generates customer presentation by pulling data from various sources
		// It needs to access Databricks to be able to read the data in databricks so Matik users at the company can easily generate presentations
		// It makes requests directly to Databricks
		"54.177.196.112",
		"54.219.155.184",

		// Dagster ****US**** EKS VPC Subets
		// Dagster K8s makes requests to databricks api to submit runs and obtain run status
		// We need to allowlist the US IP addresses to allow the Dagster deployment to launch EU clusters
		"35.167.55.1",  // dagster-us-west-2a-eip-nat
		"34.216.88.56", // dagster-us-west-2b-eip-nat
		"52.38.67.177", // dagster-us-west-2c-eip-nat

		// dbt ips to connect to databricks eu
		"52.45.144.63",
		"54.81.134.249",
		"52.22.161.231",

		// Allow support automation app to make requests to the Databricks API.
		"54.203.176.28",
		"44.239.40.106",

		// SIEM connection
		"44.230.117.45",     // Splunk HF01
		"52.161.37.112/28",  // Actions runner for samsara-security-detections
		"52.160.167.176/28", // Actions runner for samsara-security-detections

		// TODO Add NAT IP for samsara-dev-eu-west-1 once moved to SCC with new VPCs.
	}

	// Add Samsara Office & VPN IPs
	ips = append(ips, infraconsts.OfficeCidrs...)
	ips = append(ips, infraconsts.DevHomeCidrs...)
	// Add Okta IPs which are used for Okta to use SCIM API for managing users
	ips = append(ips, infraconsts.OktaIPv4Cidrs...)
	// Buildkite IP address
	// The databricks-deploy pipeline needs to be able to get information about instance pools/our databricks account to correctly populate
	// the job spec for our databricks jobs
	ips = append(ips, infraconsts.AwsBuildkiteNatGatewayIPs...)
	return ips
}

func getCAIPAllowList() []string {
	ips := []string{
		// Tableau US West Oregon 10az host name
		// https://help.tableau.com/current/pro/desktop/en-us/publish_tableau_online_ip_authorization.htm
		"155.226.128.0/21",

		// No teams are currently doing Fivetran Replication in CA. Add IPs here if
		// teams split out Fivetran replication by region.

		// ca-central-1 nat gateway elastic ips for sparkreportmetricsworker
		"16.52.130.177",  // prod-ca-central-1d-nat
		"15.157.179.242", // prod-ca-central-1a-nat
		"3.98.186.192",   // prod-ca-central-1b-nat

		// ca-central-1 Databricks AWAS Lambdas VPC for pipeline lambdas.
		"3.96.254.116",  // datapipelines-lambdas-ca-central-1a-nat
		"3.96.37.80",    // datapipelines-lambdas-ca-central-1b-nat
		"15.156.232.71", // datapipelines-lambdas-ca-central-1d-nat

		// Dojo account NAT elastic IPs for ZScaler VPC.
		"52.27.40.118", // zscaler-uw2a-nat-ip.
		"52.41.8.52",   // zscaler-uw2b-nat-ip.
		"52.89.25.127", // zscaler-uw2c-nat-ip.

		// Matik
		// A tool that generates customer presentation by pulling data from various sources
		// It needs to access Databricks to be able to read the data in databricks so Matik users at the company can easily generate presentations
		// It makes requests directly to Databricks.
		"54.177.196.112",
		"54.219.155.184",

		// Dagster ****US**** EKS VPC Subets
		// Dagster K8s makes requests to databricks api to submit runs and obtain run status
		// We need to allowlist the US IP addresses to allow the Dagster deployment to launch CA clusters.
		"35.167.55.1",  // dagster-us-west-2a-eip-nat.
		"34.216.88.56", // dagster-us-west-2b-eip-nat.
		"52.38.67.177", // dagster-us-west-2c-eip-nat.

		// dbt ips to connect to databricks ca.
		"52.45.144.63",
		"54.81.134.249",
		"52.22.161.231",

		// Allow support automation app to make requests to the Databricks API.
		"44.239.40.106",

		// SIEM connection
		"44.230.117.45",     // Splunk HF01
		"52.161.37.112/28",  // Actions runner for samsara-security-detections
		"52.160.167.176/28", // Actions runner for samsara-security-detections

		// Databricks Cluster VPC NATs.
		"3.99.83.81",  // dev-ca-central-1a-nat.
		"3.98.212.20", // dev-ca-central-1b-nat.
		"3.97.89.99",  // dev-ca-central-1d-nat.

	}

	// Add Samsara Office & VPN IPs.
	ips = append(ips, infraconsts.OfficeCidrs...)
	ips = append(ips, infraconsts.DevHomeCidrs...)
	// Add Okta IPs which are used for Okta to use SCIM API for managing users
	ips = append(ips, infraconsts.OktaIPv4Cidrs...)
	// Buildkite IP address
	// The databricks-deploy pipeline needs to be able to get information about instance pools/our databricks account to correctly populate
	// the job spec for our databricks jobs.
	ips = append(ips, infraconsts.AwsBuildkiteNatGatewayIPs...)
	return ips
}
