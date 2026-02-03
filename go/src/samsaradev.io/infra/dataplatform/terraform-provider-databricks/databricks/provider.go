package databricks

import (
	"log"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/terraform"

	"samsaradev.io/infra/dataplatform/databricks"
)

func Provider() terraform.ResourceProvider {
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"host": {
				Type:     schema.TypeString,
				Required: true,
			},
			"client_id": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("DATABRICKS_CI_CLIENT_ID", nil),
				Sensitive:   true,
			},
			"client_secret": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("DATABRICKS_CI_CLIENT_SECRET", nil),
				Sensitive:   true,
			},
			"token": {
				Type:        schema.TypeString,
				Required:    true,
				DefaultFunc: schema.EnvDefaultFunc("DATABRICKS_TOKEN", nil),
				Sensitive:   true,
			},
		},
		ResourcesMap: map[string]*schema.Resource{ // lint: sorted
			"databricks_cluster_libraries":            resourceClusterLibraries(),
			"databricks_cluster":                      resourceCluster(),
			"databricks_cluster_policy":               resourceClusterPolicy(),
			"databricks_directory":                    resourceDirectory(),
			"databricks_group":                        resourceGroup(),
			"databricks_instance_pool":                resourceInstancePool(),
			"databricks_instance_profile":             resourceInstanceProfile(),
			"databricks_ip_access_list":               resourceIPAccessList(),
			"databricks_job":                          resourceJob(),
			"databricks_notebook":                     resourceNotebook(),
			"databricks_permissions":                  resourcePermissions(),
			"databricks_sql_dashboard":                resourceSqlDashboard(),
			"databricks_sql_dashboard_schedule":       resourceSqlDashboardSchedule(),
			"databricks_sql_endpoint":                 resourceSqlEndpoint(),
			"databricks_sql_query":                    resourceSqlQuery(),
			"databricks_sql_visualization":            resourceSqlVisualization(),
			"databricks_sql_widget":                   resourceSqlWidget(),
			"databricks_user":                         resourceUser(),
			"databricks_workspace_conf":               resourceWorkspaceConf(),
			"databricks_cloud_credential":             resourceCloudCredential(),
			"databricks_cloud_credential_permissions": resourceCloudCredentialPermissions(),
			"databricks_workspace_file":               resourceFile(),
		},
		DataSourcesMap: map[string]*schema.Resource{
			"databricks_user": dataSourceUser(),
		},
		ConfigureFunc: configure,
	}
}

func configure(data *schema.ResourceData) (interface{}, error) {
	host := data.Get("host").(string)
	token := data.Get("token").(string)

	log.Printf("[INFO] Configuring Databricks client with host: %s", host)

	clientID := data.Get("client_id").(string)
	clientSecret := data.Get("client_secret").(string)

	// If client_id and client_secret are set, use the V2 API Client Credentials flow.
	if clientID != "" && clientSecret != "" {
		log.Printf("[INFO] Using V2 API Client Credentials flow with oauth")
		return databricks.NewV2(host, clientID, clientSecret)
	}

	// Otherwise, use the V1 API Client Personal Access Token flow.
	log.Printf("[INFO] Using V1 API Client Personal Access Token flow")
	return databricks.New(host, token)
}
