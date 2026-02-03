package dataplatformresource

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
)

const ServerlessSQLWarehouseAutoStopMinutes = 10

var ServerlessSQLWarehouses = map[string]struct{}{ // lint: +sorted
	"aianddata":                  {},
	"connecteddriver":            {},
	"connectedequipment":         {},
	"corefirmware":               {},
	"cursor-query-agent":         {},
	"engineeringoperations":      {},
	"genie":                      {},
	"godatalake":                 {},
	"gql":                        {},
	"infra":                      {},
	"newmarketsproductgroup":     {},
	"perf-dashboards":            {},
	"platform":                   {},
	"productmanagement":          {},
	"qualityassured":             {},
	"safety-firmware-dashboards": {},
	"safetydevbox":               {},
	"safetyproductgroup":         {},
	"safetyprofiles":             {},
	"safetyrisk":                 {},
	"security":                   {},
	"shared":                     {},
	"support":                    {},
	"tableau-extract":            {},
	"telematics":                 {},
}

type SQLEndpointConfig struct {
	Name                   string
	BillingProductGroup    string
	BillingTeam            string
	Users                  []DatabricksPrincipal
	AutoTerminationMinutes int
	ClusterSize            databricks.ClusterSize
	RnDCostAllocation      float64
}

func SQLEndpoint(c dataplatformconfig.DatabricksConfig, sqlEndpointConfig *SQLEndpointConfig) ([]tf.Resource, error) {
	clusterSize := sqlEndpointConfig.ClusterSize
	if clusterSize == "" {
		clusterSize = databricks.ClusterSizeSmall
	}

	sqlEndpoint := &databricksresource.SqlEndpoint{
		Name:           sqlEndpointConfig.Name,
		ClusterSize:    clusterSize,
		MinNumClusters: 1,
		MaxNumClusters: 4,
		AutoStopMins:   sqlEndpointConfig.AutoTerminationMinutes,
		CustomTags: []databricksresource.ClusterTag{
			{
				Key:   "samsara:product-group",
				Value: strings.ToLower(sqlEndpointConfig.BillingProductGroup),
			},
			{
				Key:   "samsara:service",
				Value: fmt.Sprintf("databrickssql-%s", strings.ToLower(sqlEndpointConfig.Name)),
			},
			{
				Key:   "samsara:rnd-allocation",
				Value: strconv.FormatFloat(sqlEndpointConfig.RnDCostAllocation, 'f', -1, 64),
			},
			{
				Key:   "samsara:dataplatform-job-type",
				Value: string(dataplatformconsts.SqlEndpoint),
			},
			{
				Key:   "samsara:iac-managed",
				Value: "true",
			},
		},
		SpotInstancePolicy: databricks.EndpointSpotInstancePolicyCostOptimized,
		// Photon does not cost extra for SQL endpoints.
		EnablePhoton: true,
	}

	if sqlEndpointConfig.BillingTeam != "" {
		sqlEndpoint.CustomTags = append(sqlEndpoint.CustomTags, databricksresource.ClusterTag{
			Key:   "samsara:team",
			Value: strings.ToLower(sqlEndpointConfig.BillingTeam),
		})
	}

	// TODO: Remove once evaluation of Serverless is completed and the feature is implemented or rolled-back.
	// We are currently testing the feasibility of Serverless SQL Warehouses. Since the setting to enable the Serverless
	// feature is not currently managed by terraform but we want the auto-termination of these wareshouses to be set to the
	// minimum, we override the auto-termination setting for SQL Warehouse that have had the Serverless feature enabled.
	if _, ok := ServerlessSQLWarehouses[sqlEndpointConfig.Name]; ok {
		sqlEndpoint.AutoStopMins = ServerlessSQLWarehouseAutoStopMinutes
	}

	ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(c.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "no ci service principal app id for region %s", c.Region)
	}

	permissionsResource, err := Permissions(PermissionsConfig{
		ResourceName: sqlEndpointConfig.Name,
		ObjectType:   databricks.ObjectTypeSqlEndpoint,
		ObjectId:     sqlEndpoint.ResourceId().Reference(),
		Owner:        ServicePrincipal{ServicePrincipalAppId: ciServicePrincipalAppId},
		Users:        sqlEndpointConfig.Users,
		Region:       c.Region,
	})
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return []tf.Resource{sqlEndpoint, permissionsResource}, nil
}
