package databricks

import (
	"context"
	"net/http"

	"github.com/samsarahq/go/oops"
)

// https://docs.databricks.com/sql/admin/sql-endpoints.html#cluster-size
type ClusterSize string

const (
	ClusterSizeXSmall = ClusterSize("X-Small")
	ClusterSizeSmall  = ClusterSize("Small")
	ClusterSizeMedium = ClusterSize("Medium")
	ClusterSizeLarge  = ClusterSize("Large")
)

type EndpointSpotInstancePolicy string

const (
	EndpointSpotInstancePolicyCostOptimized        = EndpointSpotInstancePolicy("COST_OPTIMIZED")
	EndpointSpotInstancePolicyReliabilityOptimized = EndpointSpotInstancePolicy("RELIABILITY_OPTIMIZED")
)

// https://docs.databricks.com/sql/api/sql-endpoints.html#endpointstate
type EndpointState string

const (
	EndpointStateStarting = EndpointState("STARTING")
	EndpointStateRunning  = EndpointState("RUNNING")
	EndpointStateStopping = EndpointState("STOPPING")
	EndpointStateStopped  = EndpointState("STOPPED")
	EndpointStateDeleted  = EndpointState("DELETED")
)

type EndpointStatus string

const (
	EndpointStatusHealthy  = EndpointStatus("HEALTHY")
	EndpointStatusDegraded = EndpointStatus("DEGRADED")
	EndpointStatusFailed   = EndpointStatus("FAILED")
)

// https://docs.databricks.com/sql/api/sql-endpoints.html#endpointhealth
type EndpointHealth struct {
	Status  EndpointStatus `json:"status"`
	Message string         `json:"message"`
}

type EndpointTags struct {
	CustomTags []ClusterTag `json:"custom_tags"`
}

type SqlEndpointSpec struct {
	Name               string                     `json:"name"`
	ClusterSize        ClusterSize                `json:"cluster_size"`
	MinNumClusters     int                        `json:"min_num_clusters"`
	MaxNumClusters     int                        `json:"max_num_clusters"`
	AutoStopMins       int                        `json:"auto_stop_mins"`
	Tags               EndpointTags               `json:"tags"`
	SpotInstancePolicy EndpointSpotInstancePolicy `json:"spot_instance_policy"`
	EnablePhoton       bool                       `json:"enable_photon"`
}

type CreateSqlEndpointInput struct {
	SqlEndpointSpec
}

type CreateSqlEndpointOutput struct {
	Id string `json:"id"`
}

type DeleteSqlEndpointInput struct {
	Id string
}

type DeleteSqlEndpointOutput struct {
}

type EditSqlEndpointInput struct {
	Id string
	SqlEndpointSpec
}

type EditSqlEndpointOutput struct {
}

type GetSqlEndpointInput struct {
	Id string
}

type OdbcParams struct {
	Host     string `json:"host"`
	Path     string `json:"path"`
	Protocol string `json:"protocol"`
	Port     int    `json:"port"`
}

type GetSqlEndpointOutput struct {
	Id string `json:"id"`
	SqlEndpointSpec
	NumClusters       int            `json:"num_clusters"`
	NumActiveSessions int            `json:"num_active_sessions"`
	State             EndpointState  `json:"state"`
	CreatorName       string         `json:"creator_name"`
	CreatorId         int            `json:"creator_id"`
	JdbcUrl           string         `json:"jdbc_url"`
	OdbcParams        OdbcParams     `json:"odbc_params"`
	Health            EndpointHealth `json:"health"`
}

type SqlEndpointsAPI interface {
	CreateSqlEndpoint(context.Context, *CreateSqlEndpointInput) (*CreateSqlEndpointOutput, error)
	DeleteSqlEndpoint(context.Context, *DeleteSqlEndpointInput) (*DeleteSqlEndpointOutput, error)
	EditSqlEndpoint(context.Context, *EditSqlEndpointInput) (*EditSqlEndpointOutput, error)
	GetSqlEndpoint(context.Context, *GetSqlEndpointInput) (*GetSqlEndpointOutput, error)
}

func (c *Client) CreateSqlEndpoint(ctx context.Context, input *CreateSqlEndpointInput) (*CreateSqlEndpointOutput, error) {
	var output CreateSqlEndpointOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/sql/endpoints", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) DeleteSqlEndpoint(ctx context.Context, input *DeleteSqlEndpointInput) (*DeleteSqlEndpointOutput, error) {
	var output DeleteSqlEndpointOutput
	if err := c.do(ctx, http.MethodDelete, "/api/2.0/sql/endpoints/"+input.Id, nil, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) EditSqlEndpoint(ctx context.Context, input *EditSqlEndpointInput) (*EditSqlEndpointOutput, error) {
	var output EditSqlEndpointOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/sql/endpoints/"+input.Id+"/edit", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) GetSqlEndpoint(ctx context.Context, input *GetSqlEndpointInput) (*GetSqlEndpointOutput, error) {
	var output GetSqlEndpointOutput
	if err := c.do(ctx, http.MethodGet, "/api/2.0/sql/endpoints/"+input.Id, input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}
