package databricks

import (
	"context"
	"net/http"

	"github.com/samsarahq/go/oops"
)

type EndpointSecurityPolicy string

const (
	EndpointSecurityPolicyDataAccessControl = EndpointSecurityPolicy("DATA_ACCESS_CONTROL")
)

type EndpointConfigPair struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type GlobalSqlEndpointsConfig struct {
	SecurityPolicy     EndpointSecurityPolicy `json:"security_policy"`
	DataAccessConfig   []EndpointConfigPair   `json:"data_access_config"`
	InstanceProfileArn string                 `json:"instance_profile_arn"`
}

type GetGlobalSqlEndpointsConfigInput struct{}

type GetGlobalSqlEndpointsConfigOutput struct {
	GlobalSqlEndpointsConfig
}

type EditGlobalSqlEndpointsConfigInput struct {
	GlobalSqlEndpointsConfig
}

type EditGlobalSqlEndpointsConfigOutput struct {
}

type GlobalSqlEndpointsAPI interface {
	GetGlobalSqlEndpointsConfig(context.Context, *GetGlobalSqlEndpointsConfigInput) (*GetGlobalSqlEndpointsConfigOutput, error)
	EditGlobalSqlEndpointsConfig(context.Context, *EditGlobalSqlEndpointsConfigInput) (*EditGlobalSqlEndpointsConfigOutput, error)
}

func (c *Client) GetGlobalSqlEndpointsConfig(ctx context.Context, input *GetGlobalSqlEndpointsConfigInput) (*GetGlobalSqlEndpointsConfigOutput, error) {
	var output GetGlobalSqlEndpointsConfigOutput
	if err := c.do(ctx, http.MethodGet, "/2.0/sql/config/endpoints", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) EditGlobalSqlEndpointsConfig(ctx context.Context, input *EditGlobalSqlEndpointsConfigInput) (*EditGlobalSqlEndpointsConfigOutput, error) {
	var output EditGlobalSqlEndpointsConfigOutput
	if err := c.do(ctx, http.MethodPut, "/2.0/sql/config/endpoints", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}
