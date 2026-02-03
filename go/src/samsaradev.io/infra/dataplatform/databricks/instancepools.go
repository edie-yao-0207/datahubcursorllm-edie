package databricks

import (
	"context"
	"net/http"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/ni/sparkversion"
)

type EbsVolumeType string

const (
	EbsVolumeTypeGeneralPurposeSSD             = EbsVolumeType("GENERAL_PURPOSE_SSD")
	EbsVolumeTypeGeneralThroughputOptimizedHDD = EbsVolumeType("THROUGHPUT_OPTIMIZED_HDD")
)

type InstancePoolState string

const (
	InstancePoolStateActive  = InstancePoolState("ACTIVE")
	InstancePoolStateDeleted = InstancePoolState("DELETED")
)

// https://docs.databricks.com/dev-tools/api/latest/instance-pools.html#clusterinstancepoolawsattributes
type InstancePoolAwsAttributes struct {
	Availability        AwsAvailability `json:"availability,omitempty"`
	ZoneId              string          `json:"zone_id,omitempty"`
	SpotBidPricePercent int             `json:"spot_bid_price_percent,omitempty"`
}

type DiskType struct {
	EbsVolumeType EbsVolumeType `json:"ebs_volume_type,omitempty"`
}

// https://docs.databricks.com/dev-tools/api/latest/instance-pools.html#diskspec
type DiskSpec struct {
	DiskType  DiskType `json:"disk_type"`
	DiskCount int      `json:"disk_count"`
	DiskSize  int      `json:"disk_size"`
}

type SubmitJobPools struct {
	InstancePoolName string

	// Providing a driver pool name is optional. If you don't, the job runs
	// entirely on the instance pool provided.
	DriverPoolName *string
}

type InstancePoolSpec struct {
	InstancePoolName                   string                      `json:"instance_pool_name"`
	MinIdleInstances                   int                         `json:"min_idle_instances"`
	MaxCapacity                        *int                        `json:"max_capacity"`
	AwsAttributes                      *InstancePoolAwsAttributes  `json:"aws_attributes,omitempty"`
	NodeTypeId                         string                      `json:"node_type_id"`
	CustomTags                         map[string]string           `json:"custom_tags,omitempty"`
	IdleInstanceAutoterminationMinutes int                         `json:"idle_instance_autotermination_minutes"`
	EnableElasticDisk                  bool                        `json:"enable_elastic_disk"`
	DiskSpec                           *DiskSpec                   `json:"disk_spec,omitempty"`
	PreloadedSparkVersions             []sparkversion.SparkVersion `json:"preloaded_spark_versions"`
}

type CreateInstancePoolInput struct {
	InstancePoolSpec
}

type CreateInstancePoolOutput struct {
	InstancePoolId string `json:"instance_pool_id"`
}

// https://docs.databricks.com/dev-tools/api/latest/instance-pools.html#edit
type EditInstancePoolInput struct {
	InstancePoolId                     string `json:"instance_pool_id"`
	InstancePoolName                   string `json:"instance_pool_name"`
	MinIdleInstances                   int    `json:"min_idle_instances"`
	MaxCapacity                        *int   `json:"max_capacity" hcl:"omitempty"`
	IdleInstanceAutoterminationMinutes int    `json:"idle_instance_autotermination_minutes,omitempty"`

	// "You must supply a node_type_id and it must match the original node_type_id."
	NodeTypeId string `json:"node_type_id"`
}

type EditInstancePoolOutput struct{}

type DeleteInstancePoolInput struct {
	InstancePoolId string `json:"instance_pool_id"`
}
type DeleteInstancePoolOutput struct{}

type GetInstancePoolInput struct {
	InstancePoolId string `json:"instance_pool_id"`
}

type InstancePoolStats struct {
	UsedCount        int `json:"used_count"`
	IdleCount        int `json:"idle_count"`
	PendingUsedCount int `json:"pending_used_count"`
	PendingIdleCount int `json:"pending_idle_count"`
}

type GetInstancePoolOutput struct {
	InstancePoolSpec
	DefaultTags map[string]string `json:"default_tags"`
	State       InstancePoolState `json:"state"`
	Stats       InstancePoolStats `json:"stats"`
}

type InstancePoolAndStats struct {
	InstancePoolSpec
	InstancePoolId string            `json:"instance_pool_id"`
	DefaultTags    map[string]string `json:"default_tags"`
	State          InstancePoolState `json:"state"`
	Stats          InstancePoolStats `json:"stats"`
}

type ListInstancePoolsOutput struct {
	InstancePools []InstancePoolAndStats `json:"instance_pools"`
}

type InstancePoolsAPI interface {
	CreateInstancePool(context.Context, *CreateInstancePoolInput) (*CreateInstancePoolOutput, error)
	EditInstancePool(context.Context, *EditInstancePoolInput) (*EditInstancePoolOutput, error)
	DeleteInstancePool(context.Context, *DeleteInstancePoolInput) (*DeleteInstancePoolOutput, error)
	GetInstancePool(context.Context, *GetInstancePoolInput) (*GetInstancePoolOutput, error)
	ListInstancePools(context.Context) (*ListInstancePoolsOutput, error)
}

func (c *Client) CreateInstancePool(ctx context.Context, input *CreateInstancePoolInput) (*CreateInstancePoolOutput, error) {
	var output CreateInstancePoolOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/instance-pools/create", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) EditInstancePool(ctx context.Context, input *EditInstancePoolInput) (*EditInstancePoolOutput, error) {
	var output EditInstancePoolOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/instance-pools/edit", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) DeleteInstancePool(ctx context.Context, input *DeleteInstancePoolInput) (*DeleteInstancePoolOutput, error) {
	var output DeleteInstancePoolOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/instance-pools/delete", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) GetInstancePool(ctx context.Context, input *GetInstancePoolInput) (*GetInstancePoolOutput, error) {
	var output GetInstancePoolOutput
	if err := c.do(ctx, http.MethodGet, "/api/2.0/instance-pools/get", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

// https://docs.databricks.com/dev-tools/api/latest/instance-pools.html#list
func (c *Client) ListInstancePools(ctx context.Context) (*ListInstancePoolsOutput, error) {
	var output ListInstancePoolsOutput
	if err := c.do(ctx, http.MethodGet, "/api/2.0/instance-pools/list", struct{}{}, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}
