package databricks

import (
	"context"
	"net/http"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/ni/sparkversion"
)

type ClusterState string

const (
	ClusterListV2Limit = 100
)

const (
	ClusterStatePending     = ClusterState("PENDING")
	ClusterStateRunning     = ClusterState("RUNNING")
	ClusterStateRestarting  = ClusterState("RESTARTING")
	ClusterStateResizing    = ClusterState("RESIZING")
	ClusterStateTerminating = ClusterState("TERMINATING")
	ClusterStateTerminated  = ClusterState("TERMINATED")
	ClusterStateError       = ClusterState("ERROR")
	ClusterStateUnknown     = ClusterState("UNKNOWN")
)

type AwsAvailability string

const (
	AwsAvailabilitySpot             = AwsAvailability("SPOT")
	AwsAvailabilityOnDemand         = AwsAvailability("ON_DEMAND")
	AwsAvailabilitySpotWithFallback = AwsAvailability("SPOT_WITH_FALLBACK")
)

// ClusterAutoScale maps to https://docs.databricks.com/api/latest/clusters.html#clusterautoscale.
type ClusterAutoScale struct {
	MinWorkers int `json:"min_workers,omitempty"`
	MaxWorkers int `json:"max_workers,omitempty"`
}

// ClusterAwsAttributes maps to https://docs.databricks.com/api/latest/clusters.html#clusterawsattributes.
type ClusterAwsAttributes struct {
	InstanceProfileArn  string           `json:"instance_profile_arn,omitempty"`
	FirstOnDemand       int              `json:"first_on_demand,omitempty"`
	Availability        *AwsAvailability `json:"availability,omitempty"`
	ZoneId              string           `json:"zone_id,omitempty"`
	SpotBidPricePercent int              `json:"spot_bid_price_percent,omitempty"`
}

type S3StorageInfo struct {
	Destination string `json:"destination"`
	Region      string `json:"region"`
}

type DBFSStorageInfo struct {
	Destination string `json:"destination"`
}

type VolumeStorageInfo struct {
	Destination string `json:"destination"`
}

// https://docs.databricks.com/api/latest/clusters.html#clusterclusterlogconf.
type ClusterLogConf struct {
	DBFS *DBFSStorageInfo `json:"dbfs,omitempty"`
	S3   *S3StorageInfo   `json:"s3,omitempty"`
}

type ClusterTag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type InitScript struct {
	DBFS    *DBFSStorageInfo   `json:"dbfs,omitempty"`
	S3      *S3StorageInfo     `json:"s3,omitempty"`
	Volumes *VolumeStorageInfo `json:"volumes,omitempty"`
}

type DockerImage struct {
	URL string `json:"url,omitempty"`
}

type ClientTypes struct {
	Notebooks bool `json:"notebooks"`
	Jobs      bool `json:"jobs"`
}

type WorkloadTypes struct {
	Clients ClientTypes `json:"clients,omitempty"`
}

// ClusterSpec maps a subset of in https://docs.databricks.com/api/latest/jobs.html#jobsclusterspecnewcluster.
type ClusterSpec struct {
	NumWorkers *int              `json:"num_workers,omitempty"`
	AutoScale  *ClusterAutoScale `json:"autoscale,omitempty"`

	ClusterName               string                    `json:"cluster_name,omitempty"`
	SparkVersion              sparkversion.SparkVersion `json:"spark_version,omitempty"`
	SparkConf                 map[string]string         `json:"spark_conf,omitempty"`
	SparkEnvVars              map[string]string         `json:"spark_env_vars,omitempty"`
	AwsAttributes             *ClusterAwsAttributes     `json:"aws_attributes,omitempty"`
	InitScripts               []*InitScript             `json:"init_scripts,omitempty"`
	NodeTypeId                string                    `json:"node_type_id,omitempty"`
	DriverNodeTypeId          string                    `json:"driver_node_type_id,omitempty"`
	ClusterLogConf            *ClusterLogConf           `json:"cluster_log_conf,omitempty"`
	EnableElasticDisk         bool                      `json:"enable_elastic_disk,omitempty"`
	InstancePoolId            string                    `json:"instance_pool_id,omitempty"`
	DriverInstancePoolId      string                    `json:"driver_instance_pool_id,omitempty"`
	CustomTags                map[string]string         `json:"custom_tags,omitempty"`
	AutoTerminationMinutes    int                       `json:"autotermination_minutes,omitempty"`
	EnableLocalDiskEncryption bool                      `json:"enable_local_disk_encryption,omitempty"`
	Pin                       bool                      `json:"pin,omitempty"`
	ClusterId                 string                    `json:"cluster_id,omitempty"`
	CustomDockerImage         *DockerImage              `json:"docker_image,omitempty"`
	State                     string                    `json:"state,omitempty"`
	WorkloadType              *WorkloadTypes            `json:"workload_type,omitempty"`
	DataSecurityMode          string                    `json:"data_security_mode,omitempty"`
	SingleUserName            string                    `json:"single_user_name,omitempty"`
	IsSingleNode              *bool                     `json:"is_single_node,omitempty"`
	Kind                      *string                   `json:"kind,omitempty"`
	RuntimeEngine             string                    `json:"runtime_engine,omitempty"`
}

type GetClusterInput struct {
	ClusterId string `json:"cluster_id"`
}

type GetClusterOutput struct {
	ClusterSpec
	State ClusterState `json:"state"`
}

type CreateClusterInput struct {
	ClusterSpec
}

type CreateClusterOutput struct {
	ClusterId string `json:"cluster_id"`
}

type EditClusterInput struct {
	ClusterSpec
	ClusterId string `json:"cluster_id"`
}

type PinClusterInput struct {
	ClusterId string `json:"cluster_id"`
}

type UnpinClusterInput struct {
	ClusterId string `json:"cluster_id"`
}

type ListClustersInput struct{}

type FilterBy struct {
	ClusterStates []string `json:"cluster_states,omitempty"`
}

type ListClustersInputV2 struct {
	PageToken string   `json:"page_token,omitempty"`
	PageSize  int      `json:"page_size,omitempty"`
	FilterBy  FilterBy `json:"filter_by,omitempty"`
}

type EditClusterOutput struct{}

type TerminateClusterInput struct {
	ClusterId string `json:"cluster_id"`
}

type TerminateClusterOutput struct{}

type StartClusterInput struct {
	ClusterId string `json:"cluster_id"`
}

type StartClusterOutput struct{}

type PermanentlyDeleteClusterInput struct {
	ClusterId string `json:"cluster_id"`
}

type PermanentlyDeleteClusterOutput struct{}

type PinClusterOutput struct{}

type UnpinClusterOutput struct{}

type ListClustersOutput struct {
	Clusters []ClusterSpec
}

type ListClustersOutputV2 struct {
	Clusters      []ClusterSpec `json:"clusters,omitempty"`
	NextPageToken string        `json:"next_page_token,omitempty"`
	PrevPageToken string        `json:"prev_page_token,omitempty"`
}

type ResizeClusterInput struct {
	NumWorkers int              `json:"num_workers"`
	Autoscale  ClusterAutoScale `json:"autoscale"`
	ClusterId  string           `json:"cluster_id"`
}

type ResizeClusterOutput struct {
	ErrorCode string `json:"error_code,omitempty"`
	Message   string `json:"message,omitempty"`
}

// https://docs.databricks.com/dev-tools/api/latest/clusters.html
type ClustersAPI interface {
	GetCluster(context.Context, *GetClusterInput) (*GetClusterOutput, error)
	CreateCluster(context.Context, *CreateClusterInput) (*CreateClusterOutput, error)
	EditCluster(context.Context, *EditClusterInput) (*EditClusterOutput, error)
	StartCluster(context.Context, *StartClusterInput) (*StartClusterOutput, error)
	TerminateCluster(context.Context, *TerminateClusterInput) (*TerminateClusterOutput, error)
	PermanentlyDeleteCluster(context.Context, *PermanentlyDeleteClusterInput) (*PermanentlyDeleteClusterOutput, error)
	PinCluster(context.Context, *PinClusterInput) (*PinClusterOutput, error)
	UnpinCluster(context.Context, *UnpinClusterInput) (*UnpinClusterOutput, error)
	ListClusters(context.Context, *ListClustersInput) (*ListClustersOutput, error)
	ListClustersV2(context.Context, *ListClustersInputV2) (*ListClustersOutputV2, error)
	ResizeCluster(ctx context.Context, input *ResizeClusterInput) (*ResizeClusterOutput, error)
}

func (c *Client) GetCluster(ctx context.Context, input *GetClusterInput) (*GetClusterOutput, error) {
	var output GetClusterOutput
	if err := c.do(ctx, http.MethodGet, "/api/2.0/clusters/get", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) CreateCluster(ctx context.Context, input *CreateClusterInput) (*CreateClusterOutput, error) {
	var output CreateClusterOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/clusters/create", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) EditCluster(ctx context.Context, input *EditClusterInput) (*EditClusterOutput, error) {
	var output EditClusterOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/clusters/edit", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) TerminateCluster(ctx context.Context, input *TerminateClusterInput) (*TerminateClusterOutput, error) {
	var output TerminateClusterOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/clusters/delete", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) StartCluster(ctx context.Context, input *StartClusterInput) (*StartClusterOutput, error) {
	var output StartClusterOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/clusters/start", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) PermanentlyDeleteCluster(ctx context.Context, input *PermanentlyDeleteClusterInput) (*PermanentlyDeleteClusterOutput, error) {
	var output PermanentlyDeleteClusterOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/clusters/permanent-delete", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) PinCluster(ctx context.Context, input *PinClusterInput) (*PinClusterOutput, error) {
	var output PinClusterOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/clusters/pin", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) UnpinCluster(ctx context.Context, input *UnpinClusterInput) (*UnpinClusterOutput, error) {
	var output UnpinClusterOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/clusters/unpin", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) ListClusters(ctx context.Context, input *ListClustersInput) (*ListClustersOutput, error) {
	var output ListClustersOutput
	if err := c.do(ctx, http.MethodGet, "/api/2.0/clusters/list", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

// Uses the latest 2.1 API and 	pagination is implemented internally to abstract
func (c *Client) ListClustersV2(ctx context.Context, input *ListClustersInputV2) (*ListClustersOutputV2, error) {
	var output ListClustersOutputV2
	var clusters []ClusterSpec

	requestInput := *input

	// if user provides no pagesize or a invalid pagesize, default to ClusterListV2Limit
	if requestInput.PageSize == 0 || requestInput.PageSize > ClusterListV2Limit {
		requestInput.PageSize = int(ClusterListV2Limit)
	}

	hasMore := true
	for hasMore {

		if err := c.do(ctx, http.MethodGet, "/api/2.1/clusters/list", requestInput, &output); err != nil {
			return nil, oops.Wrapf(err, "")
		}
		clusters = append(clusters, output.Clusters...)
		// NextPageToken is an empty string when there are no more clusters left to retrieve.
		hasMore = output.NextPageToken != ""
		requestInput.PageToken = output.NextPageToken
		output = ListClustersOutputV2{}

	}
	output.Clusters = clusters
	return &output, nil
}

func (c *Client) ResizeCluster(ctx context.Context, input *ResizeClusterInput) (*ResizeClusterOutput, error) {
	var output ResizeClusterOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/clusters/resize", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}
