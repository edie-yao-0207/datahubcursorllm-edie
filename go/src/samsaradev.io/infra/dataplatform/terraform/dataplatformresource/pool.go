package dataplatformresource

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

// allowedInstancePoolNodeTypes is a list of nodes that the Data Platform team currently
// support for instance pools. This list can be added to.
// Now adds support for fleet instance types (eg: r-fleet, rd-fleet, *-fleet etc).
// Using fleet instance types means instance type acquired will be part
// of a fleet family.The specific instances types are determined by the price and capacity
// conditions at the time of cluster creation.  Eg: rd-fleet.4xlarge can acquire
// node types such as r6id.4xlarge, r6id.4xlarge, r5d.4xlarge etc.
var allowedInstancePoolNodeTypes = map[string]struct{}{
	"r5.large":          {},
	"r5.xlarge":         {},
	"r5.2xlarge":        {},
	"r5.4xlarge":        {},
	"r5.12xlarge":       {},
	"r5.24xlarge":       {},
	"r5a.large":         {},
	"r5a.xlarge":        {},
	"r5a.2xlarge":       {},
	"r5a.4xlarge":       {},
	"r5a.12xlarge":      {},
	"r5a.24xlarge":      {},
	"r5d.large":         {},
	"r5d.xlarge":        {},
	"r5d.2xlarge":       {},
	"r5d.4xlarge":       {},
	"r5d.12xlarge":      {},
	"r5d.24xlarge":      {},
	"r5dn.large":        {},
	"r5dn.xlarge":       {},
	"r5dn.2xlarge":      {},
	"r5dn.4xlarge":      {},
	"r5dn.12xlarge":     {},
	"r5dn.24xlarge":     {},
	"r5n.large":         {},
	"r5n.xlarge":        {},
	"r5n.2xlarge":       {},
	"r5n.4xlarge":       {},
	"r5n.12xlarge":      {},
	"r5n.24xlarge":      {},
	"r6i.xlarge":        {},
	"r6i.2xlarge":       {},
	"r6i.4xlarge":       {},
	"r6i.8xlarge":       {},
	"r6i.16xlarge":      {},
	"r-fleet.xlarge":    {},
	"r-fleet.2xlarge":   {},
	"r-fleet.4xlarge":   {},
	"r-fleet.8xlarge":   {},
	"rd-fleet.xlarge":   {},
	"rd-fleet.2xlarge":  {},
	"rd-fleet.4xlarge":  {}, // Example instance types in this fleet are r6id.4xlarge, r6id.4xlarge, r5d.4xlarge etc.
	"rd-fleet.8xlarge":  {},
	"rgd-fleet.xlarge":  {},
	"rgd-fleet.2xlarge": {},
	"rgd-fleet.4xlarge": {},
	"rgd-fleet.8xlarge": {},
	"i3.xlarge":         {},
	"i3.2xlarge":        {},
	"i3.4xlarge":        {},
	"i3.8xlarge":        {},
	"i3.16xlarge":       {},
	"i4i.xlarge":        {},
	"i4i.2xlarge":       {},
	"i4i.4xlarge":       {},
	"i4i.8xlarge":       {},
	"i4i.16xlarge":      {},
	"m5.xlarge":         {},
	"m5d.large":         {},
	"m5d.xlarge":        {},
	"m5d.2xlarge":       {},
	"m5d.4xlarge":       {},
	"m5d.8xlarge":       {},
	"m5d.12xlarge":      {},
	"m5d.24xlarge":      {},
	"m5dn.large":        {},
	"m5dn.xlarge":       {},
	"m5dn.2xlarge":      {},
	"m5dn.4xlarge":      {},
	"m5dn.8xlarge":      {},
	"m5dn.12xlarge":     {},
	"m5dn.24xlarge":     {},
	"m-fleet.xlarge":    {},
	"m-fleet.2xlarge":   {},
	"m-fleet.4xlarge":   {},
	"m-fleet.8xlarge":   {},
	"md-fleet.xlarge":   {},
	"md-fleet.2xlarge":  {},
	"md-fleet.4xlarge":  {},
	"md-fleet.8xlarge":  {},
}

type InstancePoolConfig struct {
	Name                               string
	MinIdleInstances                   int
	MaxCapacity                        int
	NodeTypeId                         string
	IdleInstanceAutoterminationMinutes int

	// Version of spark that gets preloaded on the instance pool.
	PreloadedSparkVersion sparkversion.SparkVersion
	OnDemand              bool
	ZoneId                string
	Owner                 components.TeamInfo

	// RNDAllocation specifies percent (0 to 1) of the instance pool's cost to be
	// allocated to R&D. This applies to all jobs scheduled on the pool.
	RnDCostAllocation float64
}

func ValidateNodeTypeId(nodeTypeId string) (bool, error) {
	if _, ok := allowedInstancePoolNodeTypes[nodeTypeId]; ok {
		return true, nil
	}
	return false, oops.Errorf("node type id %s is not in allowedInstancePoolNodeType. Check (https://databricks.com/product/aws-pricing/instance-types) before adding.", nodeTypeId)
}

// Temporary region argument allows us to generate permissions for only a specific region.
// TODO: Delete this argument once the account group migration is complete.
func InstancePool(c InstancePoolConfig, region string) ([]tf.Resource, error) {
	if c.RnDCostAllocation < 0 || c.RnDCostAllocation > 1 {
		return nil, oops.Errorf("invalid RnDCostAllocation: %f", c.RnDCostAllocation)
	}

	availability := databricks.AwsAvailabilitySpot
	if c.OnDemand {
		availability = databricks.AwsAvailabilityOnDemand
	}

	if c.ZoneId == "" {
		return nil, oops.Errorf("Please explicitly provide a zone id for this pool to run in.")
	}

	pool := &databricksresource.InstancePool{
		InstancePoolName:                   c.Name,
		MinIdleInstances:                   c.MinIdleInstances,
		MaxCapacity:                        c.MaxCapacity,
		NodeTypeId:                         c.NodeTypeId,
		IdleInstanceAutoterminationMinutes: c.IdleInstanceAutoterminationMinutes,
		EnableElasticDisk:                  true,
		CustomTags: []databricksresource.ClusterTag{
			{
				Key:   "samsara:team",
				Value: strings.ToLower(c.Owner.Name()),
			},
			{
				Key:   "samsara:service",
				Value: fmt.Sprintf("databrickspool-%s", c.Name),
			},
			{
				Key:   "samsara:product-group",
				Value: strings.ToLower(team.TeamProductGroup[c.Owner.Name()]),
			},
			{
				Key:   "samsara:rnd-allocation",
				Value: strconv.FormatFloat(c.RnDCostAllocation, 'f', -1, 64),
			},
		},
		AwsAttributes: &databricksresource.InstancePoolAwsAttributes{
			Availability: availability,
			ZoneId:       c.ZoneId,
		},
	}

	if c.PreloadedSparkVersion != "" {
		pool.PreloadedSparkVersions = []sparkversion.SparkVersion{c.PreloadedSparkVersion}
	}

	if _, allowedInstance := allowedInstancePoolNodeTypes[c.NodeTypeId]; !allowedInstance {
		return nil, oops.Errorf("Invalid instance type %s for instance pool. If this is a Databricks supported instance please add to allowedInstance map in 'dataplatformresource/pool.go'. Use '$ databricks clusters list-node-types' to view allow spark node types.", c.NodeTypeId)
	}

	perm, err := Permissions(PermissionsConfig{
		ResourceName: c.Name,
		ObjectType:   databricks.ObjectTypeInstancePool,
		ObjectId:     pool.ResourceId().Reference(),
		Owner:        TeamPrincipal{Team: c.Owner},
		Region:       region,
	})
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return []tf.Resource{pool, perm}, nil
}
