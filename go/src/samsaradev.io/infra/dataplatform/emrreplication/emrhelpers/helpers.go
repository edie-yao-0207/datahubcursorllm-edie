package emrhelpers

import (
	"fmt"
	"sort"

	"samsaradev.io/infra/dataplatform/emrreplication"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/service/registry"
)

var regionToCloud = map[string]infraconsts.SamsaraCloud{
	infraconsts.SamsaraAWSDefaultRegion: infraconsts.SamsaraClouds.USProd,
	infraconsts.SamsaraAWSEURegion:      infraconsts.SamsaraClouds.EUProd,
	infraconsts.SamsaraAWSCARegion:      infraconsts.SamsaraClouds.CAProd,
}

// GetAllCellsPerRegion returns a list of all cells for a given region.
// Returns an error if the region is not supported.
func GetAllCellsPerRegion(region string) ([]string, error) {
	cloud, ok := regionToCloud[region]
	if !ok {
		return nil, fmt.Errorf("unsupported region: %s", region)
	}

	// Use a map as a set for O(1) deduplication
	cellSet := make(map[string]struct{})

	for _, entity := range emrreplication.GetAllEmrReplicationSpecs() {
		serviceInfo := registry.ServicesByName[entity.ServiceName]
		for _, cell := range serviceInfo.DeploymentShape.ClustersForCloud(cloud) {
			cellSet[cell] = struct{}{}
		}
	}

	// Convert set to sorted slice for deterministic order
	result := make([]string, 0, len(cellSet))
	for cell := range cellSet {
		result = append(result, cell)
	}
	sort.Strings(result)
	return result, nil
}

// GetCellsForEntity returns a list of all cells for a given entity in a region.
// Returns an error if the region is not supported.
func GetCellsForEntity(entity emrreplication.EmrReplicationSpec, region string) ([]string, error) {
	cloud, ok := regionToCloud[region]
	if !ok {
		return nil, fmt.Errorf("unsupported region: %s", region)
	}

	serviceInfo, exists := registry.ServicesByName[entity.ServiceName]
	if !exists {
		return nil, fmt.Errorf("service %s not found in registry", entity.ServiceName)
	}

	cells := serviceInfo.DeploymentShape.ClustersForCloud(cloud)
	sort.Strings(cells) // Sort for deterministic order
	return cells, nil
}
