package emrreplication_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/dataplatform/emrreplication"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/service/registry"
)

func TestGetAllEmrReplicationSpecs(t *testing.T) {
	entities := emrreplication.GetAllEmrReplicationSpecs()
	require.NotEmpty(t, entities, "registry should not be empty")

	for _, entity := range entities {
		// Check that Name is defined
		assert.NotEmpty(t, entity.Name, "entity name should not be empty")
		assert.NotEmpty(t, entity.OwnerTeam, "entity owner team should not be empty")
		// Verify AssetType is one of the valid values
		assert.True(t, entity.AssetType == emrreplication.DriverAssetType || entity.AssetType == emrreplication.DeviceAssetType || entity.AssetType == emrreplication.TagAssetType,
			"AssetType should be one of DriverAssetType, DeviceAssetType, or TagAssetType for entity %s", entity.Name)

		// Check that YamlPath exists in repo
		// Convert YamlPath to absolute path relative to backend root
		fullPath := filepath.Join(filepathhelpers.BackendRoot, entity.YamlPath)
		_, err := os.Stat(fullPath)
		assert.NoErrorf(t, err, "yaml file should exist at path %s for entity %s", fullPath, entity.Name)

		// Additional validation for SqsDelaySeconds
		assert.GreaterOrEqual(t, entity.SqsDelaySeconds, 0,
			"SqsDelaySeconds should be non-negative for entity %s", entity.Name)

		// Check that ServiceName is defined
		assert.NotEmpty(t, entity.ServiceName, "service name should not be empty for entity %s", entity.Name)

		// Check that ServiceName is found in the registry
		serviceInfo, found := registry.ServicesByName[entity.ServiceName]
		assert.True(t, found, "service should be found in the registry for entity %s", entity.Name)

		// Check cells for all clouds
		for _, cloud := range infraconsts.SamsaraCloudsAsSlice {
			cells := serviceInfo.DeploymentShape.ClustersForCloud(cloud)
			if !serviceInfo.ShouldDeployToCloud(cloud) {
				continue
			}
			require.NotEmpty(t, cells, "cells should not be empty for service %s in cloud %v", entity.ServiceName, cloud)
		}
	}
}

func TestGetEmrReplicationQueue(t *testing.T) {
	entityName := "entity1"
	queue := emrreplication.GetEmrReplicationQueue(entityName)
	expectedQueueName := fmt.Sprintf("samsara_emr_entity_replication_%s_queue", strings.ToLower(entityName))
	assert.Equal(t, expectedQueueName, string(queue.SqsQueueName()))
}

func TestGetCdcConfigMapForCloud(t *testing.T) {
	t.Run("returns DynamoDB entities for US cloud", func(t *testing.T) {
		result := emrreplication.GetCdcConfigMapForCloud(emrreplication.DataSourceTypeDynamoDB, infraconsts.SamsaraClouds.USProd)

		// Driver should be in DynamoDB for US (default)
		driverConfig, ok := result["drivers"]
		assert.True(t, ok, "drivers table should be in DynamoDB config for US cloud")
		assert.Equal(t, "Driver", driverConfig.EmrEntityName)
		assert.Equal(t, "id", driverConfig.SourcePrimaryKeyName)

		// Asset should NOT be in DynamoDB (it's RDS only)
		_, ok = result["devices"]
		assert.False(t, ok, "devices table should NOT be in DynamoDB config")

		// Tag should NOT be in DynamoDB (it's RDS only)
		_, ok = result["tags"]
		assert.False(t, ok, "tags table should NOT be in DynamoDB config")
	})

	t.Run("returns RDS entities for US cloud", func(t *testing.T) {
		result := emrreplication.GetCdcConfigMapForCloud(emrreplication.DataSourceTypeRDS, infraconsts.SamsaraClouds.USProd)

		// Driver should NOT be in RDS for US (it's DynamoDB by default)
		_, ok := result["drivers"]
		assert.False(t, ok, "drivers table should NOT be in RDS config for US cloud")

		// Asset should be in RDS
		assetConfig, ok := result["devices"]
		assert.True(t, ok, "devices table should be in RDS config for US cloud")
		assert.Equal(t, "Asset", assetConfig.EmrEntityName)
		assert.Equal(t, "id", assetConfig.SourcePrimaryKeyName)

		// Tag should be in RDS
		tagConfig, ok := result["tags"]
		assert.True(t, ok, "tags table should be in RDS config for US cloud")
		assert.Equal(t, "Tag", tagConfig.EmrEntityName)
	})

	t.Run("returns RDS entities for EU cloud including Driver", func(t *testing.T) {
		result := emrreplication.GetCdcConfigMapForCloud(emrreplication.DataSourceTypeRDS, infraconsts.SamsaraClouds.EUProd)

		// Driver should be in RDS for EU (override from DynamoDB default)
		driverConfig, ok := result["drivers"]
		assert.True(t, ok, "drivers table should be in RDS config for EU cloud")
		assert.Equal(t, "Driver", driverConfig.EmrEntityName)

		// Asset should be in RDS
		_, ok = result["devices"]
		assert.True(t, ok, "devices table should be in RDS config for EU cloud")

		// Tag should be in RDS
		_, ok = result["tags"]
		assert.True(t, ok, "tags table should be in RDS config for EU cloud")
	})

	t.Run("returns no DynamoDB entities for EU cloud", func(t *testing.T) {
		result := emrreplication.GetCdcConfigMapForCloud(emrreplication.DataSourceTypeDynamoDB, infraconsts.SamsaraClouds.EUProd)

		// Driver should NOT be in DynamoDB for EU (overridden to RDS)
		_, ok := result["drivers"]
		assert.False(t, ok, "drivers table should NOT be in DynamoDB config for EU cloud")
	})

	t.Run("returns RDS entities for CA cloud including Driver", func(t *testing.T) {
		result := emrreplication.GetCdcConfigMapForCloud(emrreplication.DataSourceTypeRDS, infraconsts.SamsaraClouds.CAProd)

		// Driver should be in RDS for CA (override from DynamoDB default)
		driverConfig, ok := result["drivers"]
		assert.True(t, ok, "drivers table should be in RDS config for CA cloud")
		assert.Equal(t, "Driver", driverConfig.EmrEntityName)

		// Asset should be in RDS
		_, ok = result["devices"]
		assert.True(t, ok, "devices table should be in RDS config for CA cloud")

		// Tag should be in RDS
		_, ok = result["tags"]
		assert.True(t, ok, "tags table should be in RDS config for CA cloud")
	})

	t.Run("returns entities using default DataSource for unknown cloud", func(t *testing.T) {
		// For an unknown cloud (like NoSuchCloud), entities should use their default DataSource
		result := emrreplication.GetCdcConfigMapForCloud(emrreplication.DataSourceTypeDynamoDB, infraconsts.SamsaraClouds.NoSuchCloud)

		// Driver has default DataSource=DynamoDB, so it should be included
		_, ok := result["drivers"]
		assert.True(t, ok, "drivers table should be in DynamoDB config for unknown cloud (uses default)")

		// Asset has default DataSource=RDS, so it should NOT be in DynamoDB
		_, ok = result["devices"]
		assert.False(t, ok, "devices table should NOT be in DynamoDB config for unknown cloud")
	})
}

func TestCdcConfig_GetDataSourceForCloud(t *testing.T) {
	t.Run("returns override when cloud is in DataSourceByCloud", func(t *testing.T) {
		config := &emrreplication.CdcConfig{
			DataSource: emrreplication.DataSourceTypeDynamoDB, // default
			DataSourceByCloud: map[infraconsts.SamsaraCloud]emrreplication.DataSourceType{
				infraconsts.SamsaraClouds.EUProd: emrreplication.DataSourceTypeRDS, // override for EU
			},
		}

		// EU should use the override
		ds := config.GetDataSourceForCloud(infraconsts.SamsaraClouds.EUProd)
		assert.Equal(t, emrreplication.DataSourceTypeRDS, ds)
	})

	t.Run("returns default when cloud is not in DataSourceByCloud", func(t *testing.T) {
		config := &emrreplication.CdcConfig{
			DataSource: emrreplication.DataSourceTypeDynamoDB, // default
			DataSourceByCloud: map[infraconsts.SamsaraCloud]emrreplication.DataSourceType{
				infraconsts.SamsaraClouds.EUProd: emrreplication.DataSourceTypeRDS, // override for EU only
			},
		}

		// US should use the default
		ds := config.GetDataSourceForCloud(infraconsts.SamsaraClouds.USProd)
		assert.Equal(t, emrreplication.DataSourceTypeDynamoDB, ds)

		// CA should also use the default (no override)
		ds = config.GetDataSourceForCloud(infraconsts.SamsaraClouds.CAProd)
		assert.Equal(t, emrreplication.DataSourceTypeDynamoDB, ds)
	})

	t.Run("returns default when DataSourceByCloud is nil", func(t *testing.T) {
		config := &emrreplication.CdcConfig{
			DataSource:        emrreplication.DataSourceTypeRDS,
			DataSourceByCloud: nil,
		}

		ds := config.GetDataSourceForCloud(infraconsts.SamsaraClouds.USProd)
		assert.Equal(t, emrreplication.DataSourceTypeRDS, ds)
	})
}
