package activity

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/dataplatform/emrreplication"
	"samsaradev.io/infra/testloader"
	"samsaradev.io/models"
	"samsaradev.io/products"
)

func TestGetAllAssetIdsActivity_Execute(t *testing.T) {
	var env struct {
		PrimaryAppModels models.PrimaryAppModelsInput
	}
	testloader.MustStart(t, &env)

	appModels := env.PrimaryAppModels.Models
	ctx := context.Background()

	// Create test organization
	org := appModels.Organization.CreateWithTxForTest("test org")
	require.NotNil(t, org)

	// Create test devices/assets
	device1 := &models.Device{
		Id:        1001,
		OrgId:     org.Id,
		ProductId: products.ProductVg34,
		Serial:    "test-device-1",
	}
	err := appModels.CreateDeviceForTest(ctx, device1)
	require.NoError(t, err)

	device2 := &models.Device{
		Id:        1002,
		OrgId:     org.Id,
		ProductId: products.ProductVg34,
		Serial:    "test-device-2",
	}
	err = appModels.CreateDeviceForTest(ctx, device2)
	require.NoError(t, err)

	device3 := &models.Device{
		Id:        1003,
		OrgId:     org.Id,
		ProductId: products.ProductVg34,
		Serial:    "test-device-3",
	}
	err = appModels.CreateDeviceForTest(ctx, device3)
	require.NoError(t, err)

	driver1 := appModels.AddDriverForTest(org.Id, 1, "test-driver-1")
	driver2 := appModels.AddDriverForTest(org.Id, 1, "test-driver-2")
	driver3 := appModels.AddDriverForTest(org.Id, 1, "test-driver-3")

	activity := &GetAllAssetIdsActivity{
		ReplicaAppModels: appModels,
	}

	t.Run("returns all devices IDs for org", func(t *testing.T) {
		result, err := activity.Execute(ctx, &GetAllAssetIdsActivityArgs{
			OrgId:     org.Id,
			AssetType: emrreplication.DeviceAssetType,
		})

		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Contains(t, result.AssetIds, device1.Id)
		assert.Contains(t, result.AssetIds, device2.Id)
		assert.Contains(t, result.AssetIds, device3.Id)
		assert.Equal(t, 3, len(result.AssetIds))
	})

	t.Run("returns empty list for org with no devices", func(t *testing.T) {
		emptyOrg := appModels.Organization.CreateWithTxForTest("empty org")
		require.NotNil(t, emptyOrg)

		result, err := activity.Execute(ctx, &GetAllAssetIdsActivityArgs{
			OrgId:     emptyOrg.Id,
			AssetType: emrreplication.DeviceAssetType,
		})

		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Empty(t, result.AssetIds)
	})

	t.Run("returns all driver IDs for org", func(t *testing.T) {
		result, err := activity.Execute(ctx, &GetAllAssetIdsActivityArgs{
			OrgId:     org.Id,
			AssetType: emrreplication.DriverAssetType,
		})

		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Contains(t, result.AssetIds, driver1.Id)
		assert.Contains(t, result.AssetIds, driver2.Id)
		assert.Contains(t, result.AssetIds, driver3.Id)
		assert.Equal(t, 3, len(result.AssetIds))
	})

	t.Run("returns empty list for org with no drivers", func(t *testing.T) {
		emptyOrg := appModels.Organization.CreateWithTxForTest("empty org")
		require.NotNil(t, emptyOrg)

		result, err := activity.Execute(ctx, &GetAllAssetIdsActivityArgs{
			OrgId:     emptyOrg.Id,
			AssetType: emrreplication.DriverAssetType,
		})

		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Empty(t, result.AssetIds)
	})
}
