package activity

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/testloader"
	"samsaradev.io/models"
)

func TestGetAllOrganizationsActivity_Execute(t *testing.T) {
	var env struct {
		PrimaryAppModelsInput models.PrimaryAppModelsInput
	}
	testloader.MustStart(t, &env)

	appModels := env.PrimaryAppModelsInput.Models
	ctx := context.Background()

	// Create test organizations
	org1 := appModels.Organization.CreateWithTxForTest("test org 1")
	require.NotNil(t, org1)
	err := appModels.OrgCell.UpdateRow(ctx, &models.OrgCell{
		OrgId:  org1.Id,
		CellId: "us2",
	})
	require.NoError(t, err)

	org2 := appModels.Organization.CreateWithTxForTest("test org 2")
	require.NotNil(t, org2)
	err = appModels.OrgCell.UpdateRow(ctx, &models.OrgCell{
		OrgId:  org2.Id,
		CellId: "us2",
	})
	require.NoError(t, err)

	org3 := appModels.Organization.CreateWithTxForTest("test org 3")
	require.NotNil(t, org3)
	err = appModels.OrgCell.UpdateRow(ctx, &models.OrgCell{
		OrgId:  org3.Id,
		CellId: "us2",
	})
	require.NoError(t, err)

	// This org is in a different cell, so it should not be returned.
	org4 := appModels.Organization.CreateWithTxForTest("test org 4")
	require.NotNil(t, org4)
	err = appModels.OrgCell.UpdateRow(ctx, &models.OrgCell{
		OrgId:  org4.Id,
		CellId: "sf1",
	})
	require.NoError(t, err)

	activity := &GetAllOrganizationsActivity{
		ReplicaAppModels: appModels,
	}

	t.Setenv("IS_RUNNING_IN_ECS", "true")
	t.Setenv("ECS_CLUSTERNAME_OVERRIDE", "us2")

	t.Run("returns all orgs in cell when OrgIds is nil", func(t *testing.T) {
		result, err := activity.Execute(ctx, &GetAllOrganizationsActivityArgs{})

		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Contains(t, result.OrgIds, org1.Id)
		assert.Contains(t, result.OrgIds, org2.Id)
		assert.Contains(t, result.OrgIds, org3.Id)
	})

	t.Run("filters orgs when OrgIds is specified", func(t *testing.T) {
		orgIds := []int64{org1.Id, org3.Id}
		result, err := activity.Execute(ctx, &GetAllOrganizationsActivityArgs{
			OrgIds: &orgIds,
		})

		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Contains(t, result.OrgIds, org1.Id)
		assert.NotContains(t, result.OrgIds, org2.Id)
		assert.Contains(t, result.OrgIds, org3.Id)
	})

	t.Run("excludes orgs when ExcludeOrgIds is specified", func(t *testing.T) {
		excludeOrgIds := []int64{org2.Id}
		result, err := activity.Execute(ctx, &GetAllOrganizationsActivityArgs{
			ExcludeOrgIds: &excludeOrgIds,
		})

		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Contains(t, result.OrgIds, org1.Id)
		assert.NotContains(t, result.OrgIds, org2.Id)
		assert.Contains(t, result.OrgIds, org3.Id)
	})

	t.Run("excludes multiple orgs when ExcludeOrgIds is specified", func(t *testing.T) {
		excludeOrgIds := []int64{org1.Id, org3.Id}
		result, err := activity.Execute(ctx, &GetAllOrganizationsActivityArgs{
			ExcludeOrgIds: &excludeOrgIds,
		})

		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.NotContains(t, result.OrgIds, org1.Id)
		assert.Contains(t, result.OrgIds, org2.Id)
		assert.NotContains(t, result.OrgIds, org3.Id)
	})
}
