package activity

import (
	"context"

	"github.com/samsarahq/go/oops"
	"go.uber.org/fx"

	"samsaradev.io/infra/dataplatform/emrreplication"
	"samsaradev.io/infra/workflows/workflowregistry"
	"samsaradev.io/models"
)

func init() {
	workflowregistry.MustRegisterActivityConstructor(NewGetAllAssetIdsActivity)
}

type GetAllAssetIdsActivityArgs struct {
	OrgId     int64
	AssetType emrreplication.AssetType
}

type GetAllAssetIdsActivity struct {
	ReplicaAppModels *models.AppModels
}

type GetAllAssetIdsActivityParams struct {
	fx.In
	ReplicaAppModels models.ReplicaAppModelsInput
}

type GetAllAssetIdsActivityResult struct {
	AssetIds []int64
}

func NewGetAllAssetIdsActivity(p GetAllAssetIdsActivityParams) *GetAllAssetIdsActivity {
	return &GetAllAssetIdsActivity{
		ReplicaAppModels: p.ReplicaAppModels.Models,
	}
}

func (a GetAllAssetIdsActivity) Name() string {
	return "GetAllAssetIdsActivity"
}

func (a GetAllAssetIdsActivity) Execute(ctx context.Context, args *GetAllAssetIdsActivityArgs) (GetAllAssetIdsActivityResult, error) {
	assetIds := make([]int64, 0)

	switch args.AssetType {
	case emrreplication.DriverAssetType:
		drivers, err := a.ReplicaAppModels.Driver.ByOrgId(ctx, args.OrgId)
		if err != nil {
			return GetAllAssetIdsActivityResult{}, oops.Wrapf(err, "failed to get all assets for org %d", args.OrgId)
		}
		for _, driver := range drivers {
			assetIds = append(assetIds, driver.Id)
		}
	case emrreplication.DeviceAssetType:
		devices, err := a.ReplicaAppModels.Device.ByOrgId(ctx, args.OrgId)
		if err != nil {
			return GetAllAssetIdsActivityResult{}, oops.Wrapf(err, "failed to get all assets for org %d", args.OrgId)
		}
		for _, device := range devices {
			assetIds = append(assetIds, device.Id)
		}

	case emrreplication.TagAssetType:
		tags, err := a.ReplicaAppModels.Tag.ByOrgId(ctx, args.OrgId)
		if err != nil {
			return GetAllAssetIdsActivityResult{}, oops.Wrapf(err, "failed to get all assets for org %d", args.OrgId)
		}
		for _, tag := range tags {
			assetIds = append(assetIds, tag.Id)
		}
	default:
		return GetAllAssetIdsActivityResult{}, oops.Errorf("invalid asset type: %s", args.AssetType)
	}

	return GetAllAssetIdsActivityResult{
		AssetIds: assetIds,
	}, nil
}
