package activity

import (
	"context"
	"math/rand"

	"github.com/samsarahq/go/oops"
	"go.uber.org/fx"

	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/dataplatform/emrreplication"
	"samsaradev.io/infra/samsaraaws"
	"samsaradev.io/infra/workflows/workflowregistry"
	"samsaradev.io/models"
)

func init() {
	workflowregistry.MustRegisterActivityConstructor(NewSelectOrgsAndAssetsActivity)
}

type SelectOrgsAndAssetsActivityArgs struct {
	Cell               string
	NumOrgsValidated   int
	NumAssetsValidated int
	PinnedOrgs         []int64
	AssetType          emrreplication.AssetType
}

type OrgAssetSelection struct {
	OrgId    int64   `json:"orgId"`
	AssetIds []int64 `json:"assetIds"`
}

type SelectOrgsAndAssetsActivityResult struct {
	SelectedOrgs []OrgAssetSelection `json:"selectedOrgs"`
}

type SelectOrgsAndAssetsActivity struct {
	ReplicaAppModels models.IAppModels
}

type SelectOrgsAndAssetsActivityParams struct {
	fx.In
	ReplicaAppModels models.ReplicaAppModelsInput
}

func NewSelectOrgsAndAssetsActivity(p SelectOrgsAndAssetsActivityParams) *SelectOrgsAndAssetsActivity {
	return &SelectOrgsAndAssetsActivity{
		ReplicaAppModels: p.ReplicaAppModels.Models,
	}
}

func (a SelectOrgsAndAssetsActivity) Name() string {
	return "SelectOrgsAndAssetsActivity"
}

// selectOrgsForValidation selects a combination of pinned orgs and randomly sampled orgs.
// It ensures all pinned orgs are included and fills the remaining slots with randomly selected orgs.
func selectOrgsForValidation(allOrgIds []int64, pinnedOrgs []int64, numOrgsValidated int) []int64 {
	// Create a set of pinned orgs for quick lookup
	pinnedOrgSet := make(map[int64]bool)
	for _, pinnedOrg := range pinnedOrgs {
		pinnedOrgSet[pinnedOrg] = true
	}

	// Separate pinned orgs from other orgs
	var selectedPinnedOrgs []int64
	var availableOrgs []int64
	for _, orgId := range allOrgIds {
		if pinnedOrgSet[orgId] {
			selectedPinnedOrgs = append(selectedPinnedOrgs, orgId)
		} else {
			availableOrgs = append(availableOrgs, orgId)
		}
	}

	// Shuffle available orgs for random selection
	// Note: In Go 1.20+, the global random source is automatically seeded, so no need for rand.Seed()
	rand.Shuffle(len(availableOrgs), func(i, j int) {
		availableOrgs[i], availableOrgs[j] = availableOrgs[j], availableOrgs[i]
	})

	// Select orgs: pinned orgs + random sample of remaining orgs
	selectedOrgs := selectedPinnedOrgs
	remainingSampleSize := numOrgsValidated - len(selectedPinnedOrgs)
	if remainingSampleSize > 0 && remainingSampleSize < len(availableOrgs) {
		selectedOrgs = append(selectedOrgs, availableOrgs[:remainingSampleSize]...)
	} else if remainingSampleSize > 0 {
		selectedOrgs = append(selectedOrgs, availableOrgs...)
	}

	return selectedOrgs
}

func (a SelectOrgsAndAssetsActivity) Execute(ctx context.Context, args *SelectOrgsAndAssetsActivityArgs) (SelectOrgsAndAssetsActivityResult, error) {
	// Get cell ID from ECS cluster name
	cellId := samsaraaws.GetECSClusterName()
	if args.Cell != "" {
		cellId = args.Cell
	}

	// Get org cells
	orgCells, err := a.ReplicaAppModels.OrgCellBridge().ByCellId(ctx, cellId)
	if err != nil {
		return SelectOrgsAndAssetsActivityResult{}, oops.Wrapf(err, "failed to get orgs for cell %s", cellId)
	}

	// Extract org IDs
	var allOrgIds []int64
	for _, orgCell := range orgCells {
		allOrgIds = append(allOrgIds, orgCell.OrgId)
	}

	// Select orgs using the helper function
	selectedOrgs := selectOrgsForValidation(allOrgIds, args.PinnedOrgs, args.NumOrgsValidated)

	// Create a map for quick lookup of selected orgs
	selectedOrgSet := make(map[int64]bool)
	for _, orgId := range selectedOrgs {
		selectedOrgSet[orgId] = true
	}

	// For each selected org, get a sample of assets
	var result []OrgAssetSelection
	for _, orgId := range selectedOrgs {
		// skiplint: +loopedexpensivecall
		assets, err := a.getRandomAssetsForOrg(ctx, orgId, args.NumAssetsValidated, args.AssetType)
		if err != nil {
			// Log error but don't fail the entire workflow
			slog.Errorw(ctx, oops.Wrapf(err, "EMR Validation: Failed to get random assets for org"),
				"orgId", orgId,
				"assetType", args.AssetType,
				"numAssetsRequested", args.NumAssetsValidated)
			continue
		}

		result = append(result, OrgAssetSelection{
			OrgId:    orgId,
			AssetIds: assets,
		})
	}

	return SelectOrgsAndAssetsActivityResult{
		SelectedOrgs: result,
	}, nil
}

func (a SelectOrgsAndAssetsActivity) getRandomAssetsForOrg(ctx context.Context, orgId int64, sampleSize int, assetType emrreplication.AssetType) ([]int64, error) {
	var allAssetIds []int64

	// Get assets based on the specified asset type
	switch assetType {
	case emrreplication.DriverAssetType:
		drivers, err := a.ReplicaAppModels.DriverBridge().ByOrgId(ctx, orgId)
		if err != nil {
			return nil, oops.Wrapf(err, "failed to get drivers for org %d", orgId)
		}
		for _, driver := range drivers {
			allAssetIds = append(allAssetIds, driver.Id)
		}
	case emrreplication.DeviceAssetType:
		devices, err := a.ReplicaAppModels.DeviceBridge().ByOrgId(ctx, orgId)
		if err != nil {
			return nil, oops.Wrapf(err, "failed to get devices for org %d", orgId)
		}
		for _, device := range devices {
			allAssetIds = append(allAssetIds, device.Id)
		}
	case emrreplication.TagAssetType:
		tags, err := a.ReplicaAppModels.TagBridge().ByOrgId(ctx, orgId)
		if err != nil {
			return nil, oops.Wrapf(err, "failed to get tags for org %d", orgId)
		}
		for _, tag := range tags {
			allAssetIds = append(allAssetIds, tag.Id)
		}
	default:
		// Default to devices if asset type is unknown
		devices, err := a.ReplicaAppModels.DeviceBridge().ByOrgId(ctx, orgId)
		if err != nil {
			return nil, oops.Wrapf(err, "failed to get devices for org %d", orgId)
		}
		for _, device := range devices {
			allAssetIds = append(allAssetIds, device.Id)
		}
	}

	// If no assets found, return empty slice
	if len(allAssetIds) == 0 {
		return []int64{}, nil
	}

	// If we have fewer assets than requested sample size, return all
	if len(allAssetIds) <= sampleSize {
		return allAssetIds, nil
	}

	// Randomly sample assets using partial shuffle for efficiency
	// Note: In Go 1.20+, the global random source is automatically seeded, so no need for rand.Seed()
	for i := 0; i < sampleSize; i++ {
		j := rand.Intn(len(allAssetIds)-i) + i
		allAssetIds[i], allAssetIds[j] = allAssetIds[j], allAssetIds[i]
	}

	return allAssetIds[:sampleSize], nil
}
