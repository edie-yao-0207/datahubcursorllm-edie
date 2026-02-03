package activity

import (
	"context"
	"slices"

	"github.com/samsarahq/go/oops"
	"go.uber.org/fx"

	"samsaradev.io/infra/samsaraaws"
	"samsaradev.io/infra/workflows/workflowregistry"
	"samsaradev.io/models"
)

func init() {
	workflowregistry.MustRegisterActivityConstructor(NewGetAllOrganizationsActivity)
}

type GetAllOrganizationsActivityArgs struct {
	OrgIds        *[]int64
	ExcludeOrgIds *[]int64
}

type GetAllOrganizationsActivity struct {
	ReplicaAppModels *models.AppModels
}

type GetAllOrganizationsActivityParams struct {
	fx.In
	ReplicaAppModels models.ReplicaAppModelsInput
}

type GetAllOrganizationsActivityResult struct {
	OrgIds []int64
}

func NewGetAllOrganizationsActivity(p GetAllOrganizationsActivityParams) *GetAllOrganizationsActivity {
	return &GetAllOrganizationsActivity{
		ReplicaAppModels: p.ReplicaAppModels.Models,
	}
}

func (a GetAllOrganizationsActivity) Name() string {
	return "GetAllOrganizationsActivity"
}

func (a GetAllOrganizationsActivity) Execute(ctx context.Context, args *GetAllOrganizationsActivityArgs) (GetAllOrganizationsActivityResult, error) {
	cellId := samsaraaws.GetECSClusterName()
	orgCells, err := a.ReplicaAppModels.OrgCell.ByCellId(ctx, cellId)
	if err != nil {
		return GetAllOrganizationsActivityResult{}, oops.Wrapf(err, "failed to get org cells for cell %s", cellId)
	}

	// Extract org IDs from the org cells.
	orgIdsInCell := make([]int64, len(orgCells))
	for i, orgCell := range orgCells {
		orgIdsInCell[i] = orgCell.OrgId
	}

	orgIds := make([]int64, 0)
	for _, orgId := range orgIdsInCell {
		// If we have a list of orgIds to filter by, skip any orgs that are not in
		// the list.
		if args.OrgIds != nil && !slices.Contains(*args.OrgIds, orgId) {
			continue
		}
		// If we have a list of orgIds to exclude, skip any orgs that are in the
		// list.
		if args.ExcludeOrgIds != nil && slices.Contains(*args.ExcludeOrgIds, orgId) {
			continue
		}
		orgIds = append(orgIds, orgId)
	}
	return GetAllOrganizationsActivityResult{
		OrgIds: orgIds,
	}, nil
}
