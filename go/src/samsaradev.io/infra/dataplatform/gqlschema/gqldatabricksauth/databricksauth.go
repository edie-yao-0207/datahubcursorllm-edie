package gqldatabricksauth

import (
	"context"
	"time"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/fleet/dataproducts/databricksauthtokensdocuments"
	"samsaradev.io/fleet/dataproducts/dataproductsproto"
	"samsaradev.io/infra/thunder/batch"
	"samsaradev.io/models"
)

type DatabricksUserAuthStatusResponse struct {
	AccessTokenExpired              bool
	RefreshTokenExpired             bool
	AccessTokenExpireAtUnixSeconds  int64
	RefreshTokenExpireAtUnixSeconds int64
}

// getDatabricksUserAuthStatusBatch returns the status of users' Databricks auth tokens in a batched manner.
func (s *Server) getDatabricksUserAuthStatusBatch(ctx context.Context, userBatch map[batch.Index]*models.User) (map[batch.Index]*DatabricksUserAuthStatusResponse, error) {
	result := make(map[batch.Index]*DatabricksUserAuthStatusResponse, len(userBatch))

	// Process each user in the batch
	for idx, user := range userBatch {
		if user == nil {
			result[idx] = nil
			continue
		}

		req := &dataproductsproto.GetDatabricksUserAuthStatusRequest{
			UserEmail:               user.Email,
			DatabricksWorkspaceHost: s.Config.DatabricksE2UrlBase,
			AccessScope:             databricksauthtokensdocuments.AccessScopeToProto(databricksauthtokensdocuments.AccessScopeSQL),
		}
		// skiplint: +loopedexpensivecall
		item, err := s.BenchmarksServiceClient.GetDatabricksUserAuthStatus(ctx, req)
		if err != nil {
			return nil, oops.Wrapf(err, "get databricks user auth status by user: %d (%s)", user.Id, user.Email)
		}

		if item == nil || item.DatabricksUserAuthStatus == nil {
			result[idx] = nil
			continue
		}

		databricksUserAuthStatusResponse := &DatabricksUserAuthStatusResponse{
			AccessTokenExpired:              int64(item.DatabricksUserAuthStatus.AccessTokenExpireAtUnixSeconds) < time.Now().Unix(),
			RefreshTokenExpired:             int64(item.DatabricksUserAuthStatus.RefreshTokenExpireAtUnixSeconds) < time.Now().Unix(),
			AccessTokenExpireAtUnixSeconds:  int64(item.DatabricksUserAuthStatus.AccessTokenExpireAtUnixSeconds),
			RefreshTokenExpireAtUnixSeconds: int64(item.DatabricksUserAuthStatus.RefreshTokenExpireAtUnixSeconds),
		}
		result[idx] = databricksUserAuthStatusResponse
	}

	return result, nil
}
