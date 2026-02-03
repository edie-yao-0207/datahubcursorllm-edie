package dataplatformmonitors

import (
	"fmt"

	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/libs/ni/infraconsts"
)

type databricksJobSearchLink struct {
	region string

	// If job id is provided, we'll link directly to the job. If not,
	// we'll link to the search page with the provided search key.
	jobId     string
	searchKey string
}

func (d databricksJobSearchLink) generateLink() string {
	workspaceId := dataplatformconsts.GetDevDatabricksWorkspaceIdForRegion(d.region)
	baseUrl := infraconsts.SamsaraDevDatabricksWorkspaceURLBase
	if d.region == infraconsts.SamsaraAWSEURegion {
		baseUrl = infraconsts.SamsaraEUDevDatabricksWorkspaceURLBase
	} else if d.region == infraconsts.SamsaraAWSCARegion {
		baseUrl = infraconsts.SamsaraCADevDatabricksWorkspaceURLBase
	}

	if d.jobId != "" {
		return fmt.Sprintf("%s?o=%d#job/%s", baseUrl, workspaceId, d.jobId)
	}

	return fmt.Sprintf("%s/jobs?o=%d&acl=all&query=%s", baseUrl, workspaceId, d.searchKey)
}
