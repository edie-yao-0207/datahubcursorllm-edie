package databricks

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"github.com/samsarahq/go/oops"
)

type Conf string

// Add new Workspace Conf values here
const (
	EnableIPAccessListsConf = Conf("enableIpAccessLists")
)

var allConfs = []Conf{EnableIPAccessListsConf}

type WorkspaceConf struct {
	CustomConfig map[Conf]bool `json:"custom_config"`
}

type GetWorkspaceConfInput struct {
	Keys string `json:"keys"`
}

type GetWorkspaceConfOutput struct {
	EnableIpAccessLists string `json:"enableIpAccessLists"`
}

type EditWorkspaceConfInput struct {
	WorkspaceConf
}

// The WorkspaceConf interface implements the /workspace-conf endpoint
// There is no official Databricks documentation for this API as it is soley used by the IP Access List API
// https://docs.databricks.com/dev-tools/api/latest/ip-access-list.html
// In the future they may add more workspace conf which we believe will be added to this API
type WorkspaceConfAPI interface {
	GetWorkspaceConf(context.Context) (*GetWorkspaceConfOutput, error)
	EditWorkspaceConf(context.Context, *EditWorkspaceConfInput) error
}

func (c *Client) GetWorkspaceConf(ctx context.Context) (*GetWorkspaceConfOutput, error) {
	var output GetWorkspaceConfOutput
	strConfs := make([]string, 0, len(allConfs))
	for _, conf := range allConfs {
		strConfs = append(strConfs, string(conf))
	}

	getworkspaceConfInput := GetWorkspaceConfInput{
		Keys: strings.Join(strConfs, ","),
	}

	if err := c.do(ctx, http.MethodGet, "/api/2.0/workspace-conf", getworkspaceConfInput, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) EditWorkspaceConf(ctx context.Context, input *EditWorkspaceConfInput) error {
	// The workspace-conf endpoint wants booleans in string
	// Stringify EditWorkspaceConfInput values to adhere to API structure
	payload := make(map[string]string, len(input.WorkspaceConf.CustomConfig))
	for k, v := range input.WorkspaceConf.CustomConfig {
		payload[string(k)] = strconv.FormatBool(v)
	}

	if err := c.do(ctx, http.MethodPatch, "/api/2.0/workspace-conf", payload, nil); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}
