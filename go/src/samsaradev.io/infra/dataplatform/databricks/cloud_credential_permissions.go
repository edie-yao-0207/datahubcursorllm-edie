package databricks

import (
	"context"
	"fmt"
	"net/http"
)

const apiUrl = "/api/2.1/unity-catalog/permissions/credential"

const CredentialAccess = "ACCESS"

type Change struct {
	Principal string   `json:"principal"`
	Add       []string `json:"add,omitempty"`
	Remove    []string `json:"remove,omitempty"`
}

type PatchCredentialPermissionsInput struct {
	Name    string   `json:"name"`
	Changes []Change `json:"changes"`
}

type PrivilegeAssignments struct {
	PrivilegeAssignments []PrivilegeAssignment `json:"privilege_assignments"`
}

type PrivilegeAssignment struct {
	Principal  string   `json:"principal"`
	Privileges []string `json:"privileges"`
}

type ListCredentialPermissionsInput struct {
	Name string `json:"name"`
}

type CloudCredentialPermissionsAPI interface {
	ListCredentialPermissions(context.Context, *ListCredentialPermissionsInput) (*PrivilegeAssignments, error)
	ChangeCredentialPermissions(context.Context, *PatchCredentialPermissionsInput) (*PrivilegeAssignments, error)
}

func (c *Client) ListCredentialPermissions(ctx context.Context, input *ListCredentialPermissionsInput) (*PrivilegeAssignments, error) {
	var output PrivilegeAssignments
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf("%s/%s", apiUrl, input.Name), nil, &output); err != nil {
		return nil, err
	}
	return &output, nil
}

func (c *Client) ChangeCredentialPermissions(ctx context.Context, input *PatchCredentialPermissionsInput) (*PrivilegeAssignments, error) {
	type internalChangesStruct struct {
		Changes []Change `json:"changes"`
	}

	var output PrivilegeAssignments
	if err := c.do(ctx, http.MethodPatch, fmt.Sprintf("%s/%s", apiUrl, input.Name), internalChangesStruct{input.Changes}, &output); err != nil {
		return nil, err
	}
	return &output, nil
}
