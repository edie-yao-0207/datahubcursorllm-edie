package databricks

import (
	"context"
	"net/http"

	"github.com/samsarahq/go/oops"
)

const SCIMSchemaCoreGroup = SCIMSchema("urn:ietf:params:scim:schemas:core:2.0:Group")

type GroupMember struct {
	Value string `json:"value"`
}

type Group struct {
	Id           string         `json:"id"`
	DisplayName  string         `json:"displayName"`
	Members      []*GroupMember `json:"members"`
	Entitlements []*Entitlement `json:"entitlements,omitempty"`
	Roles        []*Role        `json:"roles,omitempty"`
}

type GetGroupsInput struct {
	Attributes []string `json:"attributes"`
}

type GetGroupsOutput struct {
	Groups []*Group `json:"-"`
}

type GetGroupInput struct {
	Id string `json:"-"`
}

type GetGroupOutput struct {
	Group
}

type CreateGroupInput struct {
	Group
}

type CreateGroupOutput struct {
	Group
}

type PatchGroupInput struct {
	Id         string            `json:"-"`
	Operations []*PatchOperation `json:"Operations"`
}

type PatchGroupOutput struct {
	Group
}

type DeleteGroupInput struct {
	Id string `json:"-"`
}

type DeleteGroupOutput struct{}

type GroupsAPI interface {
	GetGroups(context.Context, *GetGroupsInput) (*GetGroupsOutput, error)
	GetGroup(context.Context, *GetGroupInput) (*GetGroupOutput, error)
	CreateGroup(context.Context, *CreateGroupInput) (*CreateGroupOutput, error)
	PatchGroup(context.Context, *PatchGroupInput) (*PatchGroupOutput, error)
	DeleteGroup(context.Context, *DeleteGroupInput) (*DeleteGroupOutput, error)
}

func (c *Client) GetGroups(ctx context.Context, input *GetGroupsInput) (*GetGroupsOutput, error) {
	// Paginate.
	// https://ldapwiki.com/wiki/SCIM%20Pagination
	var output GetGroupsOutput

	type paginatedInput struct {
		PaginationParameters
		*GetGroupsInput
	}

	type paginatedOutput struct {
		PaginationAttributes
		Resources []*Group `json:"Resources"`
	}

	itemsPerPage := 100
	pageInput := paginatedInput{
		PaginationParameters: PaginationParameters{
			StartIndex: 1,
			Count:      itemsPerPage,
		},
		GetGroupsInput: input,
	}
	for {
		var pageOutput paginatedOutput
		if err := c.do(ctx, http.MethodGet, "/api/2.0/preview/scim/v2/Groups", pageInput, &pageOutput); err != nil {
			return nil, oops.Wrapf(err, "")
		}
		output.Groups = append(output.Groups, pageOutput.Resources...)

		pageInput.StartIndex += pageOutput.PaginationAttributes.ItemsPerPage
		if pageInput.StartIndex > pageOutput.PaginationAttributes.TotalResults {
			break
		}
	}

	return &output, nil
}

func (c *Client) GetGroup(ctx context.Context, input *GetGroupInput) (*GetGroupOutput, error) {
	var output GetGroupOutput
	if err := c.do(ctx, http.MethodGet, "/api/2.0/preview/scim/v2/Groups/"+input.Id, input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) CreateGroup(ctx context.Context, input *CreateGroupInput) (*CreateGroupOutput, error) {
	wrappedInput := struct {
		Schema SCIMSchema `json:"schema"`
		*CreateGroupInput
	}{
		Schema:           SCIMSchemaCoreGroup,
		CreateGroupInput: input,
	}
	var output CreateGroupOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/preview/scim/v2/Groups", wrappedInput, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) PatchGroup(ctx context.Context, input *PatchGroupInput) (*PatchGroupOutput, error) {
	wrappedInput := struct {
		Schemas []SCIMSchema `json:"schemas"`
		*PatchGroupInput
	}{
		Schemas:         []SCIMSchema{SCIMSchemaPatchOp},
		PatchGroupInput: input,
	}
	var output PatchGroupOutput
	if err := c.do(ctx, http.MethodPatch, "/api/2.0/preview/scim/v2/Groups/"+input.Id, wrappedInput, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) DeleteGroup(ctx context.Context, input *DeleteGroupInput) (*DeleteGroupOutput, error) {
	var output DeleteGroupOutput
	if err := c.do(ctx, http.MethodDelete, "/api/2.0/preview/scim/v2/Groups/"+input.Id, input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}
