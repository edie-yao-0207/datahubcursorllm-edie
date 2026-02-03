package databricks

import (
	"context"
	"net/http"
	"time"

	"github.com/samsarahq/go/oops"
)

type SCIMSchema string

const (
	SCIMSchemaCoreUser = SCIMSchema("urn:ietf:params:scim:schemas:core:2.0:User")
	SCIMSchemaPatchOp  = SCIMSchema("urn:ietf:params:scim:api:messages:2.0:PatchOp")
)

type Op string

const (
	OpAdd    = Op("add")
	OpRemove = Op("remove")
)

type UserGroup struct {
	GroupId string `json:"value"`
}

type Entitlement struct {
	Value string `json:"value"`
}

type Role struct {
	Value string `json:"value"`
}

type User struct {
	Id           string         `json:"id"`
	UserName     string         `json:"userName"`
	DisplayName  string         `json:"displayName,omitempty"`
	Groups       []*UserGroup   `json:"groups,omitempty"`
	Entitlements []*Entitlement `json:"entitlements,omitempty"`
	Roles        []*Role        `json:"roles,omitempty"`
}

type PaginationParameters struct {
	StartIndex int `json:"startIndex,omitempty"`
	Count      int `json:"count,omitempty"`
}

type PaginationAttributes struct {
	TotalResults int          `json:"totalResults"`
	ItemsPerPage int          `json:"itemsPerPage"`
	StartIndex   int          `json:"StartIndex"`
	Schemas      []SCIMSchema `json:"schemas"`
}

type GetUsersInput struct {
	Attributes []string `json:"attributes"`
	Filter     string   `json:"filter,omitempty"`
}

type GetUsersOutput struct {
	Users []*User `json:"-"`
}

type GetUserInput struct {
	Id string `json:"-"`
}

type GetUserOutput struct {
	User
}

type CreateUserInput struct {
	User
}

type CreateUserOutput struct {
	User
}

type PatchOperationValue struct {
	Value interface{} `json:"value"`
}

type PatchOperation struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value"`
}

type PatchUserInput struct {
	Id         string            `json:"-"`
	Operations []*PatchOperation `json:"Operations"`
}

type PatchUserOutput struct{}

type DeleteUserInput struct {
	Id string `json:"-"`
}

type DeleteUserOutput struct{}

type MeInput struct{}

type UsersAPI interface {
	GetUsers(context.Context, *GetUsersInput) (*GetUsersOutput, error)
	GetUser(context.Context, *GetUserInput) (*GetUserOutput, error)
	CreateUser(context.Context, *CreateUserInput) (*CreateUserOutput, error)
	PatchUser(context.Context, *PatchUserInput) (*PatchUserOutput, error)
	DeleteUser(context.Context, *DeleteUserInput) (*DeleteUserOutput, error)
	Me(ctx context.Context, input *MeInput) (*GetUserOutput, error)
}

func (c *Client) GetUsers(ctx context.Context, input *GetUsersInput) (*GetUsersOutput, error) {
	// Paginate.
	// https://ldapwiki.com/wiki/SCIM%20Pagination
	var output GetUsersOutput

	type paginatedInput struct {
		PaginationParameters
		*GetUsersInput
	}

	type paginatedOutput struct {
		PaginationAttributes
		Resources []*User `json:"Resources"`
	}

	itemsPerPage := 100
	pageInput := paginatedInput{
		PaginationParameters: PaginationParameters{
			StartIndex: 1,
			Count:      itemsPerPage,
		},
		GetUsersInput: input,
	}
	for {
		var pageOutput paginatedOutput
		if err := c.do(ctx, http.MethodGet, "/api/2.0/preview/scim/v2/Users", pageInput, &pageOutput); err != nil {
			return nil, oops.Wrapf(err, "")
		}
		output.Users = append(output.Users, pageOutput.Resources...)

		pageInput.StartIndex += pageOutput.PaginationAttributes.ItemsPerPage
		if pageInput.StartIndex > pageOutput.PaginationAttributes.TotalResults {
			break
		}

		// We're hitting up against the rate limit set by Databricks due to the number of users we now have.
		// https://docs.databricks.com/en/resources/limits.html#api-rate-limits
		// By sleeping for 5 seconds between each paginated list of users, we avoid hitting this rate limit.
		time.Sleep(5 * time.Second)
	}

	return &output, nil
}

func (c *Client) GetUser(ctx context.Context, input *GetUserInput) (*GetUserOutput, error) {
	var output GetUserOutput
	if err := c.do(ctx, http.MethodGet, "/api/2.0/preview/scim/v2/Users/"+input.Id, input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) CreateUser(ctx context.Context, input *CreateUserInput) (*CreateUserOutput, error) {
	wrappedInput := struct {
		Schema SCIMSchema `json:"schema"`
		*CreateUserInput
	}{
		Schema:          SCIMSchemaCoreUser,
		CreateUserInput: input,
	}
	var output CreateUserOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/preview/scim/v2/Users", wrappedInput, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) PatchUser(ctx context.Context, input *PatchUserInput) (*PatchUserOutput, error) {
	wrappedInput := struct {
		Schemas []SCIMSchema `json:"schemas"`
		*PatchUserInput
	}{
		Schemas:        []SCIMSchema{SCIMSchemaPatchOp},
		PatchUserInput: input,
	}
	var output PatchUserOutput
	if err := c.do(ctx, http.MethodPatch, "/api/2.0/preview/scim/v2/Users/"+input.Id, wrappedInput, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}

	// We're hitting up against the rate limit set by Databricks. This patch API has rate limit of 2/s
	// https://docs.databricks.com/en/resources/limits.html#api-rate-limits
	// We dont anticipate lot of patches, but there is one time backlog of patch 400+ users likely due to
	// SCIM provisioning change that removed "(Managed by Terraform)" from the display name.
	// By sleeping for 4 seconds, we avoid hitting this rate limit and once backlog is cleared we can potentially reduce it.
	time.Sleep(4 * time.Second)

	return &output, nil
}

func (c *Client) DeleteUser(ctx context.Context, input *DeleteUserInput) (*DeleteUserOutput, error) {
	var output DeleteUserOutput
	if err := c.do(ctx, http.MethodDelete, "/api/2.0/preview/scim/v2/Users/"+input.Id, input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) Me(ctx context.Context, input *MeInput) (*GetUserOutput, error) {
	var output GetUserOutput
	if err := c.do(ctx, http.MethodGet, "/api/2.0/preview/scim/v2/Me", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil

}
