package databricks

import (
	"context"
	"fmt"
	"net/http"

	"github.com/samsarahq/go/oops"
)

type IPAccessListType string

const (
	AllowIPAccess = IPAccessListType("ALLOW")
	BlockIPAccess = IPAccessListType("BLOCK")
)

type IPAccessListInput struct {
	Label       string           `json:"label"`
	ListType    IPAccessListType `json:"list_type"`
	IPAddresses []string         `json:"ip_addresses"`
}

type IPAccessList struct {
	IPAccessListInput
	ListId       string `json:"list_id"`
	AddressCount int64  `json:"address_count"`
	CreatedAt    int64  `json:"created_at"`
	UpdatedAt    int64  `json:"updated_at"`
	UpdatedBy    int64  `json:"updated_by"`
	Enabled      bool   `json:"enabled"`
}

type CreateIPAccessListInput struct {
	IPAccessListInput
}

type CreateIPAccessListOutput struct {
	IPAccessList IPAccessList `json:"ip_access_list"`
}

type GetAllIPAccessListsOutput struct {
	IPAccessLists []IPAccessList `json:"ip_access_lists"`
}

type GetIPAccessListInput struct {
	IPAccessListId string `json:"-"`
}

type GetIPAccessListOutput struct {
	IPAccessList IPAccessList `json:"ip_access_list"`
}

type EditIPAccessListInput struct {
	IPAccessListInput
	IPAccessListId string `json:"-"`
	Enabled        bool   `json:"enabled"`
}

type EditIPAccessListOutput struct {
	IPAccessList IPAccessList `json:"ip_access_list"`
}

type DeleteIPAccessListInput struct {
	IPAccessListId string `json:"ip_access_list_id"`
}

type DeleteIPAccessListOutput struct{}

// Go Implementation of the Databricks IP Access List API
// https://docs.databricks.com/dev-tools/api/latest/ip-access-list.html
type IPAccessListAPI interface {
	CreateIPAccessList(context.Context, *CreateIPAccessListInput) (*CreateIPAccessListOutput, error)
	GetAllIPAccessLists(context.Context) (*GetAllIPAccessListsOutput, error)
	GetIPAccessList(context.Context, *GetIPAccessListInput) (*GetIPAccessListOutput, error)
	EditIPAccessList(context.Context, *EditIPAccessListInput) (*EditIPAccessListOutput, error)
	DeleteIPAccessList(context.Context, *DeleteIPAccessListInput) error
}

func (c *Client) CreateIPAccessList(ctx context.Context, input *CreateIPAccessListInput) (*CreateIPAccessListOutput, error) {
	var output CreateIPAccessListOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/ip-access-lists", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) GetAllIPAccessLists(ctx context.Context) (*GetAllIPAccessListsOutput, error) {
	var output GetAllIPAccessListsOutput
	if err := c.do(ctx, http.MethodGet, "/api/2.0/ip-access-lists", nil, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) GetIPAccessList(ctx context.Context, input *GetIPAccessListInput) (*GetIPAccessListOutput, error) {
	var output GetIPAccessListOutput
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf("/api/2.0/ip-access-lists/%s", input.IPAccessListId), nil, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) EditIPAccessList(ctx context.Context, input *EditIPAccessListInput) (*EditIPAccessListOutput, error) {
	var output EditIPAccessListOutput
	if err := c.do(ctx, http.MethodPut, fmt.Sprintf("/api/2.0/ip-access-lists/%s", input.IPAccessListId), input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) DeleteIPAccessList(ctx context.Context, input *DeleteIPAccessListInput) error {
	var output DeleteIPAccessListOutput
	if err := c.do(ctx, http.MethodDelete, fmt.Sprintf("/api/2.0/ip-access-lists/%s", input.IPAccessListId), input, &output); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}
