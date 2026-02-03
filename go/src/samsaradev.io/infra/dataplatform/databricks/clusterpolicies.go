package databricks

import (
	"context"
	"net/http"

	"github.com/samsarahq/go/oops"
)

// Client implementation of (2.0) https://docs.databricks.com/dev-tools/api/latest/policies.html

type ClusterPolicy struct {
	PolicyId             string `json:"policy_id"`
	Name                 string `json:"name"`
	Definition           string `json:"definition"`
	CreatorUserName      string `json:"creator_user_name"`
	CreatedAtTimestampMs int64  `json:"created_at_timestamp"`
}

type GetClusterPolicyInput struct {
	PolicyId string `json:"policy_id"`
}

type GetClusterPolicyOutput struct {
	ClusterPolicy
}

type PolicyListOrder string
type PolicySortColumn string

const (
	ListOrderAsc  PolicyListOrder = "ASC"
	ListOrderDesc PolicyListOrder = "DESC"

	SortColumnCreationTime PolicySortColumn = "POLICY_CREATION_TIME"
	SortColumnName         PolicySortColumn = "POLICY_NAME"
)

type ListClusterPoliciesInput struct {
	// SortOrder defaults to DESC, and SortColumn defaults to POLICY_CREATION_TIME
	SortOrder  *PolicyListOrder  `json:"sort_order"`
	SortColumn *PolicySortColumn `json:"sort_column"`
}

type ListClusterPoliciesOutput struct {
	Policies   []*ClusterPolicy `json:"policies"`
	TotalCount int64            `json:"total_count"`
}

type CreateClusterPolicyInput struct {
	Name       string `json:"name"`
	Definition string `json:"definition"`
}

type CreateClusterPolicyOutput struct {
	PolicyId string `json:"policy_id"`
}

type EditClusterPolicyInput struct {
	PolicyId   string `json:"policy_id"`
	Name       string `json:"name"`
	Definition string `json:"definition"`
}

type EditClusterPolicyOutput struct {
}

type DeleteClusterPolicyInput struct {
	PolicyId string `json:"policy_id"`
}

type DeleteClusterPolicyOutput struct {
}

type ClusterPoliciesAPI interface {
	GetClusterPolicy(context.Context, *GetClusterPolicyInput) (*GetClusterPolicyOutput, error)
	ListClusterPolicies(context.Context, *ListClusterPoliciesInput) (*ListClusterPoliciesOutput, error)
	CreateClusterPolicy(context.Context, *CreateClusterPolicyInput) (*CreateClusterPolicyOutput, error)
	EditClusterPolicy(context.Context, *EditClusterPolicyInput) (*EditClusterPolicyOutput, error)
	DeleteClusterPolicy(context.Context, *DeleteClusterPolicyInput) (*DeleteClusterPolicyOutput, error)
}

func (c *Client) GetClusterPolicy(ctx context.Context, input *GetClusterPolicyInput) (*GetClusterPolicyOutput, error) {
	var output GetClusterPolicyOutput
	if err := c.do(ctx, http.MethodGet, "/api/2.0/policies/clusters/get", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) ListClusterPolicies(ctx context.Context, input *ListClusterPoliciesInput) (*ListClusterPoliciesOutput, error) {
	var output ListClusterPoliciesOutput
	if err := c.do(ctx, http.MethodGet, "/api/2.0/policies/clusters/list", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) CreateClusterPolicy(ctx context.Context, input *CreateClusterPolicyInput) (*CreateClusterPolicyOutput, error) {
	var output CreateClusterPolicyOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/policies/clusters/create", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) EditClusterPolicy(ctx context.Context, input *EditClusterPolicyInput) (*EditClusterPolicyOutput, error) {
	var output EditClusterPolicyOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/policies/clusters/edit", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) DeleteClusterPolicy(ctx context.Context, input *DeleteClusterPolicyInput) (*DeleteClusterPolicyOutput, error) {
	var output DeleteClusterPolicyOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/policies/clusters/delete", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}
