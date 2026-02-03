package databricks

// Client implementation of https://docs.databricks.com/dev-tools/api/latest/instance-profiles.html

import (
	"context"
	"net/http"

	"github.com/samsarahq/go/oops"
)

type InstanceProfile struct {
	InstanceProfileArn    string `json:"instance_profile_arn"`
	IsMetaInstanceProfile bool   `json:"is_meta_instance_profile"`
	IamRoleArn            string `json:"iam_role_arn,omitempty"`
}

type AddInstanceProfileInput struct {
	InstanceProfileArn string `json:"instance_profile_arn"`
	SkipValidation     bool   `json:"skip_validation"`
	IamRoleArn         string `json:"iam_role_arn,omitempty"`
}
type AddInstanceProfileOutput struct{}

type ListInstanceProfilesInput struct{}
type ListInstanceProfilesOutput struct {
	InstanceProfiles []InstanceProfile `json:"instance_profiles"`
}

type RemoveInstanceProfileInput struct {
	InstanceProfileArn string `json:"instance_profile_arn"`
}
type RemoveInstanceProfileOutput struct{}

type EditInstanceProfileInput struct {
	InstanceProfileArn string `json:"instance_profile_arn"`
	IamRoleArn         string `json:"iam_role_arn,omitempty"`
}

type EditInstanceProfileOutput struct{}

type InstanceProfilesAPI interface {
	AddInstanceProfile(context.Context, *AddInstanceProfileInput) (*AddInstanceProfileOutput, error)
	ListInstanceProfiles(context.Context, *ListInstanceProfilesInput) (*ListInstanceProfilesOutput, error)
	RemoveInstanceProfile(context.Context, *RemoveInstanceProfileInput) (*RemoveInstanceProfileOutput, error)
	EditInstanceProfile(context.Context, *EditInstanceProfileInput) (*EditInstanceProfileOutput, error)
}

func (c *Client) AddInstanceProfile(ctx context.Context, input *AddInstanceProfileInput) (*AddInstanceProfileOutput, error) {
	var output AddInstanceProfileOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/instance-profiles/add", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}
func (c *Client) ListInstanceProfiles(ctx context.Context, input *ListInstanceProfilesInput) (*ListInstanceProfilesOutput, error) {
	var output ListInstanceProfilesOutput
	if err := c.do(ctx, http.MethodGet, "/api/2.0/instance-profiles/list", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}
func (c *Client) RemoveInstanceProfile(ctx context.Context, input *RemoveInstanceProfileInput) (*RemoveInstanceProfileOutput, error) {
	var output RemoveInstanceProfileOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/instance-profiles/remove", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}
func (c *Client) EditInstanceProfile(ctx context.Context, input *EditInstanceProfileInput) (*EditInstanceProfileOutput, error) {
	var output EditInstanceProfileOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/instance-profiles/edit", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}
