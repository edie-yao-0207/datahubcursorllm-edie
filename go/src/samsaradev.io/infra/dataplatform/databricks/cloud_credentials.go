package databricks

import (
	"context"
	"fmt"
	"net/http"

	"github.com/samsarahq/go/oops"
)

// Cloud credentials are a private preview feature.
// Much of this API is inferred from using the API and from the documentation
// present here
// https://docs.google.com/document/d/1gi9stfg9TlZz97LncHd71fzDV4DI5ZWpTy7dCiHNmxA/view
// There is a copy in the repo as well for us.

const apiEndpoint = "/api/2.1/unity-catalog/credentials"
const CredentialPurposeService = "SERVICE"

type CloudCredential struct {
	Name          string                    `json:"name"`
	AwsIamRole    CloudCredentialAwsIamRole `json:"aws_iam_role"`
	ReadOnly      bool                      `json:"read_only"`
	Purpose       string                    `json:"purpose"`
	Owner         string                    `json:"owner"`
	ID            string                    `json:"id"`
	MetastoreID   string                    `json:"metastore_id"`
	CreatedAt     int64                     `json:"created_at"`
	CreatedBy     string                    `json:"created_by"`
	UpdatedAt     int64                     `json:"updated_at"`
	UpdatedBy     string                    `json:"updated_by"`
	FullName      string                    `json:"full_name"`
	SecurableType string                    `json:"securable_type"`
	SecurableKind string                    `json:"securable_kind"`
	IsolationMode string                    `json:"isolation_mode"`
}

type GetCredentialOutput struct {
	CredentialInfo CloudCredential `json:"credential_info"`
}

type ListCredentialsOutput struct {
	Credentials []CloudCredential `json:"credentials"`
}

type CloudCredentialAwsIamRole struct {
	RoleArn            string `json:"role_arn"`
	UnityCatalogIamArn string `json:"unity_catalog_iam_arn"`
	ExternalID         string `json:"external_id"`
}

type CreateCredentialInput struct {
	Name    string `json:"name"`
	Purpose string `json:"purpose"`

	// When creating, we should only specify the `RoleArn`.
	// the other fields are set by databricks in the response.
	AwsIamRole CloudCredentialAwsIamRole `json:"aws_iam_role"`
}

type GetCredentialInput struct {
	Name string `json:"name"`
}

type DeleteCredentialInput struct {
	Name string `json:"name"`
}

type DeleteCredentialOutput struct {
}

// Note that the API has an `update` endpoint, but I haven't added it for now.
// Updating doesn't seem necessary at this moment because we can just recreate the credential if necessary
// (and users access them by name, so nothing changes there).
type CloudCredentialAPI interface {
	ListCloudCredentials(ctx context.Context) (*ListCredentialsOutput, error)
	GetCloudCredential(ctx context.Context, input *GetCredentialInput) (*GetCredentialOutput, error)
	CreateCloudCredential(ctx context.Context, input *CreateCredentialInput) (*CloudCredential, error)
	DeleteCloudCredential(ctx context.Context, input *DeleteCredentialInput) (*DeleteCredentialOutput, error)
}

func (c *Client) ListCloudCredentials(ctx context.Context) (*ListCredentialsOutput, error) {
	var rawOutput ListCredentialsOutput
	if err := c.do(ctx, http.MethodGet, apiEndpoint, nil, &rawOutput); err != nil {
		return nil, oops.Wrapf(err, "")
	}

	// Based on the docs, listing credentials returns a bunch of other things (like
	// storage credentials).
	// We need to filter out entries that are not of purpose "SERVICE"
	var output ListCredentialsOutput
	for _, credential := range rawOutput.Credentials {
		if credential.Purpose == CredentialPurposeService {
			output.Credentials = append(output.Credentials, credential)
		}
	}

	return &output, nil
}

func (c *Client) GetCloudCredential(ctx context.Context, input *GetCredentialInput) (*GetCredentialOutput, error) {
	var output GetCredentialOutput
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf("%s/%s", apiEndpoint, input.Name), nil, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) CreateCloudCredential(ctx context.Context, input *CreateCredentialInput) (*CloudCredential, error) {
	var output CloudCredential
	if input.Purpose != "SERVICE" {
		return nil, oops.Errorf("Only valid purpose as of today is SERVICE. Found invalid purpose: %s", input.Purpose)
	}
	if err := c.do(ctx, http.MethodPost, apiEndpoint, input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) DeleteCloudCredential(ctx context.Context, input *DeleteCredentialInput) (*DeleteCredentialOutput, error) {
	var output DeleteCredentialOutput
	if err := c.do(ctx, http.MethodDelete, fmt.Sprintf("%s/%s", apiEndpoint, input.Name), nil, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}
