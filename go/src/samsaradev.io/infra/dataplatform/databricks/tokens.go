package databricks

import (
	"context"
	"net/http"

	"github.com/samsarahq/go/oops"
)

type TokenInfo struct {
	TokenId      string `json:"token_id"`
	CreationTime int64  `json:"creation_time"`
	ExpiryTime   int64  `json:"expiry_time"`
	Comment      string `json:"comment"`
}

type CreateTokenInput struct {
	LifetimeSeconds int64  `json:"lifetime_seconds"`
	Comment         string `json:"comment"`
}

type CreateTokenOutput struct {
	TokenValue string    `json:"token_value"`
	TokenInfo  TokenInfo `json:"token_info"`
}

type ListTokensInput struct{}

type ListTokensOutput struct {
	TokenInfos []TokenInfo `json:"token_infos"`
}

type DeleteTokenInput struct {
	TokenId string `json:"token_id"`
}

type DeleteTokenOutput struct{}

type TokensAPI interface {
	CreateToken(context.Context, *CreateTokenInput) (*CreateTokenOutput, error)
	ListTokens(context.Context, *ListTokensInput) (*ListTokensOutput, error)
	DeleteToken(context.Context, *DeleteTokenInput) (*DeleteTokenOutput, error)
}

func (c *Client) CreateToken(ctx context.Context, input *CreateTokenInput) (*CreateTokenOutput, error) {
	var output CreateTokenOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/token/create", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}
func (c *Client) ListTokens(ctx context.Context, input *ListTokensInput) (*ListTokensOutput, error) {
	var output ListTokensOutput
	if err := c.do(ctx, http.MethodGet, "/api/2.0/token/list", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}
func (c *Client) DeleteToken(ctx context.Context, input *DeleteTokenInput) (*DeleteTokenOutput, error) {
	var output DeleteTokenOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/token/delete", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}
