package dataplatformresource

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
)

// OktaSSOCognitoPool sets up Cognito resources required for ALB SAML
// integration, which lets us log into internal applications using Okta.
//
// The Cognito and Okta configurations depend on each other. To bootstrap, (1)
// create an Okta App using a placeholder "Single Sign On URL", (2) set up a
// Cognito pool using the app's metadata file and URL, and (3) go back to Okta
// and update the app's Single Sign On URL to match the Cognito domain and
// Audience Restriction to match the Cognito pool ID.
type OktaSSOCognitoPool struct {
	// Name is the name of the Okta integration. It must be unique within our AWS
	// account.
	Name string

	// LoadBalancerDomain is the domain name where the ALB will be hosting
	// traffic. It can be a default ALB domain name, or a custom domain name, e.g.
	// "foo.internal.samsara.com".
	LoadBalancerDomain string

	// OktaMetadataFile comes from "Identity Provider metadata" in App Settings in
	// Okta. It should be an XML file.
	OktaMetadataFile string

	// OktaSSORedirectBindingURI comes from App Settings in Okta. It should look like
	// "https://samsara.oktapreview.com/app/samsara_amundsen_1/exkzts0ou2ajPaul10h7/sso/saml".
	OktaSSORedirectBindingURI string
}

func (p *OktaSSOCognitoPool) UserPool() *awsresource.CognitoUserPool {
	return &awsresource.CognitoUserPool{
		BaseResource: tf.BaseResource{
			MetaParameters: tf.MetaParameters{
				Lifecycle: tf.Lifecycle{
					// We must provide Pool ID from Okta and it has to be match. If we
					// accidentally change it by recreating the pool, we would also need
					// to ask IT to change it in Okta, which takes time.
					PreventDestroy: true,
				},
			},
		},
		Name: p.Name,
		Schema: []awsresource.CognitoAttribute{
			{
				Name:                   "email",
				AttributeDataType:      "String",
				DeveloperOnlyAttribute: false,
				Mutable:                true,
				Required:               true,
				StringAttributeConstraints: &awsresource.CognitoStringAttributeConstraints{
					MinLength: 0,
					MaxLength: 2048,
				},
			},
		},
	}
}

func (p *OktaSSOCognitoPool) UserPoolDomain() *awsresource.CognitoUserPoolDomain {
	return &awsresource.CognitoUserPoolDomain{
		// Domain specifies the Cognito hostname that will serve SAML requests to be
		// <domain>.auth.<region>.amazoncognito.com. This is the hostname we will
		// use in app settings on Okta's side. The domain seems to be global within
		// a region, so we prepend "samsara-" and hope it's sufficiently unique.
		Domain:     "samsara-" + p.Name,
		UserPoolId: p.UserPool().ResourceId().Reference(),
	}
}

func (p *OktaSSOCognitoPool) UserPoolClient() *awsresource.CognitoUserPoolClient {
	return &awsresource.CognitoUserPoolClient{
		Name:                            p.Name,
		UserPoolId:                      p.UserPool().ResourceId().Reference(),
		AllowedOAuthFlowsUserPoolClient: aws.Bool(true),
		AllowedOauthFlows: []string{
			"code",
			"implicit",
		},
		AllowedOauthScopes: []string{
			"email",
			"openid",
			"profile",
		},
		CallbackUrls: []string{
			fmt.Sprintf("https://%s/oauth2/idpresponse", p.LoadBalancerDomain),
		},
		LogoutUrls: []string{
			fmt.Sprintf("https://%s/logout", p.LoadBalancerDomain),
		},
		ExplicitAuthFlows: []string{
			"ALLOW_REFRESH_TOKEN_AUTH",
		},
		GenerateSecret: aws.Bool(true),
		ReadAttributes: []string{
			"email",
		},
		WriteAttributes: []string{
			"email",
		},
		SupportedIdentityProviders: []string{
			p.IdentityProvider().ResourceId().ReferenceAttr("provider_name"),
		},
	}
}

func (p *OktaSSOCognitoPool) IdentityProvider() *awsresource.CognitoIdentityProvider {
	return &awsresource.CognitoIdentityProvider{
		UserPoolId:   p.UserPool().ResourceId().Reference(),
		ProviderName: p.Name + "-okta",
		ProviderType: "SAML",
		AttributeMapping: map[string]string{
			"email":    "email",
			"username": "username",
		},
		ProviderDetails: map[string]string{
			"IDPSignout": "false",
			// Terraform HCL encoder doesn't escape quotes correctly.
			"MetadataFile":          strings.ReplaceAll(p.OktaMetadataFile, `"`, `\"`),
			"SSORedirectBindingURI": p.OktaSSORedirectBindingURI,
		},
	}
}

func (p *OktaSSOCognitoPool) Resources() []tf.Resource {
	return []tf.Resource{
		p.UserPool(),
		p.UserPoolDomain(),
		p.UserPoolClient(),
		p.IdentityProvider(),
	}
}
