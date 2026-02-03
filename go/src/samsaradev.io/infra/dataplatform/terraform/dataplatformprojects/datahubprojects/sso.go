package datahubprojects

import (
	"fmt"

	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/libs/ni/infraconsts"
)

func ssoResources(config dataplatformconfig.DatabricksConfig) map[string][]tf.Resource {
	var resources []tf.Resource
	var subdomain string
	switch config.Region {
	case infraconsts.SamsaraAWSDefaultRegion:
		subdomain = "internal"
	case infraconsts.SamsaraAWSEURegion:
		subdomain = "internal.eu"
	case infraconsts.SamsaraAWSCARegion:
		subdomain = "internal.ca"
	}
	sso := dataplatformresource.OktaSSOCognitoPool{
		Name:               DatahubResourceBaseName,
		LoadBalancerDomain: fmt.Sprintf("datahub.%s.samsara.com", subdomain),
		OktaMetadataFile: `<?xml version="1.0" encoding="UTF-8"?><md:EntityDescriptor entityID="http://www.okta.com/exkf3aio96HbvcqZq4h7" xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata"><md:IDPSSODescriptor WantAuthnRequestsSigned="false" protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol"><md:KeyDescriptor use="signing"><ds:KeyInfo xmlns:ds="http://www.w3.org/2000/09/xmldsig#"><ds:X509Data><ds:X509Certificate>MIIDpjCCAo6gAwIBAgIGAZWANcAYMA0GCSqGSIb3DQEBCwUAMIGTMQswCQYDVQQGEwJVUzETMBEG
A1UECAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwNU2FuIEZyYW5jaXNjbzENMAsGA1UECgwET2t0YTEU
MBIGA1UECwwLU1NPUHJvdmlkZXIxFDASBgNVBAMMC3NhbXNhcmFyYW1wMRwwGgYJKoZIhvcNAQkB
Fg1pbmZvQG9rdGEuY29tMB4XDTI1MDMxMDEzMTgyMVoXDTM1MDMxMDEzMTkyMFowgZMxCzAJBgNV
BAYTAlVTMRMwEQYDVQQIDApDYWxpZm9ybmlhMRYwFAYDVQQHDA1TYW4gRnJhbmNpc2NvMQ0wCwYD
VQQKDARPa3RhMRQwEgYDVQQLDAtTU09Qcm92aWRlcjEUMBIGA1UEAwwLc2Ftc2FyYXJhbXAxHDAa
BgkqhkiG9w0BCQEWDWluZm9Ab2t0YS5jb20wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIB
AQCw6ZkdNTGad+AHUPAL3HqH8xYa56vjDNen2X2bhcSd0PgW3OWc4y4rqcAE/KbHhF2dDSC56LAl
pD1B+bjLxaGFGzTLBKT/BBR1rCfZjwrg78s83bNAAplhXmVBsKU+90qguhJBJ4lCM9Ca/MPb9AxN
UjntbS5fAzKHXOa9cmIgrf+xDU4e9Z43zx7ulRe4tlQQR90Ht2LBTe4pzBXRYP6TjIVGifvl2u/9
yFIAN4aFYWBxPJXfl6xfGx7fy8M7LxOZq1oYQU9AiCI9kSuiTP+15kSeN5fFgC1Yfx7GeSM6w508
uaqdUFNpwpbP3MgvAUmv8OF6yMYZPktYY9OjU2sZAgMBAAEwDQYJKoZIhvcNAQELBQADggEBAI9B
kn70e9JvlwkHbg3Xc+DiZJ6V/dS299PDBDKclPz78BXraw23Vz/i90A0VteW3kRQSee0piPduOAK
tF8/8ySPdY5AukmooHCtF4buGn51ejGruYWuUNvztwDckBNADuHizFTUdWjoGfm055Cu9jiQXtos
UVFkyHLBYBl2W9Gexc8PWBbVkWeglzEFMBBypkZn4DEXmkTgyrhEHYqeDjcdiXYlC8sDEIeCCNSI
G7l/jT4+RwLDFdprsmBiVGDvnQaaCGfez5TNTWTaq1aJMiq65BrTdTnMfjp/5u4Thk+WInSRxQPa
wF/lGyQUK2rV+zmua2aWOHs8mForMyYOi0g=</ds:X509Certificate></ds:X509Data></ds:KeyInfo></md:KeyDescriptor><md:NameIDFormat>urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified</md:NameIDFormat><md:NameIDFormat>urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress</md:NameIDFormat><md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST" Location="https://samsararamp.samsara.com/app/samsararamp_datahubcognitosamlsr_1/exkf3aio96HbvcqZq4h7/sso/saml"/><md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="https://samsararamp.samsara.com/app/samsararamp_datahubcognitosamlsr_1/exkf3aio96HbvcqZq4h7/sso/saml"/></md:IDPSSODescriptor></md:EntityDescriptor>`,
		OktaSSORedirectBindingURI: "https://samsararamp.samsara.com/app/samsararamp_datahubcognitosamlsr_1/exkf3aio96HbvcqZq4h7/sso/saml",
	}

	resources = append(resources, sso.Resources()...)

	return map[string][]tf.Resource{
		"sso": resources,
	}
}
