package dagsterprojects

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
		Name:               DagsterResourceBaseName,
		LoadBalancerDomain: fmt.Sprintf("dagster.%s.samsara.com", subdomain),
		OktaMetadataFile: `<?xml version="1.0" encoding="UTF-8"?><md:EntityDescriptor entityID="http://www.okta.com/exke3hqj89veNCgx54h7" xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata"><md:IDPSSODescriptor WantAuthnRequestsSigned="false" protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol"><md:KeyDescriptor use="signing"><ds:KeyInfo xmlns:ds="http://www.w3.org/2000/09/xmldsig#"><ds:X509Data><ds:X509Certificate>MIIDpjCCAo6gAwIBAgIGAZRGc1KzMA0GCSqGSIb3DQEBCwUAMIGTMQswCQYDVQQGEwJVUzETMBEG
A1UECAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwNU2FuIEZyYW5jaXNjbzENMAsGA1UECgwET2t0YTEU
MBIGA1UECwwLU1NPUHJvdmlkZXIxFDASBgNVBAMMC3NhbXNhcmFyYW1wMRwwGgYJKoZIhvcNAQkB
Fg1pbmZvQG9rdGEuY29tMB4XDTI1MDEwODE1MDQ1MFoXDTM1MDEwODE1MDU1MFowgZMxCzAJBgNV
BAYTAlVTMRMwEQYDVQQIDApDYWxpZm9ybmlhMRYwFAYDVQQHDA1TYW4gRnJhbmNpc2NvMQ0wCwYD
VQQKDARPa3RhMRQwEgYDVQQLDAtTU09Qcm92aWRlcjEUMBIGA1UEAwwLc2Ftc2FyYXJhbXAxHDAa
BgkqhkiG9w0BCQEWDWluZm9Ab2t0YS5jb20wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIB
AQCaEs6cTsVYGtLM4+yK0Ww+pFFcMKj/lm2+1+ZABnQY0vsRHxaldLD9f3S2t/o1c1Y7wq7Waced
/5vwI9rhUmfq/LRxDNKEZECbQw7WIjAreAv/ALufJGUBIXUfQtsZpWF9KioWbHL6WAF1v6GIz1wc
cy1wtwCxa1/RDvzjmjukny3rzOqeQWEkswbW/Wv5sNgpzfW1A0Wfb2XN/HNDXFU75iomv+t/9nM9
4VzZGALc/+uCU4MLCPGnz81myGKRYO8gSVn3hnVGfmDDB5pBkWwWdwWyBcTWKWRjSL1Z1T+cimzo
KF1AbeqJwxlftkHQhdUghkr4ZNkuWVLIRri0cuxHAgMBAAEwDQYJKoZIhvcNAQELBQADggEBAIdP
NYI3vllGorCPkHt+vKxxE/CaJLoVpv04C260YKb10uj3yy0/IsXELXfoEo4yUyWWmloCaSVaskzN
MBwT36LcRkeJRXp7NnHJazsR4Li4WzCgz0krGx4Vk/TNYbAQT07e7UYJ7XDmSajmNFoy47AjZnYZ
8b4U38R7G91MKPwTPsrevggs3hEpsJRbu1L4yM/XwHlIGW6yX9nPP7Alo0SwoOBixUB8B6Vb3eTp
5GYftJ6xhm0C9vNTdlm0Djv6TSv2s/F3jtkOkibzlMuHaA+yedQPBUPRSjyyeBOBVjuLz88ttqqv
r+90es3W4wSTJpGLSE75FSdi0mzEn9MrIiI=</ds:X509Certificate></ds:X509Data></ds:KeyInfo></md:KeyDescriptor><md:NameIDFormat>urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress</md:NameIDFormat><md:NameIDFormat>urn:oasis:names:tc:SAML:2.0:nameid-format:persistent</md:NameIDFormat><md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST" Location="https://samsararamp.samsara.com/app/samsararamp_dagsterconfig_1/exke3hqj89veNCgx54h7/sso/saml"/><md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="https://samsararamp.samsara.com/app/samsararamp_dagsterconfig_1/exke3hqj89veNCgx54h7/sso/saml"/></md:IDPSSODescriptor></md:EntityDescriptor>`,
		OktaSSORedirectBindingURI: "https://samsararamp.samsara.com/app/samsararamp_dagsterconfig_1/exke3hqj89veNCgx54h7/sso/saml",
	}

	resources = append(resources, sso.Resources()...)

	return map[string][]tf.Resource{
		"sso": resources,
	}
}
