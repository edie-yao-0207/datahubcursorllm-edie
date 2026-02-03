package datahubprojects

import (
	"fmt"
	"strings"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformprojecthelpers"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/team"
)

var DatahubEKSClusterOIDCRegionProvider = map[string]string{
	infraconsts.SamsaraAWSDefaultRegion: fmt.Sprintf("oidc.eks.%s.amazonaws.com/id/F89DF96197BE9D911A280AD16D82FEF7", infraconsts.SamsaraAWSDefaultRegion),
}

// generates a inbound security group for the ALB that accepts connections from VPN IPs over HTTPS.
func generateAlbSecurityGroup(vpcId string) *awsresource.SecurityGroup {
	securityGroupName := fmt.Sprintf("%s-%s", DatahubResourceBaseName, "vpn-ingress")
	officeHttps := &awsresource.SecurityGroup{
		Name:        securityGroupName,
		Description: "Allows port 443 ingress from office and VPN IPs for Datahub ALB.",
		VpcID:       vpcId,
		IngressBlocks: []*awsresource.IngressEgressBlock{
			&awsresource.IngressEgressBlock{
				Protocol:   "tcp",
				FromPort:   443,
				ToPort:     443,
				CidrBlocks: append(infraconsts.OfficeCidrs, infraconsts.DevHomeCidrs...),
			},
		},
		EgressBlocks: []*awsresource.IngressEgressBlock{
			&awsresource.IngressEgressBlock{
				Protocol:   "-1",
				FromPort:   0,
				ToPort:     0,
				CidrBlocks: []string{"0.0.0.0/0"},
			},
		},
		BaseResource: tf.BaseResource{
			MetaParameters: tf.MetaParameters{
				Lifecycle: tf.Lifecycle{
					CreateBeforeDestroy: true,
				},
			},
		},
	}
	return officeHttps
}

func generateDomainCertificate(config dataplatformconfig.DatabricksConfig) *awsresource.ACMCertificate {

	var subdomain string
	switch config.Region {
	case infraconsts.SamsaraAWSDefaultRegion:
		subdomain = "internal"
	case infraconsts.SamsaraAWSEURegion:
		subdomain = "internal.eu"
	case infraconsts.SamsaraAWSCARegion:
		subdomain = "internal.ca"
	}

	tags := awsresource.ConvertAWSTagsToTagMap(getDatahubTags("acm"))
	certificate := &awsresource.ACMCertificate{
		ResourceName:     fmt.Sprintf("%s-%s", DatahubResourceBaseName, "internal-dns"),
		DomainName:       fmt.Sprintf("datahub.%s.samsara.com", subdomain),
		ValidationMethod: "DNS",
		Tags:             tags,
	}
	return certificate
}

// Datahub's AWS load balancers are managed by AwsLoadBalancerController in the EKS cluster. However, some resources need to be bootstrapped
// to configure the controller during installation. In addition to the following resources, an IAM identity provider (oidc) was
// created in the console because we currently do not have a golang struct to manage it in terraform. The AWSLoadBalancerController was installed
// on the EKS cluster via helm. (see https://kubernetes-sigs.github.io/aws-load-balancer-controller/v2.5/)
func loadBalancerResources(config dataplatformconfig.DatabricksConfig, v vpcIds) (map[string][]tf.Resource, error) {

	accountId := awsregionconsts.RegionDatabricksAccountID[config.Region]
	oidcProvider := DatahubEKSClusterOIDCRegionProvider[config.Region]

	securityGroup := generateAlbSecurityGroup(v.vpcId)
	certificate := generateDomainCertificate(config)

	loadBalancerControllerRole := awsresource.IAMRole{
		Name:        fmt.Sprintf("%s-%s", DatahubResourceBaseName, "eks-load-balancer-controller-role"),
		Description: "Role used by load balancers for Data Platform's Datahub eks cluster",
		AssumeRolePolicy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Principal: map[string]string{
						"Federated": fmt.Sprintf("arn:aws:iam::%d:oidc-provider/%s", accountId, oidcProvider),
					},
					Action: []string{"sts:AssumeRoleWithWebIdentity"},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{
							fmt.Sprintf("%s:aud", oidcProvider): "sts.amazonaws.com",
							fmt.Sprintf("%s:sub", oidcProvider): "system:serviceaccount:kube-system:datahub-eks-load-balancer-controller-role",
						},
					},
				},
			},
		},
		Tags: map[string]string{
			"samsara:service":        "datahub",
			"samsara:team":           strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group":  strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
			"samsara:rnd-allocation": "1",
		},
	}

	loadBalancerControllerPolicyAttachment := awsresource.IAMRolePolicy{
		Name:   fmt.Sprintf("%s-%s", DatahubResourceBaseName, "eks-load-balancer-controller-policy"),
		Role:   loadBalancerControllerRole.ResourceId(),
		Policy: dataplatformprojecthelpers.GenerateAWSLoadBalancerControllerPolicy(),
	}

	return map[string][]tf.Resource{
		"load_balancer": {
			certificate,
			securityGroup,
			&loadBalancerControllerRole,
			&loadBalancerControllerPolicyAttachment,
		},
	}, nil
}
