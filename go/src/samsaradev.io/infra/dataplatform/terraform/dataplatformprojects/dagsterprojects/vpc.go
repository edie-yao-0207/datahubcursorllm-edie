package dagsterprojects

import (
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/definitions/vpc"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
)

type vpcIds struct {
	vpcId            string
	publicSubnetIds  []string
	privateSubnetIds []string
	securityGroupIds []string
}

func networkInterfaceResources(v vpcIds) []tf.Resource {
	var enis []tf.Resource
	for i, subnetId := range v.privateSubnetIds {
		name := strings.Split(subnetId, ".")[1]
		eni := awsresource.NetworkInterface{
			SubnetId:       subnetId,
			InterfaceType:  "efa",
			SecurityGroups: v.securityGroupIds,
			Tags:           getDagsterTags("eni"),
		}.WithContext(
			tf.Context{
				Environment: name,
				Detail:      fmt.Sprintf("%d", i)},
		)
		enis = append(enis, eni)
	}

	return enis
}

func vpcResources(config dataplatformconfig.DatabricksConfig) (map[string][]tf.Resource, vpcIds, error) {
	var resources []tf.Resource

	k8sClusterNetworkTag := &awsresource.Tag{
		Key:   fmt.Sprintf("kubernetes.io/cluster/%s", DagsterResourceBaseName),
		Value: "owned",
	}
	dagsterK8sVPC, err := vpc.DONOTUSE_DeprecatedDefine(vpc.VPCConfig{
		Environment:          DagsterResourceBaseName,
		Region:               config.Region,
		Cidr16:               16,
		CreatePrivateSubnets: true,
		CreateDBSubnetGroup:  true,
		PublicSubnetTags: []*awsresource.Tag{
			{
				Key:   "kubernetes.io/role/elb",
				Value: "1",
			},
			k8sClusterNetworkTag,
		},
		PrivateSubnetTags: []*awsresource.Tag{
			{
				Key:   "kubernetes.io/role/internal-elb",
				Value: "1",
			},
			k8sClusterNetworkTag,
		},
		Tags: getDagsterTags("k8s-vpc"),
	})
	if err != nil {
		return nil, vpcIds{}, oops.Wrapf(err, "")
	}
	dagsterK8sVPC.VPC.BaseResource.MetaParameters.Lifecycle.PreventDestroy = true
	resources = append(resources, dagsterK8sVPC.Resources()...)

	// create a default security group that we manage in terraform for enabling traffic between services
	vpcSecurityGroupName := fmt.Sprintf("%s-vpc-internal", DagsterResourceBaseName)
	vpcSecurityGroup := &awsresource.SecurityGroup{
		Name:        vpcSecurityGroupName,
		Description: "allow all traffic between services within VPC",
		VpcID:       dagsterK8sVPC.VPC.ResourceId().Reference(),
		IngressBlocks: []*awsresource.IngressEgressBlock{
			{
				Protocol: "-1",
				FromPort: 0,
				ToPort:   0,
				Self:     true,
			},
		},
		EgressBlocks: []*awsresource.IngressEgressBlock{
			{
				Protocol:   "-1",
				FromPort:   0,
				ToPort:     0,
				CidrBlocks: []string{"0.0.0.0/0"},
			},
		},
		Tags: getDagsterTags("vpc-internal-sg"),
	}
	resources = append(resources, vpcSecurityGroup)

	vpcIds := vpcIds{
		vpcId: dagsterK8sVPC.VPC.ResourceId().Reference(),
		publicSubnetIds: []string{
			fmt.Sprintf("${aws_subnet.%s-%sa-subnet-public.id}", DagsterResourceBaseName, config.Region),
			fmt.Sprintf("${aws_subnet.%s-%sb-subnet-public.id}", DagsterResourceBaseName, config.Region),
			fmt.Sprintf("${aws_subnet.%s-%sc-subnet-public.id}", DagsterResourceBaseName, config.Region),
		},
		privateSubnetIds: []string{
			fmt.Sprintf("${aws_subnet.%s-%sa-subnet-private.id}", DagsterResourceBaseName, config.Region),
			fmt.Sprintf("${aws_subnet.%s-%sb-subnet-private.id}", DagsterResourceBaseName, config.Region),
			fmt.Sprintf("${aws_subnet.%s-%sc-subnet-private.id}", DagsterResourceBaseName, config.Region),
		},
		securityGroupIds: []string{
			vpcSecurityGroup.ResourceId().Reference(),
		},
	}

	enis := networkInterfaceResources(vpcIds)
	if enis != nil {
		resources = append(resources, enis...)
	}

	vpcFlowResource := dataplatformresource.VpcFlowLogsResource(
		fmt.Sprintf("vpc-flow-logs-dagster-eks-%s", config.Region),
		dagsterK8sVPC.VPC.ResourceId().Reference(),
		config.Region,
	)

	resources = append(resources, vpcFlowResource...)

	dnsResolverResources := dataplatformresource.DNSResolverResources(config.Region, "dagster", dagsterK8sVPC.VPC.ResourceId().Reference())
	resources = append(resources, dnsResolverResources...)

	// Create a Route53 Resolver DNSSEC config
	dnssecConfig := &awsresource.Route53ResolverDnssecConfig{
		Name:  fmt.Sprintf("%s-dnssec-resolver", DagsterResourceBaseName),
		VpcID: dagsterK8sVPC.VPC.ResourceId().Reference(),
	}
	resources = append(resources, dnssecConfig)

	return map[string][]tf.Resource{
		"vpc": resources,
	}, vpcIds, nil
}
