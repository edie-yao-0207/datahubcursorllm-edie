package sparkk8s

import (
	"fmt"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/definitions/vpc"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
)

type vpcIds struct {
	vpcId            string
	publicSubnetIds  []string
	privateSubnetIds []string
	securityGroupIds []string
}

func vpcResources(config dataplatformconfig.DatabricksConfig) (map[string][]tf.Resource, vpcIds, error) {
	var resources []tf.Resource

	k8sClusterNetworkTag := &awsresource.Tag{
		Key:   fmt.Sprintf("kubernetes.io/cluster/%s", SparkK8sResourceBaseName),
		Value: "owned",
	}
	sparkK8sVPC, err := vpc.DONOTUSE_DeprecatedDefine(vpc.VPCConfig{
		Environment:          SparkK8sResourceBaseName,
		Region:               config.Region,
		Cidr16:               16,
		CreatePrivateSubnets: true,
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
			{
				Key:   "karpenter.sh/discovery",
				Value: SparkK8sResourceBaseName,
			},
		},
		Tags: getEksTags("k8s-vpc"),
	})
	if err != nil {
		return nil, vpcIds{}, oops.Wrapf(err, "")
	}
	resources = append(resources, sparkK8sVPC.Resources()...)

	// create a default security group that we manage in terraform for enabling traffic between services
	vpcSecurityGroupName := fmt.Sprintf("%s-vpc-internal", SparkK8sResourceBaseName)
	securityGroupTags := getEksTags("vpc-internal-sg")
	securityGroupTags = append(securityGroupTags, &awsresource.Tag{
		Key:   "karpenter.sh/discovery",
		Value: SparkK8sResourceBaseName,
	})

	vpcSecurityGroup := &awsresource.SecurityGroup{
		Name:        vpcSecurityGroupName,
		Description: "allow all traffic between services within VPC",
		VpcID:       sparkK8sVPC.VPC.ResourceId().Reference(),
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
		Tags: securityGroupTags,
	}
	resources = append(resources, vpcSecurityGroup)

	vpcIds := vpcIds{
		vpcId: sparkK8sVPC.VPC.ResourceId().Reference(),
		publicSubnetIds: []string{
			fmt.Sprintf("${aws_subnet.%s-%sa-subnet-public.id}", SparkK8sResourceBaseName, config.Region),
			fmt.Sprintf("${aws_subnet.%s-%sb-subnet-public.id}", SparkK8sResourceBaseName, config.Region),
			fmt.Sprintf("${aws_subnet.%s-%sc-subnet-public.id}", SparkK8sResourceBaseName, config.Region),
		},
		privateSubnetIds: []string{
			fmt.Sprintf("${aws_subnet.%s-%sa-subnet-private.id}", SparkK8sResourceBaseName, config.Region),
			fmt.Sprintf("${aws_subnet.%s-%sb-subnet-private.id}", SparkK8sResourceBaseName, config.Region),
			fmt.Sprintf("${aws_subnet.%s-%sc-subnet-private.id}", SparkK8sResourceBaseName, config.Region),
		},
		securityGroupIds: []string{
			vpcSecurityGroup.ResourceId().Reference(),
		},
	}

	return map[string][]tf.Resource{
		"vpc": resources,
	}, vpcIds, nil
}
