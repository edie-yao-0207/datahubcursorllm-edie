package dataplatformresource

import (
	"fmt"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
)

func DNSResolverResources(region string, serviceName string, vpcIdReference string) []tf.Resource {
	serviceRegionName := fmt.Sprintf("%s-%s", serviceName, region)
	s3Bucket := awsregionconsts.RegionPrefix[region] + "databricks-dns-resolver-query-logs"
	path := fmt.Sprintf("%s/vpc-dns-logs/", serviceName)
	s3PathArn := fmt.Sprintf("arn:aws:s3:::%s/%s", s3Bucket, path)

	dnsResolverConfig := awsresource.Route53ResolverQueryLogConfig{
		ResourceName:   fmt.Sprintf("%s-query-log-config", serviceRegionName),
		RecordName:     fmt.Sprintf("%s-query-log-config", serviceRegionName),
		DestinationArn: s3PathArn,
	}
	dnsRuleAssociation := awsresource.Route53ResolverQueryLogConfigAssociation{
		ResourceName:             fmt.Sprintf("%s-query-log-config-assocation", serviceRegionName),
		ResolverQueryLogConfigId: dnsResolverConfig.ResourceId().Reference(),
		ResourceID:               vpcIdReference,
	}
	return []tf.Resource{&dnsResolverConfig, &dnsRuleAssociation}
}
