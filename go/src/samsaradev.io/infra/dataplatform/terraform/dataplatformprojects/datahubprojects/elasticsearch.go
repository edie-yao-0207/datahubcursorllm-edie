package datahubprojects

import (
	"fmt"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/libs/ni/policy"
)

const datahubESClusterName = "datahub"

func elasticsearchResources(vpcIds vpcIds) (map[string][]tf.Resource, error) {
	cloudwatchAuditLogsGroup := awsresource.CloudwatchLogGroup{
		ResourceName:    "datahub_elasticsearch_audit_logs",
		Name:            "/datahub/elasticsearch/audit_logs",
		RetentionInDays: 90,
		Tags:            awsresource.ConvertAWSTagsToTagMap(getDatahubTags("elasticsearch")),
	}

	cloudwatchESApplicationLogsGroup := awsresource.CloudwatchLogGroup{
		ResourceName:    "datahub_elasticsearch_es_application_logs",
		Name:            "/datahub/elasticsearch/es_application_logs",
		RetentionInDays: 90,
		Tags:            awsresource.ConvertAWSTagsToTagMap(getDatahubTags("elasticsearch")),
	}

	cloudwatchSearchSlowLogsGroup := awsresource.CloudwatchLogGroup{
		ResourceName:    "datahub_elasticsearch_search_slow_logs",
		Name:            "/datahub/elasticsearch/search_slow_logs",
		RetentionInDays: 90,
		Tags:            awsresource.ConvertAWSTagsToTagMap(getDatahubTags("elasticsearch")),
	}

	cloudwatchIndexSlowLogsGroup := awsresource.CloudwatchLogGroup{
		ResourceName:    "datahub_elasticsearch_index_slow_logs",
		Name:            "/datahub/elasticsearch/index_slow_logs",
		RetentionInDays: 90,
		Tags:            awsresource.ConvertAWSTagsToTagMap(getDatahubTags("elasticsearch")),
	}

	elasticsearchDomain := awsresource.ElasticsearchDomain{
		DomainName: datahubESClusterName,
		AccessPolicies: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Principal: map[string]string{
						"AWS": "arn:aws:iam::492164655156:role/datahub-eks",
					},
					Action:   []string{"es:*"},
					Resource: []string{fmt.Sprintf("arn:aws:es:us-west-2:492164655156:domain/%s/*", datahubESClusterName)},
				},
				{
					Effect: "Allow",
					Principal: map[string]string{
						"AWS": "arn:aws:iam::492164655156:role/datahub-eks-nodes",
					},
					Action:   []string{"es:*"},
					Resource: []string{fmt.Sprintf("arn:aws:es:us-west-2:492164655156:domain/%s/*", datahubESClusterName)},
				},
				{
					Effect: "Allow",
					Principal: map[string]string{
						"AWS": "arn:aws:iam::492164655156:role/datahub-eks-fargate-pods",
					},
					Action:   []string{"es:*"},
					Resource: []string{fmt.Sprintf("arn:aws:es:us-west-2:492164655156:domain/%s/*", datahubESClusterName)},
				},
			},
		},
		ElasticsearchVersion: "7.10",
		ClusterConfig: awsresource.ElasticsearchDomainClusterConfig{
			InstanceCount:        3,
			InstanceType:         "i3.2xlarge.elasticsearch",
			ZoneAwarenessEnabled: true,
			ZoneAwarenessConfig: awsresource.ElasticsearchDomainZoneAwarenessConfig{
				AvailabilityZoneCount: 3,
			},
		},
		EncryptAtRest: awsresource.ElasticsearchDomainEncryptAtRest{
			Enabled: true,
		},
		NodeToNodeEncryption: awsresource.ElasticsearchDomainNodeToNodeEncryption{
			Enabled: true,
		},
		VpcOptions: awsresource.ElasticsearchDomainVpcOptions{
			SecurityGroupIds: vpcIds.securityGroupIds,
			SubnetIds:        vpcIds.privateSubnetIds,
		},
		Tags: awsresource.ConvertAWSTagsToTagMap(getDatahubTags("elasticsearch")),
	}

	return map[string][]tf.Resource{
		"elasticsearch": {
			&cloudwatchAuditLogsGroup,
			&cloudwatchESApplicationLogsGroup,
			&cloudwatchSearchSlowLogsGroup,
			&cloudwatchIndexSlowLogsGroup,
			&elasticsearchDomain,
		},
	}, nil
}
