package dataplatformmonitors

import (
	"fmt"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/emitters"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/libs/ni/infraconsts"
)

var stream = "dynamodb_deltalake_kinesis_data_streams_firehose"

var dynamodbDeltaLakeMonitors = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource

	// Create monitors only in the US region for now.
	for _, region := range []string{infraconsts.SamsaraAWSDefaultRegion} {

		firehoseS3SuccessMonitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:        fmt.Sprintf("Firehose for %s Kinesis Data Stream in %s is failing to write records to S3", stream, region),
			Message:     fmt.Sprintf("The Firehose for %s Kinesis Data Stream in %s is failing to write records to S3. The impact of this data delay in replication. If it persists for 24hrs there will be data loss. Please go through the runbook attached.", stream, region),
			Query:       fmt.Sprintf("avg(last_1h):avg:aws.firehose.delivery_to_s_3success{deliverystreamname:%s,region:%s} <= 0.90", stream, region),
			Severity:    lowSeverity,
			RunbookLink: "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5777116/RunBook+Firehose+for+Kinesis+Data+Stream+is+failing+to+write+records+to+S3",

			DatadogDashboardLink: &dataPlatformDatadogDashboardLink{
				dashboardLink: dynamodbdeltalakeDashboard,
				variables: map[string]string{
					"region": region,
				},
			},

			DataPlatformDatadogTags: []dataPlatformDatadogTagType{dynamodbDeltaLakeTag},
		})

		if err != nil {
			return nil, oops.Wrapf(err, "error creating monitor for %s in region %s", stream, region)
		}

		monitors = append(monitors, firehoseS3SuccessMonitor)

		firehoseS3FreshnessMonitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:        fmt.Sprintf("Firehose for %s Kinesis Data Stream in %s is seeing data freshness lag", stream, region),
			Message:     fmt.Sprintf("The Firehose for %s Kinesis Data Stream in %s is encountering data freshness lag to S3. The impact of this data delay in replication. If it persists for 24hrs there will be data loss. Please go through the runbook attached.", stream, region),
			Query:       fmt.Sprintf("avg(last_1h):aws.firehose.delivery_to_s_3data_freshness{deliverystreamname:%s,region:%s} >= 3600", stream, region),
			Severity:    lowSeverity,
			RunbookLink: "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5683097/RunBook+Firehose+for+Kinesis+Data+Stream+in+is+seeing+data+freshness+lag",

			DatadogDashboardLink: &dataPlatformDatadogDashboardLink{
				dashboardLink: dynamodbdeltalakeDashboard,
				variables: map[string]string{
					"region": region,
				},
			},

			DataPlatformDatadogTags: []dataPlatformDatadogTagType{dynamodbDeltaLakeTag},
		})

		if err != nil {
			return nil, oops.Wrapf(err, "error creating monitor for %s in region %s", stream, region)
		}

		monitors = append(monitors, firehoseS3FreshnessMonitor)

		kinesisPutRecordsFailureMonitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:        fmt.Sprintf("Kinesis Data Stream %s is seeing PUT records failure in the region %s", stream, region),
			Message:     fmt.Sprintf("The %s Kinesis Data Stream in %s is encountering Put records failure. The impact of this is potential dataloss. Please go through the runbook attached.", stream, region),
			Query:       fmt.Sprintf("avg(last_5m):aws.kinesis.put_records_failed_records{streamname:%s,region:%s} > 0", stream, region),
			Severity:    lowSeverity,
			RunbookLink: "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5720437/RunBook+Kinesis+Data+Stream+is+seeing+PUT+records+failure",

			DatadogDashboardLink: &dataPlatformDatadogDashboardLink{
				dashboardLink: dynamodbdeltalakeDashboard,
				variables: map[string]string{
					"region": region,
				},
			},

			DataPlatformDatadogTags: []dataPlatformDatadogTagType{dynamodbDeltaLakeTag},
		})

		if err != nil {
			return nil, oops.Wrapf(err, "error creating monitor for %s in region %s", stream, region)
		}

		monitors = append(monitors, kinesisPutRecordsFailureMonitor)

		firehoseKinesisRecordReadsZeroMonitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:        fmt.Sprintf("Firehose for kinesis stream %s in %s has read zero records in last 15 minutes", stream, region),
			Message:     fmt.Sprintf("The Firehose for kinesis stream %s in %s is read zero records in last 15 mins. The impact of this is potential dataloss. Please go through the runbook attached.", stream, region),
			Query:       fmt.Sprintf("avg(last_15m):aws.firehose.data_read_from_kinesis_stream_records.sum{streamname:%s,region:%s} <= 0", stream, region),
			Severity:    lowSeverity,
			RunbookLink: "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5683104/RunBook+Firehose+for+kinesis+stream+has+read+zero+records+in+last+N+minutes",

			DatadogDashboardLink: &dataPlatformDatadogDashboardLink{
				dashboardLink: dynamodbdeltalakeDashboard,
				variables: map[string]string{
					"region": region,
				},
			},

			DataPlatformDatadogTags: []dataPlatformDatadogTagType{dynamodbDeltaLakeTag},
		})

		if err != nil {
			return nil, oops.Wrapf(err, "error creating monitor for %s in region %s", stream, region)
		}

		monitors = append(monitors, firehoseKinesisRecordReadsZeroMonitor)

	}

	return monitors, nil
})
