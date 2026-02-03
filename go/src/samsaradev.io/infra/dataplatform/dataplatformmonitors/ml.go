package dataplatformmonitors

import (
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/emitters"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/team"
)

var langfuseK8sCronExportMonitor = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
		Query:                   "sum(last_1h):default_zero(sum:kubernetes_state.job.succeeded{kube_cluster_name:aiml-shared-*,kube_cronjob:*clickhouse-export} by {kube_cronjob}.as_count()) < 1",
		Name:                    "No K8s cron export runs for Langfuse data have succeeded in the last hour on job {kube_cronjob}",
		Message:                 "We have not seen any successful runs of the K8s cron job to export Langfuse data to Clickhouse in the last hour.",
		DataPlatformDatadogTags: []dataPlatformDatadogTagType{mlInfraTag},

		RunbookLink: "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5039452",
		TeamToPage:  team.MLCoreInfra,
		Severity:    lowSeverity,
	})

	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return []tf.Resource{monitor}, nil
})
