package main

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"go.uber.org/fx"

	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dataplatform/dataplatformworkflows/emrbackfillworkflow"
	"samsaradev.io/infra/dataplatform/dataplatformworkflows/emrbackfillworkflow/emrtokinesis"
	"samsaradev.io/infra/dataplatform/dataplatformworkflows/emrbackfillworkflow/orgbatchemrbackfillworkflow"
	"samsaradev.io/infra/workflows/workflowsworker"
	"samsaradev.io/system"
)

func provideKinesisClient() kinesisiface.KinesisAPI {
	return kinesis.New(session.New())
}

func main() {
	system.NewFx(
		&config.ConfigParams{},
		fx.Provide(provideKinesisClient),
		workflowsworker.Run(
			workflowsworker.RegisterWorkflow[*emrbackfillworkflow.EmrBackfillWorkflow](),
			workflowsworker.RegisterWorkflow[*emrtokinesis.EmrToKinesisWorkflow](),
			workflowsworker.RegisterWorkflow[*orgbatchemrbackfillworkflow.OrgBatchEmrBackfillWorkflow](),
		),
	).Run()
}
