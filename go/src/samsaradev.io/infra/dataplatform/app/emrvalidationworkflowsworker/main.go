package main

import (
	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dataplatform/dataplatformworkflows/emrvalidationworkflow"
	"samsaradev.io/infra/workflows/workflowsworker"
	"samsaradev.io/system"

	// Import to register Databricks client constructor
	_ "samsaradev.io/infra/dataplatform/databricksoauthfx"
)

func main() {
	system.NewFx(
		&config.ConfigParams{},
		workflowsworker.Run(
			workflowsworker.RegisterWorkflow[*emrvalidationworkflow.EmrValidationWorkflow](),
		),
	).Run()
}
