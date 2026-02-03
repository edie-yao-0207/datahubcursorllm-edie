package main

import (
	"log"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/datapipelines/datapipelineerrors"
)

func main() {
	if err := datapipelineerrors.GenerateDataPipelineErrorDefinitions(); err != nil {
		log.Fatalln(oops.Wrapf(err, "failed to generate data pipeline error definitions"))
	}
}
