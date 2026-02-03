package main

import (
	"log"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/datapipelines"
)

func main() {
	if err := datapipelines.GenerateDataPipelineOwnershipFile(); err != nil {
		log.Fatalln(oops.Wrapf(err, "failed to generate datapipelines ownership file"))
	}
}
