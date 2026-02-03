package main

import (
	"log"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/datastreamlake"
)

func main() {
	if err := datastreamlake.GenerateDataStreamLakeSchemaFiles(); err != nil {
		log.Fatalln(oops.Wrapf(err, "failed to generate data stream spark schemas"))
	}

	if err := datastreamlake.GenerateFirehoseStreamWriters(); err != nil {
		log.Fatalln(oops.Wrapf(err, "failed to generate data stream writers"))
	}

	if err := datastreamlake.GenerateDataStreamCopyToDeltaNotebook(); err != nil {
		log.Fatalln(oops.Wrapf(err, "failed to generate data stream writers"))
	}
}
