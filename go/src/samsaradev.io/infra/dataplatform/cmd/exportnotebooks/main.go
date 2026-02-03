// `exportnotebooks` exports all databricks notebooks from a workspace. This
// tool pulls one notebook at a time, which avoids hitting directory export size
// limit. This is useful when we need to search through every notebook, e.g. to
// check if any references a table we want to deprecate.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"path/filepath"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/app/exportnotebooks"
	"samsaradev.io/infra/dataplatform/dataplatformhelpers"
)

func main() {
	ctx := context.Background()

	var region string
	var output string
	flag.StringVar(&region, "region", "us-west-2", "region (us-west-2 or eu-west-1), default: us-west-2")
	flag.StringVar(&output, "output", "", "output directory")
	flag.Parse()

	if output == "" {
		log.Fatalln("must specify -output")
	}

	output, err := filepath.Abs(output)
	if err != nil {
		log.Fatalln(oops.Wrapf(err, "abs: %s", output))
	}
	if err = os.MkdirAll(output, 0755); err != nil {
		log.Fatalln(oops.Wrapf(err, "mkdirs: %s", output))
	}

	client, err := dataplatformhelpers.GetDatabricksE2Client(region)
	if err != nil {
		log.Fatalln(err)
	}

	err = exportnotebooks.Export(ctx, client, output)
	if err != nil {
		log.Fatalln(oops.Wrapf(err, "error exporting notebooks"))
	}
}
