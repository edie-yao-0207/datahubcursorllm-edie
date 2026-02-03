package main

import (
	"context"
	"flag"
	"log"

	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/dataplatformhelpers"
	"samsaradev.io/infra/dataplatform/glueversiondeleter"
)

func main() {
	ctx := context.Background()

	var region string
	flag.StringVar(&region, "region", "us-west-2", "region (us-west-2 or eu-west-1), default: us-west-2")
	flag.Parse()

	sess := dataplatformhelpers.GetDatabricksAWSSession(region)
	glueClient := glue.New(sess)

	g := glueversiondeleter.New(glueClient)
	err := g.DeleteOldGlueTableVersions(ctx)
	if err != nil {
		log.Fatal(oops.Wrapf(err, "error deleting old glue table versions"))
	}
}
