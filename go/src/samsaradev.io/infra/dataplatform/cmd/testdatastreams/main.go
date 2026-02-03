package main

import (
	"context"
	"fmt"

	"samsaradev.io/infra/dataplatform/datastreamlake"
	"samsaradev.io/infra/dataplatform/jackteststreamwriters"
	"samsaradev.io/infra/samsaraaws/firehoseiface"
)

func main() {
	fmt.Printf("Hello! Begging to test the stream\n")
	ctx := context.Background()
	firehoseClient := firehoseiface.NewFirehose()
	jackStreamWriter := jackteststreamwriters.NewJackTestStreamWriter(firehoseClient)
	err := jackStreamWriter.WriteRecord(ctx, &datastreamlake.TestRecord{
		StringValue: "aria_test",
	})
	fmt.Println(err)
	if err != nil {
		fmt.Printf("Failed to write to Jack Test Stream, errr: %s\n", err.Error())
	} else {
		fmt.Printf("Succesfully wrote to the Jack Test Stream\n")
	}
}
