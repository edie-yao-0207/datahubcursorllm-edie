package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/service/firehose"

	"samsaradev.io/infra/dataplatform/datastreamlake"
	"samsaradev.io/infra/dataplatform/jackteststreamwriters"
	"samsaradev.io/infra/samsaraaws/firehoseiface"
)

func main() {

	ctx := context.Background()

	firehoseClient := firehoseiface.NewFirehose()

	records := []*datastreamlake.TestRecord{
		{
			StringValue: "aria_test_3",
		},
		{
			StringValue: "aria_test_4",
		},
	}
	jackStreamWriter := jackteststreamwriters.NewJackTestStreamWriter(firehoseClient)

	f := &jackteststreamwriters.FirehoseRecords{}
	for _, record := range records {
		err := f.ValidateAndAddFirehoseRecord(record)
		if err != nil {
			fmt.Println(err)
		}
	}

	fireHoseRecords := []*firehose.Record{}
	for _, record := range records {
		rawRecord, err := json.Marshal(record)
		if err != nil {
			fmt.Println(err)
		}
		fireHoseRecords = append(fireHoseRecords, &firehose.Record{
			Data: rawRecord,
		})
	}
	err := jackStreamWriter.WriteRecordBatch(ctx, records)
	if err != nil {
		fmt.Println(err)
	}

}
