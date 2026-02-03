package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"samsaradev.io/infra/dataplatform/datastreamlake"
	"samsaradev.io/infra/dataplatform/jackteststreamwriters"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/samsaraaws/firehoseiface"
	"samsaradev.io/infra/testloader"
	"samsaradev.io/vendormocks/mock_firehoseiface"
)

func init() {
	// Inject the mock Firehose API client into the container in order to mock out the DataStream calls
	fxregistry.MustRegisterOverrideTestConstructor(fxregistry.Override{
		Constructor:          mock_firehoseiface.NewMockFirehoseAPI,
		InterfaceConstructor: func(mock *mock_firehoseiface.MockFirehoseAPI) firehoseiface.FirehoseAPI { return mock },
		IsDefault:            true,
	})
}

func TestJackStreamWriteRecord(t *testing.T) {
	var env struct {
		DataStreamsClient *mock_firehoseiface.MockFirehoseAPI
	}
	testloader.MustStart(t, &env)

	ctx := context.Background()
	jackStreamWriter := jackteststreamwriters.NewJackTestStreamWriter(env.DataStreamsClient)
	record := &datastreamlake.TestRecord{
		StringValue: "aria_test",
	}

	fireHoseRecords := []*firehose.Record{}
	rawRecord, err := json.Marshal(record)
	assert.NoError(t, err)
	fireHoseRecords = append(fireHoseRecords, &firehose.Record{
		Data: rawRecord,
	})

	env.DataStreamsClient.EXPECT().PutRecordBatchWithContext(ctx, &firehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String("jack_test"),
		Records:            fireHoseRecords,
	}, gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *firehose.PutRecordBatchInput, opts ...request.Option) (*firehose.PutRecordBatchOutput, error) {
			return nil, nil
		},
	).Times(1)

	err = jackStreamWriter.WriteRecord(ctx, record)
	assert.NoError(t, err)
}

func TestJackStreamWriteRecordsBatch(t *testing.T) {
	var env struct {
		DataStreamsClient *mock_firehoseiface.MockFirehoseAPI
	}
	testloader.MustStart(t, &env)

	ctx := context.Background()
	records := []*datastreamlake.TestRecord{
		&datastreamlake.TestRecord{
			StringValue: "aria_test_1",
		},
		&datastreamlake.TestRecord{
			StringValue: "aria_test_2",
		},
	}
	jackStreamWriter := jackteststreamwriters.NewJackTestStreamWriter(env.DataStreamsClient)

	f := &jackteststreamwriters.FirehoseRecords{}
	for _, record := range records {
		err := f.ValidateAndAddFirehoseRecord(record)
		assert.NoError(t, err)
	}

	fireHoseRecords := []*firehose.Record{}
	for _, record := range records {
		rawRecord, err := json.Marshal(record)
		assert.NoError(t, err)
		fireHoseRecords = append(fireHoseRecords, &firehose.Record{
			Data: rawRecord,
		})
	}

	env.DataStreamsClient.EXPECT().PutRecordBatchWithContext(ctx, &firehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String("jack_test"),
		Records:            fireHoseRecords,
	}, gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *firehose.PutRecordBatchInput, opts ...request.Option) (*firehose.PutRecordBatchOutput, error) {
			return nil, nil
		},
	).Times(1)

	err := jackStreamWriter.WriteRecordBatch(ctx, records)
	assert.NoError(t, err)
}

func TestJackStreamValidateRecord(t *testing.T) {
	var env struct {
		DataStreamsClient *mock_firehoseiface.MockFirehoseAPI
	}
	testloader.MustStart(t, &env)

	invalidBytes := make([]byte, 1000000)
	f := &jackteststreamwriters.FirehoseRecords{}
	err := f.ValidateAndAddFirehoseRecord(&datastreamlake.TestRecord{
		StringValue: string(invalidBytes),
	})
	assert.Error(t, err, "Skipping data stream record in batch for stream jack_test due to size limits. Max size is 1000KB.")

	validBytes := make([]byte, 100000)
	err = f.ValidateAndAddFirehoseRecord(&datastreamlake.TestRecord{
		StringValue: string(validBytes),
	})
	assert.NoError(t, err)

}

func TestJackStreamValidateAndWriteRecord(t *testing.T) {
	var env struct {
		DataStreamsClient *mock_firehoseiface.MockFirehoseAPI
	}
	testloader.MustStart(t, &env)

	ctx := context.Background()
	record := &datastreamlake.TestRecord{
		StringValue: "aria_test_1",
	}
	jackStreamWriter := jackteststreamwriters.NewJackTestStreamWriter(env.DataStreamsClient)
	f := &jackteststreamwriters.FirehoseRecords{}
	err := f.ValidateAndAddFirehoseRecord(record)
	assert.NoError(t, err)

	fireHoseRecords := []*firehose.Record{}
	rawRecord, err := json.Marshal(record)
	assert.NoError(t, err)
	fireHoseRecords = append(fireHoseRecords, &firehose.Record{
		Data: rawRecord,
	})

	env.DataStreamsClient.EXPECT().PutRecordBatchWithContext(ctx, &firehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String("jack_test"),
		Records:            fireHoseRecords,
	}, gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *firehose.PutRecordBatchInput, opts ...request.Option) (*firehose.PutRecordBatchOutput, error) {
			return nil, nil
		},
	).Times(1)

	err = jackStreamWriter.WriteValidatedFirehoseRecords(ctx, f)
	assert.NoError(t, err)
}

func TestJackStreamValidateAndWriteRecordsBatch(t *testing.T) {
	var env struct {
		DataStreamsClient *mock_firehoseiface.MockFirehoseAPI
	}
	testloader.MustStart(t, &env)
	ctx := context.Background()

	records := []*datastreamlake.TestRecord{
		&datastreamlake.TestRecord{
			StringValue: "aria_test_1",
		},
		&datastreamlake.TestRecord{
			StringValue: "aria_test_2",
		},
	}

	jackStreamWriter := jackteststreamwriters.NewJackTestStreamWriter(env.DataStreamsClient)
	f := &jackteststreamwriters.FirehoseRecords{}
	for _, record := range records {
		err := f.ValidateAndAddFirehoseRecord(record)
		assert.NoError(t, err)
	}

	fireHoseRecords := []*firehose.Record{}
	for _, record := range records {
		rawRecord, err := json.Marshal(record)
		assert.NoError(t, err)
		fireHoseRecords = append(fireHoseRecords, &firehose.Record{
			Data: rawRecord,
		})
	}

	env.DataStreamsClient.EXPECT().PutRecordBatchWithContext(ctx, &firehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String("jack_test"),
		Records:            fireHoseRecords,
	}, gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *firehose.PutRecordBatchInput, opts ...request.Option) (*firehose.PutRecordBatchOutput, error) {
			return nil, nil
		},
	).Times(1)
	err := jackStreamWriter.WriteValidatedFirehoseRecords(ctx, f)
	assert.NoError(t, err)
}
