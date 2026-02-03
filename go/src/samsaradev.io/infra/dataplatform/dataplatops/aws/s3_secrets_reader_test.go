package aws

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
)

type fakeS3GetObject struct {
	body string
	err  error
}

func (f *fakeS3GetObject) GetObjectWithContext(_ aws.Context, _ *s3.GetObjectInput, _ ...request.Option) (*s3.GetObjectOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &s3.GetObjectOutput{
		Body: io.NopCloser(strings.NewReader(f.body)),
	}, nil
}

func TestS3SecretsReader_Read_NullValueErrors(t *testing.T) {
	r := &s3SecretsReader{
		bucket: "b",
		key:    "k",
		s3:     &fakeS3GetObject{body: `{"db_password": null}`},
	}

	_, err := r.Read(context.Background(), "db_password")
	require.Error(t, err)
	require.Contains(t, err.Error(), "is null")
}

func TestToString_NilDoesNotReturnLiteralNull(t *testing.T) {
	require.Equal(t, "", toString(nil))
}
