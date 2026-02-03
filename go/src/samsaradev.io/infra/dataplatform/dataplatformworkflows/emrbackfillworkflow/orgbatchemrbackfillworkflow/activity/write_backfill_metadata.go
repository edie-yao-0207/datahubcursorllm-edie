package activity

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/samsarahq/go/oops"
	"go.uber.org/fx"

	"samsaradev.io/infra/dataplatform/dataplatformworkflows/emrbackfillworkflow/helpers"
	"samsaradev.io/infra/samsaraaws/s3iface"
	"samsaradev.io/infra/workflows/workflowregistry"
	"samsaradev.io/models"
)

func init() {
	workflowregistry.MustRegisterActivityConstructor(NewWriteBackfillMetadataActivity)
}

type WriteBackfillMetadataActivityArgs struct {
	BackfillRequestId string
	EntityName        string
	FailedOrgIds      []int64
}

type WriteBackfillMetadataActivity struct {
	ReplicaAppModels *models.AppModels
	S3Client         s3iface.S3API
}

type WriteBackfillMetadataActivityParams struct {
	fx.In
	ReplicaAppModels models.ReplicaAppModelsInput
	S3Client         s3iface.S3API
}

type WriteBackfillMetadataActivityResult struct {
}

func NewWriteBackfillMetadataActivity(p WriteBackfillMetadataActivityParams) *WriteBackfillMetadataActivity {
	return &WriteBackfillMetadataActivity{
		ReplicaAppModels: p.ReplicaAppModels.Models,
		S3Client:         p.S3Client,
	}
}

func (a WriteBackfillMetadataActivity) Name() string {
	return "WriteBackfillMetadataActivity"
}

func (a WriteBackfillMetadataActivity) Execute(ctx context.Context, args *WriteBackfillMetadataActivityArgs) (WriteBackfillMetadataActivityResult, error) {
	metadata := map[string]interface{}{
		"failed_org_ids": args.FailedOrgIds,
	}
	rawMetadata, err := json.Marshal(metadata)
	if err != nil {
		return WriteBackfillMetadataActivityResult{}, oops.Wrapf(err, "failed to marshal metadata to JSON")
	}

	// Write metrics to S3.
	bucket, err := helpers.GetS3ExportBucketName()
	if err != nil {
		return WriteBackfillMetadataActivityResult{}, oops.Wrapf(err, "failed to get S3 export bucket name")
	}
	_, err = a.S3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fmt.Sprintf("backfill/data/%s/%s/metrics.json", args.EntityName, args.BackfillRequestId)),
		Body:   bytes.NewReader(rawMetadata),
		ACL:    aws.String(s3.ObjectCannedACLBucketOwnerFullControl),
	})
	if err != nil {
		return WriteBackfillMetadataActivityResult{}, oops.Wrapf(err, "failed to write metrics to S3")
	}

	return WriteBackfillMetadataActivityResult{}, nil
}
