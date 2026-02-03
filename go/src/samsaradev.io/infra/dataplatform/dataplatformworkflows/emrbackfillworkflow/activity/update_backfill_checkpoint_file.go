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
	workflowregistry.MustRegisterActivityConstructor(NewUpdateBackfillCheckpointFile)
}

type UpdateBackfillCheckpointFileArgs struct {
	BackfillRequestId string
	EntityName        string
	BackfillStartTime int64
	BackfillEndTime   int64
}

type UpdateBackfillCheckpointFile struct {
	ReplicaAppModels *models.AppModels
	S3Client         s3iface.S3API
}

type UpdateBackfillCheckpointFileParams struct {
	fx.In
	ReplicaAppModels models.ReplicaAppModelsInput
	S3Client         s3iface.S3API
}

type UpdateBackfillCheckpointFileResult struct {
}

func NewUpdateBackfillCheckpointFile(p UpdateBackfillCheckpointFileParams) *UpdateBackfillCheckpointFile {
	return &UpdateBackfillCheckpointFile{
		ReplicaAppModels: p.ReplicaAppModels.Models,
		S3Client:         p.S3Client,
	}
}

func (a UpdateBackfillCheckpointFile) Name() string {
	return "UpdateBackfillCheckpointFile"
}

func (a UpdateBackfillCheckpointFile) Execute(ctx context.Context, args *UpdateBackfillCheckpointFileArgs) (UpdateBackfillCheckpointFileResult, error) {
	// Create metadata JSON
	metadata := map[string]interface{}{
		"completed":           true,
		"backfill_start_time": args.BackfillStartTime,
		"backfill_end_time":   args.BackfillEndTime,
	}
	rawMetadata, err := json.Marshal(metadata)
	if err != nil {
		return UpdateBackfillCheckpointFileResult{}, oops.Wrapf(err, "failed to marshal metadata to JSON")
	}

	// Write metadata to S3
	bucket, err := helpers.GetS3ExportBucketName()
	if err != nil {
		return UpdateBackfillCheckpointFileResult{}, oops.Wrapf(err, "failed to get S3 export bucket name")
	}
	_, err = a.S3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fmt.Sprintf("backfill/data/%s/%s/metadata.json", args.EntityName, args.BackfillRequestId)),
		Body:   bytes.NewReader(rawMetadata),
		ACL:    aws.String(s3.ObjectCannedACLBucketOwnerFullControl),
	})
	if err != nil {
		return UpdateBackfillCheckpointFileResult{}, oops.Wrapf(err, "failed to write metadata to S3")
	}

	return UpdateBackfillCheckpointFileResult{}, nil
}
