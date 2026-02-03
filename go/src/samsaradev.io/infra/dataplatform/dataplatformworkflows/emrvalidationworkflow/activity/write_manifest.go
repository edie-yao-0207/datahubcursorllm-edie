package activity

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/samsarahq/go/oops"
	"go.uber.org/fx"

	"samsaradev.io/infra/workflows/workflowregistry"
)

func init() {
	workflowregistry.MustRegisterActivityConstructor(NewWriteManifestActivity)
}

type ValidationRunMetadata struct {
	WorkflowId     string    `json:"workflowId"`
	Timestamp      time.Time `json:"timestamp"`
	Cell           string    `json:"cell"`
	EntityName     string    `json:"entityName"`
	ValidationDate time.Time `json:"validationDate"`
}

type ManifestContent struct {
	// Metadata about the validation run
	ValidationRun ValidationRunMetadata `json:"validationRun"`

	// List of exported files
	ExportedFiles []ExportedFile `json:"exportedFiles"`

	// Selected orgs and their assets for validation
	// This is the key information needed by downstream validation jobs
	OrgAssetSelections []OrgAssetSelection `json:"orgAssetSelections"`
}

type WriteManifestActivityArgs struct {
	ManifestPath  string
	S3Region      string
	EntityName    string
	ExportedFiles []ExportedFile
	SelectedOrgs  []OrgAssetSelection
	ValidationRun ValidationRunMetadata
}

type WriteManifestActivityResult struct {
	ManifestS3Path string `json:"manifestS3Path"`
}

type WriteManifestActivity struct {
	// AWS session will be created per activity execution
}

type WriteManifestActivityParams struct {
	fx.In
	// No additional dependencies needed for S3 operations
}

func NewWriteManifestActivity(p WriteManifestActivityParams) *WriteManifestActivity {
	return &WriteManifestActivity{}
}

func (a WriteManifestActivity) Name() string {
	return "WriteManifestActivity"
}

func (a WriteManifestActivity) Execute(ctx context.Context, args *WriteManifestActivityArgs) (WriteManifestActivityResult, error) {
	// Create AWS session using default credential chain
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(args.S3Region),
	})

	// TODO: Remove once we've tested the workflow's functionality in production environment.
	// Create AWS session with playground profile for write access
	// sess, err := session.NewSessionWithOptions(session.Options{
	// 	SharedConfigState: session.SharedConfigEnable,
	// 	Profile:           "sso-playground", // Use playground profile for aws-eng-playground-global account
	// 	Config: aws.Config{
	// 		Region: aws.String(args.S3Region),
	// 	},
	// })
	if err != nil {
		return WriteManifestActivityResult{}, oops.Wrapf(err, "failed to create AWS session")
	}

	s3Client := s3.New(sess)

	// Parse S3 path
	bucket, key, err := parseS3Path(args.ManifestPath)
	if err != nil {
		return WriteManifestActivityResult{}, oops.Wrapf(err, "failed to parse manifest S3 path: %s", args.ManifestPath)
	}

	// Create manifest content
	manifest := ManifestContent{
		ValidationRun:      args.ValidationRun,
		ExportedFiles:      args.ExportedFiles,
		OrgAssetSelections: args.SelectedOrgs,
	}

	// Convert to JSON
	manifestJson, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return WriteManifestActivityResult{}, oops.Wrapf(err, "failed to marshal manifest to JSON")
	}

	// Upload manifest to S3
	_, err = s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        strings.NewReader(string(manifestJson)),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return WriteManifestActivityResult{}, oops.Wrapf(err, "failed to upload manifest to S3")
	}

	return WriteManifestActivityResult{
		ManifestS3Path: args.ManifestPath,
	}, nil
}

// parseS3Path parses an S3 path into bucket and key components
func parseS3Path(s3Path string) (bucket, key string, err error) {
	if !strings.HasPrefix(s3Path, "s3://") {
		return "", "", oops.Errorf("invalid S3 path format: %s", s3Path)
	}

	path := strings.TrimPrefix(s3Path, "s3://")
	parts := strings.SplitN(path, "/", 2)

	bucket = parts[0]
	if bucket == "" {
		return "", "", oops.Errorf("invalid S3 path format: bucket name cannot be empty: %s", s3Path)
	}

	if len(parts) > 1 {
		key = parts[1]
	}

	return bucket, key, nil
}
