package datapipelineapi

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/libs/ni/infraconsts"
)

func (c *Client) DeleteNodeData(ctx context.Context, nodeId string) error {
	bucket := "samsara-data-pipelines-delta-lake"
	if c.Region == infraconsts.SamsaraAWSEURegion {
		bucket = "samsara-eu-data-pipelines-delta-lake"
	} else if c.Region == infraconsts.SamsaraAWSCARegion {
		bucket = "samsara-ca-data-pipelines-delta-lake"
	}

	splitName := strings.Split(nodeId, ".")
	database := splitName[0]
	table := splitName[1]

	var deleteErr error
	err := c.S3.ListObjectsV2PagesWithContext(ctx,
		&s3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
			Prefix: aws.String(fmt.Sprintf("%s/%s/", database, table)),
		},
		func(output *s3.ListObjectsV2Output, b bool) bool {
			objectIdentifiers := make([]*s3.ObjectIdentifier, len(output.Contents))
			for idx, file := range output.Contents {
				objectIdentifiers[idx] = &s3.ObjectIdentifier{
					Key: file.Key,
				}
			}

			_, err := c.S3.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
				Bucket: aws.String(bucket),
				Delete: &s3.Delete{
					Objects: objectIdentifiers,
				},
			})
			if err != nil {
				deleteErr = oops.Wrapf(err, "error deleting object identifiers: %v", objectIdentifiers)
				return false
			}

			return true
		},
	)
	if err != nil {
		if deleteErr != nil {
			return oops.Wrapf(deleteErr, "error deleting node data for %s", nodeId)
		}
		return oops.Wrapf(err, "error listing objects. %s/%s/%s/", bucket, database, table)
	}

	return nil
}
