package metadatagenerator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/dataplatform/datastreamlake"
	"samsaradev.io/infra/dataplatform/ksdeltalake"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/samsaraaws/s3iface"
)

type GithubSourceGenerator struct {
	s3Client s3iface.S3API
}

func newGithubSourceGenerator(s3Client s3iface.S3API) (*GithubSourceGenerator, error) {
	return &GithubSourceGenerator{
		s3Client: s3Client,
	}, nil
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(newGithubSourceGenerator)
}

func (a *GithubSourceGenerator) Name() string {
	return "amundsen-github-source-generator"
}

func (a *GithubSourceGenerator) GenerateMetadata(metadata *Metadata) error {
	ctx := context.Background()

	// For now, we are just producing a simple map of table name -> github link.
	// Amundsen may support linking out to other source types (or may be amenable to us extending it), so we
	// should consider making this more general in case we decide we want to link out to more things.
	githubSources := make(map[string]string)

	// Data Pipelines
	for _, pipelineNode := range metadata.DataPipelineNodes {
		parts := strings.SplitN(pipelineNode.Name, ".", 2)
		if len(parts) != 2 {
			slog.Warnw(ctx, "Couldn't figure out github source for node. Skipping....\n", "node", pipelineNode)
			continue
		}
		githubSources[pipelineNode.Name] = fmt.Sprintf("https://github.com/samsara-dev/backend/blob/master/dataplatform/tables/transformations/%s/%s.sql", parts[0], parts[1])
	}

	// SQL Views
	for viewName, viewMetadata := range metadata.SQLViews {
		githubSources[viewName] = fmt.Sprintf("https://github.com/samsara-dev/backend/blob/master/dataplatform/tables/sqlview/%s/%s.sql", viewMetadata.Database, viewMetadata.View)
	}

	// KinesisStats: Link to a github search over the samsara firmware and backend repor acrross all go code.
	for _, stat := range ksdeltalake.AllTables() {
		kinesisGithubLink := fmt.Sprintf("https://github.com/search?q=org%%3Asamsara-dev+ObjectStatEnum_%s+repo%%3Asamsara-dev%%2Ffirmware+repo%%3Asamsara-dev%%2Fbackend+language%%3AGo&type=Code&ref=advsearch&l=Go&l=", stat.Name)
		githubSources[strings.ToLower(stat.QualifiedName())] = kinesisGithubLink
	}

	// Data Streams
	for _, stream := range datastreamlake.Registry {
		recordObjectType := reflect.TypeOf(stream.Record).Name()
		githubSources[fmt.Sprintf("datastreams.%s", stream.StreamName)] = fmt.Sprintf("https://github.com/samsara-dev/backend/search?l=Go&q=%s", recordObjectType)
		githubSources[fmt.Sprintf("datastreams_schema.%s", stream.StreamName)] = fmt.Sprintf("https://github.com/samsara-dev/backend/search?l=Go&q=%s", recordObjectType)
		githubSources[fmt.Sprintf("datastreams_history.%s", stream.StreamName)] = fmt.Sprintf("https://github.com/samsara-dev/backend/search?l=Go&q=%s", recordObjectType)
	}

	// Big Stats
	for _, stat := range ksdeltalake.AllS3BigStatTables() {
		bigStatGithubLink := fmt.Sprintf("https://github.com/samsara-dev/firmware/search?q=ObjectStatS3BinaryMessage+%s", stat.S3BinaryMessageField)
		githubSources[strings.ToLower(stat.S3BigStatsName())] = bigStatGithubLink
		githubSources[fmt.Sprintf("kinesisstats.%s_with_s3_big_stat", strings.ToLower(stat.Name))] = bigStatGithubLink
	}

	rawSources, err := json.Marshal(githubSources)
	if err != nil {
		return oops.Wrapf(err, "error marshaling sources map to json")
	}

	_, err = a.s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String("samsara-amundsen-metadata"),
		Key:    aws.String("delta/table_sources.json"),
		Body:   bytes.NewReader(rawSources),
		ACL:    aws.String(s3.ObjectCannedACLBucketOwnerFullControl),
	})
	if err != nil {
		return oops.Wrapf(err, "error uploading owners file to S3")
	}

	return nil
}

var _ MetadataGenerator = &GithubSourceGenerator{}
