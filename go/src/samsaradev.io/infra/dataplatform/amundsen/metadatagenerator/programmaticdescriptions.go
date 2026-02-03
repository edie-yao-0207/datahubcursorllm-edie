package metadatagenerator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dataplatform/datapipelines"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/samsaraaws/s3iface"
	"samsaradev.io/libs/ni/infraconsts"
)

type ProgrammaticDescriptionsGenerator struct {
	s3Client s3iface.S3API
}

func newProgrammaticDescriptionGenerator(s3Client s3iface.S3API, appConfig *config.AppConfig) (*ProgrammaticDescriptionsGenerator, error) {
	return &ProgrammaticDescriptionsGenerator{
		s3Client: s3Client,
	}, nil
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(newProgrammaticDescriptionGenerator)
}

func (p *ProgrammaticDescriptionsGenerator) Name() string {
	return "amundsen-programmatic-descriptions-generator"
}

// ProgrammaticDescriptions in Amundsen get rendered in Markdown. In order to keep it organized, we populate this struct
// which has a header and content. Header being the H2 for the content and then content being markdown that can contain anything.
// In the future we can support different types of descriptionEntries but during this testing/rollout phase of this
// new type of metadata we only support this.
//
// # A single table can have  many programmatic descriptions hence the slice type programmaticDescription with a list of descriptionEntry
//
// See this article and the Programmatic Descriptions sectio for more information:
// https://technology.edmunds.com/2020/05/27/Adding-Data-Quality-into-Amundsen-with-Programmatic-Descriptions/
// And the Amundsen example of Programmatic Descriptions here:
// https://github.com/amundsen-io/amundsen/blob/main/databuilder/example/sample_data/sample_table_programmatic_source.csv
type descriptionEntry struct {
	header  string
	content string
}

type ProgrammaticDescription []descriptionEntry

func (p ProgrammaticDescription) MarshalJSON() ([]byte, error) {
	result := make([]map[string]string, 0, len(p))
	for _, entry := range p {
		// ProgrammaticDescriptions contain a source and a description
		// https://github.com/amundsen-io/amundsen/blob/main/databuilder/databuilder/models/table_metadata.py#L176
		// We separate these out in the JSON for smooth Python parsing on the other end
		result = append(result, map[string]string{
			"description_source": entry.header,
			"description":        entry.content,
		})
	}

	return json.Marshal(result)
}

func (p *ProgrammaticDescriptionsGenerator) GenerateMetadata(metadata *Metadata) error {
	ctx := context.Background()

	programmaticDescriptions := make(map[string]ProgrammaticDescription)

	// Data Pipeline Toolshed links
	for _, node := range datapipelines.AllNodes {
		linkList := generatePipelineToolshedLinkList(node)
		programmaticDescriptions[node] = ProgrammaticDescription{
			{
				header:  "Data Pipelines Containing Node",
				content: linkList,
			},
		}
	}

	rawProgrammaticDescriptions, err := json.Marshal(programmaticDescriptions)
	if err != nil {
		return oops.Wrapf(err, "error marshaling programmatic descriptions to json")
	}

	_, err = p.s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String("samsara-amundsen-metadata"),
		Key:    aws.String("delta/table_programmatic_descriptions.json"),
		Body:   bytes.NewReader(rawProgrammaticDescriptions),
		ACL:    aws.String(s3.ObjectCannedACLBucketOwnerFullControl),
	})
	if err != nil {
		return oops.Wrapf(err, "error uploading owners file to S3")
	}

	return nil
}

func generatePipelineToolshedLinkList(nodeName string) string {
	pipelines := datapipelines.NodesToSFNNames[nodeName]

	var links []string
	for _, pipeline := range pipelines {
		// HACK: Amundsen is only available in the US right now. We will need to pipe the region in here when we make it available for EU
		toolshedLink := fmt.Sprintf("https://toolshed.internal.samsara.com/dataplatform/datapipelines/show/arn:aws:states:%s:%d:stateMachine:%s", infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSDatabricksAccountID, pipeline)
		links = append(links, fmt.Sprintf(" - [%s](%s)  ", pipeline, toolshedLink))
	}

	return strings.Join(links, "\n")
}

var _ MetadataGenerator = &ProgrammaticDescriptionsGenerator{}
