package metadatagenerator

import (
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/datapipelines/configvalidator"
	"samsaradev.io/infra/dataplatform/s3tables"
	"samsaradev.io/infra/dataplatform/s3views"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/samsaraaws/s3iface"
)

type MetadataGenerator interface {
	Name() string
	GenerateMetadata(m *Metadata) error
}

type DataHubMetadataGenerator struct {
	S3Client   s3iface.S3API
	Generators []MetadataGenerator
	Metadata   *Metadata
}

// Metadata consists of pieces of metadata from the backend codebase that needs to be accessed by multiple generators.
// Populate/set up this metadata in the Setup() function for the DataHubMetadataGenerator
type Metadata struct {
	S3Tables          map[string]s3tables.S3TableConfig
	SQLViews          map[string]s3views.S3ViewConfig
	DataPipelineNodes []configvalidator.NodeConfiguration
}

func (a *DataHubMetadataGenerator) Setup() error {
	s3Tables, err := s3tables.ReadS3TableFiles()
	if err != nil {
		return oops.Wrapf(err, "error getting s3 tables")
	}

	sqlViews, err := s3views.ReadSQLViewFiles()
	if err != nil {
		return oops.Wrapf(err, "error getting sql views")
	}

	dataPipelineNodes, err := configvalidator.ReadNodeConfigurations()
	if err != nil {
		return oops.Wrapf(err, "error reading node configurations")
	}

	a.Metadata = &Metadata{
		S3Tables:          s3Tables,
		SQLViews:          sqlViews,
		DataPipelineNodes: dataPipelineNodes,
	}

	return nil
}

func newDataHubMetadataGenerator(
	s3Client s3iface.S3API,
	ownersGenerator *OwnersGenerator,
	githubSourceGenerator *GithubSourceGenerator,
	tagGenerator *TagGenerator,
	programmaticDescriptionsGenerator *ProgrammaticDescriptionsGenerator,
	descriptionsGenerator *DescriptionsGenerator,
	columnDescriptionGenerator *ColumnDescriptionGenerator,
	partitionGenerator *PartitionGenerator,
	uniqueKeysGenerator *UniqueKeysGenerator,
	lineageGenerator *LineageGenerator,
	tableBadgesGenerator *TableBadgesGenerator,
	teamsGenerator *TeamsGenerator,
) (*DataHubMetadataGenerator, error) {
	dataHubMetadataGenerator := &DataHubMetadataGenerator{
		S3Client: s3Client,
		Generators: []MetadataGenerator{
			ownersGenerator,
			githubSourceGenerator,
			tagGenerator,
			programmaticDescriptionsGenerator,
			descriptionsGenerator,
			partitionGenerator,
			columnDescriptionGenerator,
			uniqueKeysGenerator,
			lineageGenerator,
			tableBadgesGenerator,
			teamsGenerator,
		},
	}

	if err := dataHubMetadataGenerator.Setup(); err != nil {
		return nil, oops.Wrapf(err, "error setting up datahub metadata generator")
	}

	return dataHubMetadataGenerator, nil
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(newDataHubMetadataGenerator)
}
