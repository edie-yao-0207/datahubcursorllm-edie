package metadatagenerator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/samsaraaws/s3iface"
)

type LineageGenerator struct {
	s3Client s3iface.S3API
}

func newLineageGenerator(s3Client s3iface.S3API) (*LineageGenerator, error) {
	return &LineageGenerator{
		s3Client: s3Client,
	}, nil
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(newLineageGenerator)
}

func (a *LineageGenerator) Name() string {
	return "amundsen-lineage-generator"
}

// This table takes in a directory as a string as input. It walks through the directory looking for json schema files
// From these schema files, it returns a dictionary whose key is a table name (ex: db.table) and value is the downstream dependencyes (ex: also db.table) of the key table
func createLineageDictPerPath(lineageDirectory string) (map[string][]string, error) {

	dataLineage := make(map[string][]string)
	err := filepath.Walk(lineageDirectory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return oops.Wrapf(err, "error opening %s", lineageDirectory)
		}

		fileName := info.Name()
		if strings.HasSuffix(fileName, ".json") && !strings.HasSuffix(fileName, ".test.json") {

			jsonFile, err := os.Open(path)
			if err != nil {
				return oops.Wrapf(err, "error opening json schema file")
			}
			// defer the closing of our jsonFile so that we can parse it later on
			defer jsonFile.Close()

			byteValue, _ := ioutil.ReadAll(jsonFile)

			var result map[string]interface{}
			err = json.Unmarshal([]byte(byteValue), &result)
			if err != nil {
				return oops.Wrapf(err, "error unmarshalling json schema file")
			}

			dependencies := result["dependencies"]
			tableName := fmt.Sprintf("%s", result["name"])
			if _, found := result["view"]; found {
				tableName = fmt.Sprintf("%s.%s", result["database"], result["view"])
			}
			// initialize tableName with an empty to ensure leaf nodes are included
			dataLineage[tableName] = append(dataLineage[tableName], []string{}...)
			if dependencies != nil {
				dependencies_list := reflect.ValueOf(dependencies)
				for i := 0; i < dependencies_list.Len(); i++ {
					item := dependencies_list.Index(i).Interface().(map[string]interface{})
					dependency := fmt.Sprintf("%s.%s", item["dbname"], item["tablename"])
					dataLineage[dependency] = append(dataLineage[dependency], tableName)
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, oops.Wrapf(err, "error walking lineage directory")
	}

	return dataLineage, nil
}

func createLineageDict(lineagePaths []string) (map[string][]string, error) {
	dataLineage := make(map[string][]string)
	for _, path := range lineagePaths {
		lineageDictForPath, err := createLineageDictPerPath(path)
		if err != nil {
			return nil, oops.Wrapf(err, "error creating lineage dict for path %s", path)
		}
		for table, dependencies := range lineageDictForPath {
			if _, ok := dataLineage[table]; ok {
				// If table already in lineage map, then append rather than overwrite
				dataLineage[table] = append(dataLineage[table], dependencies...)
			} else {
				dataLineage[table] = dependencies
			}
		}
	}
	return dataLineage, nil
}

func LoadDataLineageMetadata() (map[string][]string, error) {
	sqlviewsPath := os.ExpandEnv("$BACKEND_ROOT/dataplatform/tables/sqlview")
	datapipelinesPath := os.ExpandEnv("$BACKEND_ROOT/dataplatform/tables/transformations")

	lineagePaths := []string{datapipelinesPath, sqlviewsPath}

	dataLineage, err := createLineageDict(lineagePaths)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to generate lineage metadata and write to S3.")
	}
	return dataLineage, nil
}

func (a *LineageGenerator) GenerateMetadata(metadata *Metadata) error {
	ctx := context.Background()

	dataLineageMetadata, err := LoadDataLineageMetadata()
	if err != nil {
		return oops.Wrapf(err, "error marhsaling data lineage map to json")
	}

	rawLineage, err := json.Marshal(dataLineageMetadata)
	if err != nil {
		return oops.Wrapf(err, "error marshalling data lineage metadata to json")
	}

	_, err = a.s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String("samsara-amundsen-metadata"),
		Key:    aws.String("delta/table_lineage.json"),
		Body:   bytes.NewReader(rawLineage),
		ACL:    aws.String(s3.ObjectCannedACLBucketOwnerFullControl),
	})
	if err != nil {
		return oops.Wrapf(err, "error uploading lineage file to S3")
	}

	return nil
}
