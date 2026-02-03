package s3tables

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/ni/filepathhelpers"
)

type SchemaMetadata struct {
	Comment string `json:"comment"`
}

type ColumnSchema struct {
	Name        string         `json:"name"`
	Type        string         `json:"type"`
	Description string         `json:"description"`
	Metadata    SchemaMetadata `json:"metadata"`
}

type S3TableConfigCSV struct {
	Separator string `json:"separator"`
	Quote     string `json:"quote"`
	Escape    string `json:"escape"`
	Header    bool   `json:"header"`
	Multiline bool   `json:"multiline"`
}

type S3TableConfig struct {
	Table               string            `json:"table"`
	Owner               string            `json:"owner"`
	Description         string            `json:"description"`
	Database            string            `json:"database"`
	RegionToLocation    map[string]string `json:"region_to_location"`
	DataType            string            `json:"datatype"`
	Schema              []ColumnSchema    `json:"schema"`
	PartitionKeys       []ColumnSchema    `json:"partitions"`
	CSV                 S3TableConfigCSV  `json:"csv"`
	UnityCatalogEnabled bool              `json:"unity_catalog_enabled"`
}

func ReadS3TableFiles() (map[string]S3TableConfig, error) {
	s3TableNameToMetadata := make(map[string]S3TableConfig)
	err := filepath.Walk(filepath.Join(filepathhelpers.BackendRoot, "dataplatform/tables/s3data"), func(path string, info os.FileInfo, err error) error {
		if info.IsDir() || filepath.Ext(path) != ".json" || strings.HasSuffix(path, "_data.json") {
			return nil
		}
		fileBytes, err := ioutil.ReadFile(path)
		if err != nil {
			return oops.Wrapf(err, "error reading config file %s", path)
		}

		// Unmarshal JSON, get table config values, and add entry in map if dne
		var tableConfig S3TableConfig
		err = json.Unmarshal(fileBytes, &tableConfig)
		if err != nil {
			return oops.Wrapf(err, "error unmarshaling JSON")
		}

		sparkTableName := fmt.Sprintf("%s.%s", tableConfig.Database, tableConfig.Table)
		s3TableNameToMetadata[sparkTableName] = tableConfig

		return nil
	})
	if err != nil {
		return nil, oops.Wrapf(err, "error reading s3 tables directory")
	}

	return s3TableNameToMetadata, nil
}
