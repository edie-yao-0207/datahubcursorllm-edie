package s3views

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/ni/filepathhelpers"
)

type FieldSchema struct {
	Name        string        `json:"name"`
	Type        string        `json:"type"`
	Fields      []FieldSchema `json:"fields"`
	Description string        `json:"description"`
}

type S3ViewConfig struct {
	View                string        `json:"view"`
	Owner               string        `json:"owner"`
	Description         string        `json:"description"`
	Database            string        `json:"database"`
	UseRegionalValues   bool          `json:"use_regional_values"`
	Schema              []FieldSchema `json:"schema"`
	Regions             []string      `json:"regions,omitempty"`
	UnityCatalogEnabled bool          `json:"unity_catalog_enabled"`
}

// RegionalViewTemplate can be used to insert region specific
// values into s3 views. For example, if your view needs to
// read from a US bucket and an EU bucket where the only
// difference is the bucket prefix `samsara...` vs
// `samsara_eu...` then you can set UseRegionalValues
// above to true and the region specific prefix will be
// inserted where ever you provide the {{.BucketPrefix}}
// template action.
type RegionalViewTemplate struct {
	BucketPrefix             string
	ProductUsageShareCatalog string
}

func ReadSQLViewFiles() (map[string]S3ViewConfig, error) {
	sqlViewNameToMetadata := make(map[string]S3ViewConfig)
	err := filepath.Walk(filepath.Join(filepathhelpers.BackendRoot, "dataplatform/tables/sqlview"), func(path string, info os.FileInfo, err error) error {
		if info.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}

		fileBytes, err := ioutil.ReadFile(path)
		if err != nil {
			return oops.Wrapf(err, "error reading config file %s", path)
		}

		var viewConfig S3ViewConfig
		err = json.Unmarshal(fileBytes, &viewConfig)
		if err != nil {
			return oops.Wrapf(err, "error unmarshaling JSON")
		}

		sparkViewName := fmt.Sprintf("%s.%s", viewConfig.Database, viewConfig.View)
		sqlViewNameToMetadata[sparkViewName] = viewConfig
		return nil
	})
	if err != nil {
		return nil, oops.Wrapf(err, "error reading sql views directory")
	}

	return sqlViewNameToMetadata, nil
}
