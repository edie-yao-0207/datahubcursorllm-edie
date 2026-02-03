package dataplatformprojects

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/hashicorp/go-multierror"
	"github.com/samsarahq/go/oops"
	"github.com/xeipuuv/gojsonschema"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/libs/ni/infraconsts"
)

func getProjectMonolithStateFilePath(region string, providerGroup string, projectName string) string {
	infix := "-"
	if dataplatformresource.IsE2ProviderGroup(providerGroup) {
		infix = "-dev-"
	}

	regionAbbrv := "us"
	if region == infraconsts.SamsaraAWSEURegion {
		regionAbbrv = "eu"
	} else if region == infraconsts.SamsaraAWSCARegion {
		regionAbbrv = "ca"
	}

	return fmt.Sprintf("team/databricks%s%s/%s/terraform.tfstate", infix, regionAbbrv, projectName)
}

func validateSchema(jsonFilePath string, schemaFilePath string) error {
	schema, err := ioutil.ReadFile(filepath.Join(filepathhelpers.BackendRoot, schemaFilePath))
	if err != nil {
		return oops.Wrapf(err, "Failed to read schema json")
	}
	schemaLoader := gojsonschema.NewBytesLoader(schema)

	file, err := os.Open(jsonFilePath)
	if err != nil {
		return oops.Wrapf(err, "Unable to open file at %s", jsonFilePath)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	var schemaConfigRaw map[string]interface{}
	if err := decoder.Decode(&schemaConfigRaw); err != nil {
		return oops.Wrapf(err, "failed to decode file")
	}

	nodeLoader := gojsonschema.NewGoLoader(schemaConfigRaw)
	result, err := gojsonschema.Validate(schemaLoader, nodeLoader)
	if err != nil {
		return oops.Wrapf(err, "Failed to validate node config %s", jsonFilePath)
	}

	if !result.Valid() {
		var errs error
		for _, err := range result.Errors() {
			errs = multierror.Append(errs, errors.New(err.String()))
		}
		return oops.Wrapf(errs, "Invalid node configuration: %s", jsonFilePath)
	}
	return nil
}
