package configvalidator

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/samsarahq/go/oops"
	"github.com/xeipuuv/gojsonschema"

	"samsaradev.io/helpers/ni/filepathhelpers"
)

var TransformationDir = filepath.Join(filepathhelpers.BackendRoot, "dataplatform/tables/transformations")

// These are the sqlite reports that are allowed to set hardcoded dates. Because this feature is quite nonstandard,
// we want to make sure that we talk through why a new report would need to set these.
var SqliteEnabledHardcodedDates = []string{
	"cm_health_report",
	"camera_connector_health",
}

func ReadNodeConfigurations() ([]NodeConfiguration, error) {
	var nodeConfigurations []NodeConfiguration
	nodeNames := map[string]bool{}
	walkErr := filepath.Walk(TransformationDir, func(path string, info os.FileInfo, err error) error {

		// By convention, the directory names should be snake_case with underscores and no hyphens.
		if info.IsDir() {
			if strings.Contains(info.Name(), "-") {
				return oops.Wrapf(err, "Invalid directory name %s. Please remove any `-` characters and use snake_case.", info.Name())
			}
			return nil
		}

		if filepath.Ext(path) != ".json" || strings.HasSuffix(path, ".test.json") {
			return nil
		}

		nodeConfig, err := loadAndValidateNodeConfigurationFile(path)
		if err != nil {
			return oops.Wrapf(err, "Invalid node configuration for %s", info.Name())
		}

		if nodeNames[strings.ToLower(nodeConfig.Name)] {
			return oops.Errorf("Duplicate node name: %s. Can not have nodes with colliding names.", nodeConfig.Name)
		}
		nodeNames[nodeConfig.Name] = true

		nodeConfigurations = append(nodeConfigurations, *nodeConfig)
		return nil
	})

	if walkErr != nil {
		return nil, oops.Wrapf(walkErr, "Error walking transformations directory")
	}

	return nodeConfigurations, nil
}

func loadAndValidateNodeConfigurationFile(filePath string) (*NodeConfiguration, error) {
	nodeSchema, err := ioutil.ReadFile(filepath.Join(filepathhelpers.BackendRoot, "go/src/samsaradev.io/infra/dataplatform/datapipelines/nodetypes/node.schema.json"))
	if err != nil {
		return nil, oops.Wrapf(err, "Failed to read node schema json")
	}
	schemaLoader := gojsonschema.NewBytesLoader(nodeSchema)

	file, err := os.Open(filePath)
	if err != nil {
		return nil, oops.Wrapf(err, "Unable to open file at %s", filePath)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	var nodeConfigRaw map[string]interface{}
	if err := decoder.Decode(&nodeConfigRaw); err != nil {
		return nil, oops.Wrapf(err, "failed to decode file")
	}

	// If expectation file exists, expectation field must also exist
	expectationFile := strings.TrimSuffix(filePath, filepath.Ext(filePath)) + ".expectation.sql"
	_, ok := nodeConfigRaw["expectation"]
	if _, err := os.Stat(expectationFile); err == nil {
		if !ok {
			return nil, oops.Errorf("Expectation sql file found, but expectation field missing. Node configuration file at path %s must contain expectation field", filePath)
		}
	} else {
		if ok {
			return nil, oops.Errorf("Expectation field found, but expectation sql file missing. Please define a expectation.sql file at path %s", filepath.Dir(filePath))
		}
	}

	nodeLoader := gojsonschema.NewGoLoader(nodeConfigRaw)
	result, err := gojsonschema.Validate(schemaLoader, nodeLoader)
	if err != nil {
		return nil, oops.Wrapf(err, "Failed to validate node config %s", filePath)
	}

	if !result.Valid() {
		var errs error
		for _, err := range result.Errors() {
			errs = multierror.Append(errs, errors.New(err.String()))
		}
		return nil, oops.Wrapf(errs, "Invalid node configuration: %s", filePath)
	}

	// Passed JSON validation against node.schema.json
	// Unmarhsal into NodeConfiguration struct
	fileBytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, oops.Wrapf(err, "Unable to read file at %s", filePath)
	}

	var nodeConfig NodeConfiguration
	jsonErr := json.Unmarshal(fileBytes, &nodeConfig)
	if jsonErr != nil {
		return nil, oops.Wrapf(jsonErr, "Error unmarshaling JSON")
	}

	// Ensure that the db name and table names don't have `-`
	if strings.Contains(nodeConfig.Name, "-") {
		return nil, oops.Errorf("Node name %s should not have `-` characters in it. Please use snake_case.", nodeConfig.Name)
	}

	if nodeConfig.SqliteReportConfig != nil {
		if nodeConfig.SqliteReportConfig.HardcodedStartDate != nil || nodeConfig.SqliteReportConfig.HardcodedEndDate != nil {
			found := false
			for _, pattern := range SqliteEnabledHardcodedDates {
				if strings.Contains(nodeConfig.Name, pattern) {
					found = true
					break
				}
			}
			if !found {
				return nil, oops.Errorf("Sqlite Export with hardcoded dates is not generally supported. If you need this feature, please contact Data Platform to add your report to the exemptions list.")
			}
		}
	}

	return &nodeConfig, nil
}
