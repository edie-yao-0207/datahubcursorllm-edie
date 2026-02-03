package dataplatformresource

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/samsarahq/go/oops"
	"github.com/stretchr/testify/assert"

	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
)

type JobData struct {
	Name             string `json:"name"`
	DriverNodeTypeID string `json:"driver_node_type_id"`
	NodeTypeID       string `json:"node_type_id"`
}

func getJSONFiles(folder string) ([]string, error) {
	var files []string
	err := filepath.Walk(folder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(path) == ".json" {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

func readJSONFile(filePath string) ([]JobData, error) {
	var result []JobData

	bytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var data struct {
		Jobs []JobData `json:"jobs"`
	}

	err = json.Unmarshal(bytes, &data)
	if err != nil {
		return nil, err
	}

	for _, job := range data.Jobs {
		result = append(result, job)
	}

	return result, nil
}

func collectAllNotebookJobs(folderPath string) ([]JobData, error) {

	jsonFiles, err := getJSONFiles(folderPath)
	if err != nil {
		return nil, oops.Wrapf(err, "Error reading JSON files from: %s", folderPath)
	}

	var allData []JobData
	for _, file := range jsonFiles {
		if strings.HasSuffix(file, "metadata.json") {
			data, err := readJSONFile(file)
			if err != nil {
				return nil, oops.Wrapf(err, "Error reading JSON file: %s", file)
			}

			allData = append(allData, data...)

		}
	}

	if len(allData) == 0 {
		return nil, oops.Errorf("No notebooks jobs found to test in %s", folderPath)
	}

	return allData, nil

}

func TestNotebookForValidInstanceTypes(t *testing.T) {

	supportedInstanceTypes := dataplatformconsts.DatabricksNitroInstanceTypePrefixes
	// If you need to run this locally, make sure to run it from the backend folder.
	allData, err := collectAllNotebookJobs(dataplatformterraformconsts.NotebooksRoot)

	assert.NoError(t, err, "Encountered error collecting notebooks")

	// Test for all notebook jobs collected.
	for _, jobData := range allData {

		notebook := jobData.Name

		if jobData.DriverNodeTypeID != "" {
			driverNode := jobData.DriverNodeTypeID
			assert.Equal(
				t,
				true,
				clusterhelpers.ValidDatabricksClusterNitroInstanceType(driverNode),
				fmt.Sprintf("Unsupported driver instance type: %s, for notebook: %s, only support types are: %v", driverNode, notebook, supportedInstanceTypes),
			)
		}

		if jobData.NodeTypeID != "" {
			workerNode := jobData.NodeTypeID
			assert.Equal(
				t,
				true,
				clusterhelpers.ValidDatabricksClusterNitroInstanceType(workerNode),
				fmt.Sprintf("Unsupported worker instance type: %s, for notebook: %s, only support types are: %v", workerNode, notebook, supportedInstanceTypes),
			)
		}

	}

}
