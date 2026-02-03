package dataplatformprojects

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

func fetchJobClusters(filePath string) ([]JobCluster, error) {
	var result []JobCluster

	bytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var data WorkflowConfigs

	err = json.Unmarshal(bytes, &data)
	if err != nil {
		return nil, err
	}

	for _, jobCluster := range data.JobClusters {
		result = append(result, jobCluster)
	}

	return result, nil
}

func collectAllWorkflowsClusters(folderPath string) ([]JobCluster, error) {

	jsonFiles, err := getJSONFiles(folderPath)
	if err != nil {
		return nil, oops.Wrapf(err, "Error reading JSON files from: %s", folderPath)
	}

	var allData []JobCluster
	for _, file := range jsonFiles {
		if !strings.HasSuffix(file, "metadata.json") {
			data, err := fetchJobClusters(file)
			if err != nil {
				return nil, oops.Wrapf(err, "Error reading JSON file: %s", file)
			}

			allData = append(allData, data...)

		}
	}

	if len(allData) == 0 {
		return nil, oops.Errorf("No workflow jobs clusters found to test in %s", folderPath)
	}

	return allData, nil

}

type JobCluster struct {
	JobClusterKey string `json:"job_cluster_key"`
	NewCluster    struct {
		NodeTypeID       string `json:"node_type_id"`
		DriverNodeTypeID string `json:"driver_node_type_id"`
	} `json:"new_cluster"`
}

type WorkflowConfigs struct {
	JobClusters []JobCluster `json:"job_clusters"`
}

func TestWorkflowsForValidInstanceTypes(t *testing.T) {

	supportedInstanceTypes := dataplatformconsts.DatabricksNitroInstanceTypePrefixes
	// If you need to run this locally, make sure to run it from the backend folder.
	allData, err := collectAllWorkflowsClusters(dataplatformterraformconsts.WorkflowsRoot)

	assert.NoError(t, err, "Encountered error collecting workflows")

	// Test for all workflow clusters jobs collected.
	for _, jobCluster := range allData {

		clusterKey := jobCluster.JobClusterKey

		if jobCluster.NewCluster.DriverNodeTypeID != "" {
			driverNode := jobCluster.NewCluster.DriverNodeTypeID
			assert.Equal(
				t,
				true,
				clusterhelpers.ValidDatabricksClusterNitroInstanceType(driverNode),
				fmt.Sprintf("Unsupported driver instance type: %s, for cluster-key: %s, only support types are: %v", driverNode, clusterKey, supportedInstanceTypes),
			)
		}

		if jobCluster.NewCluster.NodeTypeID != "" {
			workerNode := jobCluster.NewCluster.NodeTypeID
			assert.Equal(
				t,
				true,
				clusterhelpers.ValidDatabricksClusterNitroInstanceType(workerNode),
				fmt.Sprintf("Unsupported worker instance type: %s, for cluster-key: %s, only support types are: %v", workerNode, clusterKey, supportedInstanceTypes),
			)
		}

	}

}
