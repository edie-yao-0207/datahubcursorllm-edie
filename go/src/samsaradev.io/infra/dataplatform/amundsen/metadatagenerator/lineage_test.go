package metadatagenerator_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"samsaradev.io/infra/dataplatform/amundsen/metadatagenerator"
)

func TestLineageDictCreation(t *testing.T) {

	type testCase struct {
		description              string
		node                     string
		expected_downstream_deps []string
	}

	dataLineage, _ := metadatagenerator.LoadDataLineageMetadata()

	testCases := []testCase{
		{
			description:              "Testing data lineage downstream dependency dictionary for node dataplatform_beta_report.report",
			node:                     "dataplatform_beta_report.report",
			expected_downstream_deps: []string{},
		},
		{
			description:              "Testing data lineage downstream dependency dictionary for node dataplatform_beta_report.metered_usage",
			node:                     "dataplatform_beta_report.metered_usage",
			expected_downstream_deps: []string{"dataplatform_beta_report.report", "dataplatform_dev.dummy_view_for_lineage"},
		},
		{
			description:              "Testing data lineage downstream dependency dictionary for node dataplatform_beta_report.whitelist_usage",
			node:                     "dataplatform_beta_report.whitelist_usage",
			expected_downstream_deps: []string{"dataplatform_beta_report.report"},
		},
		{
			description:              "Testing data lineage downstream dependency dictionary for view dataproducts.org_active_vgs",
			node:                     "dataproducts.org_active_vgs",
			expected_downstream_deps: []string{"dataproducts.org_attributes"},
		},
		{
			description:              "Testing data lineage downstream dependency dictionary for view dataproducts.fleet_driving_patterns",
			node:                     "dataproducts.fleet_driving_patterns",
			expected_downstream_deps: []string{"dataproducts.org_attributes"},
		},
	}

	for _, ts := range testCases {
		t.Run(
			fmt.Sprintf("TestCase: %s", ts.description), func(t *testing.T) {
				actual_downstream_deps := dataLineage[ts.node]
				assert.ElementsMatch(t, actual_downstream_deps, ts.expected_downstream_deps)
			})
	}
}
