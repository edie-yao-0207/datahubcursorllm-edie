package dynamodbdeltalake

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"samsaradev.io/infra/dataplatform/dynamodbdeltalake"
)

func TestTableComputeSpecsValid(t *testing.T) {

	testcase := dynamodbdeltalake.AllTables()
	for _, tc := range testcase {
		t.Run(tc.TableName, func(t *testing.T) {

			assert.False(t,
				tc.CustomSparkConfigurations != nil && tc.ServerlessConfig != nil,
				"CustomSparkConfiguration is not supported with ServerlessConfig. Please set only one.",
			)
		})
	}
}
