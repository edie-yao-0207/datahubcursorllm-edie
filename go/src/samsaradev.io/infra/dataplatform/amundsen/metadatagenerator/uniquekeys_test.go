package metadatagenerator

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"samsaradev.io/infra/dataplatform/datapipelines/configvalidator"
	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes/outputtypes"
)

func TestGetUniqueKeys(t *testing.T) {
	metadata := &Metadata{
		DataPipelineNodes: []configvalidator.NodeConfiguration{
			{
				Name: "jacks_db.jacks_table",
				Output: &outputtypes.TableOutput{
					DBName:     "jacks_db",
					TableName:  "jacks_table",
					PrimaryKey: []string{"col1", "col2"},
				},
			},
		},
	}

	uniqueKeys := getUniqueKeys(metadata)
	assert.True(t, assertUniqueKey(uniqueKeys, "productsdb.devices", []string{"id"}), "expected unique keys and actual unique keys for productsdb.devices don't match")
	assert.True(t, assertUniqueKey(uniqueKeys, "jacks_db.jacks_table", []string{"col1", "col2"}), "expected unique keys and actual unique keys for jacks_db.jacks_table don't match")
	assert.True(t, assertUniqueKey(uniqueKeys, "kinesisstats.osdaccelerometer", []string{"org_id", "object_type", "object_id", "time"}), "expected unique keys and actual unique keys for kinesisstats.osdaccelerometer don't match")
	assert.True(t, assertUniqueKey(uniqueKeys, "kinesisstats.location", []string{"org_id", "device_id", "time"}), "expected unique keys and actual unique keys for kinesisstats.location don't match")
	assert.True(t, assertUniqueKey(uniqueKeys, "s3bigstats.osdreporteddeviceconfig", []string{"org_id", "object_type", "object_id", "time"}), "expected unique keys and actual unique keys for s3bigstats.osdreporteddeviceconfig don't match")
	assert.True(t, assertUniqueKey(uniqueKeys, "kinesisstats.osdreporteddeviceconfig_with_s3_big_stat", []string{"org_id", "object_type", "object_id", "time"}), "expected unique keys and actual unique keys for kinesisstats.osdreporteddeviceconfig_with_s3_big_stat don't match")
}

func assertUniqueKey(uniqueKeyMap map[string][]string, tableName string, expectedUniqueKeys []string) bool {
	if uniqueKeys, ok := uniqueKeyMap[tableName]; ok {
		return reflect.DeepEqual(uniqueKeys, expectedUniqueKeys)
	}

	return false
}
