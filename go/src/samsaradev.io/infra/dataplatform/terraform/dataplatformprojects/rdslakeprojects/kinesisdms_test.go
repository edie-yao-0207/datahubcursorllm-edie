package rdslakeprojects

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
)

func TestBuildKinesisDMSTableMappings_WithPartitionKeyAttribute(t *testing.T) {
	spec := rdsKinesisReplicationSpec{
		MySqlDbName: "eurofleetdb",
		DbShardName: "prod-eurofleet-shard-1db",
		Tables: []rdsReplicationSpecTable{
			{
				Name:                  "driver_status_edits",
				Version:               1,
				CdcVersion:            0,
				PartitionKeyAttribute: "org_id",
			},
		},
	}

	// Test CDC mapping
	cdcMapping := buildKinesisDMSTableMappings(spec, true)
	require.NotNil(t, cdcMapping.Rules)

	// Find the object-mapping rule for driver_status_edits
	var objectMappingRule *ruleWithMappingParams
	for _, rule := range cdcMapping.Rules {
		if rule.RuleType != nil && *rule.RuleType == "object-mapping" &&
			rule.ObjectLocator != nil && rule.ObjectLocator.TableName != nil &&
			*rule.ObjectLocator.TableName == "driver_status_edits" {
			objectMappingRule = &ruleWithMappingParams{rule}
			break
		}
	}

	require.NotNil(t, objectMappingRule, "Expected object-mapping rule for driver_status_edits")
	require.NotNil(t, objectMappingRule.MappingParameters, "Expected MappingParameters to be set")
	assert.Equal(t, "attribute-name", *objectMappingRule.MappingParameters.PartitionKeyType)
	assert.Equal(t, "org_id", *objectMappingRule.MappingParameters.PartitionKeyName)
}

func TestBuildKinesisDMSTableMappings_WithoutPartitionKeyAttribute(t *testing.T) {
	spec := rdsKinesisReplicationSpec{
		MySqlDbName: "testdb",
		DbShardName: "prod-test-shard-1db",
		Tables: []rdsReplicationSpecTable{
			{
				Name:       "test_table",
				Version:    1,
				CdcVersion: 0,
				// No PartitionKeyAttribute
			},
		},
	}

	cdcMapping := buildKinesisDMSTableMappings(spec, true)
	require.NotNil(t, cdcMapping.Rules)

	// Verify no object-mapping rule is created when PartitionKeyAttribute is empty
	for _, rule := range cdcMapping.Rules {
		if rule.RuleType != nil && *rule.RuleType == "object-mapping" {
			t.Error("Did not expect object-mapping rule when PartitionKeyAttribute is empty")
		}
	}
}

func TestBuildKinesisDMSTableMappings_BaseRules(t *testing.T) {
	spec := rdsKinesisReplicationSpec{
		MySqlDbName: "testdb",
		DbShardName: "prod-test-shard-1db",
		Tables: []rdsReplicationSpecTable{
			{
				Name:       "test_table",
				Version:    1,
				CdcVersion: 0,
			},
		},
	}

	// Test CDC mapping
	cdcMapping := buildKinesisDMSTableMappings(spec, true)
	require.NotNil(t, cdcMapping.Rules)
	require.GreaterOrEqual(t, len(cdcMapping.Rules), 3, "Expected at least 3 base transformation rules")

	// Verify base transformation rules
	// Rule 0: _rowid column
	assert.Equal(t, "transformation", *cdcMapping.Rules[0].RuleType)
	assert.Equal(t, "_rowid", *cdcMapping.Rules[0].Value)

	// Rule 1: _dms_shard_name column
	assert.Equal(t, "transformation", *cdcMapping.Rules[1].RuleType)
	assert.Equal(t, "_dms_shard_name", *cdcMapping.Rules[1].Value)

	// Rule 2: _dms_task_type column
	assert.Equal(t, "transformation", *cdcMapping.Rules[2].RuleType)
	assert.Equal(t, "_dms_task_type", *cdcMapping.Rules[2].Value)
	assert.Contains(t, *cdcMapping.Rules[2].Expression, "cdc")

	// Test load mapping
	loadMapping := buildKinesisDMSTableMappings(spec, false)
	assert.Contains(t, *loadMapping.Rules[2].Expression, "load")
}

// Helper type to access MappingParameters
type ruleWithMappingParams struct {
	*awsresource.DMSTableMappingRule
}
