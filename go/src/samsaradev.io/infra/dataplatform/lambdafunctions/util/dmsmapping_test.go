package util_test

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/stretchr/testify/assert"

	"samsaradev.io/infra/dataplatform/lambdafunctions/util"
)

func TestMergeTablesIntoDmsMapping(t *testing.T) {
	testCases := []struct {
		label        string
		existing     *util.DMSTableMapping
		targetTables []util.NewTableDefinition
		expected     *util.DMSTableMapping
	}{
		{
			label: "initial_setup",
			existing: &util.DMSTableMapping{
				Rules: []*util.DMSTableMappingRule{
					&util.DMSTableMappingRule{
						RuleName: aws.String("exclude-all"),
						RuleId:   aws.Int(1),
						RuleType: aws.String("selection"),
						ObjectLocator: &util.DMSTableMappingRuleObjectLocator{
							SchemaName: aws.String("%"),
							TableName:  aws.String("%"),
						},
						RuleAction: aws.String("exclude"),
					},
				},
			},
			targetTables: []util.NewTableDefinition{
				{
					SchemaName: "schema",
					TableName:  "table1",
					Version:    0,
				},
			},
			expected: &util.DMSTableMapping{
				Rules: []*util.DMSTableMappingRule{
					&util.DMSTableMappingRule{
						RuleId:   aws.Int(1),
						RuleName: aws.String("1"),
						RuleType: aws.String("transformation"),
						ObjectLocator: &util.DMSTableMappingRuleObjectLocator{
							SchemaName: aws.String("schema"),
							TableName:  aws.String("table1"),
						},
						RuleAction: aws.String("add-suffix"),
						RuleTarget: aws.String("table"),
						Value:      aws.String("_v0"),
					},
					&util.DMSTableMappingRule{
						RuleId:   aws.Int(2),
						RuleName: aws.String("2"),
						RuleType: aws.String("selection"),
						ObjectLocator: &util.DMSTableMappingRuleObjectLocator{
							SchemaName: aws.String("schema"),
							TableName:  aws.String("table1"),
						},
						RuleAction: aws.String("explicit"),
					},
				},
			},
		},
		{
			label: "replace_version_change",
			existing: &util.DMSTableMapping{
				Rules: []*util.DMSTableMappingRule{
					&util.DMSTableMappingRule{
						RuleId:   aws.Int(1),
						RuleName: aws.String("1"),
						RuleType: aws.String("transformation"),
						ObjectLocator: &util.DMSTableMappingRuleObjectLocator{
							SchemaName: aws.String("schema"),
							TableName:  aws.String("table1"),
						},
						RuleAction: aws.String("add-suffix"),
						RuleTarget: aws.String("table"),
						Value:      aws.String("_v0"),
					},
					&util.DMSTableMappingRule{
						RuleId:   aws.Int(2),
						RuleName: aws.String("2"),
						RuleType: aws.String("selection"),
						ObjectLocator: &util.DMSTableMappingRuleObjectLocator{
							SchemaName: aws.String("schema"),
							TableName:  aws.String("table1"),
						},
						RuleAction: aws.String("explicit"),
					},
				},
			},
			targetTables: []util.NewTableDefinition{
				{
					SchemaName: "schema",
					TableName:  "table1",
					Version:    1,
				},
			},
			expected: &util.DMSTableMapping{
				Rules: []*util.DMSTableMappingRule{
					&util.DMSTableMappingRule{
						RuleId:   aws.Int(1),
						RuleName: aws.String("1"),
						RuleType: aws.String("transformation"),
						ObjectLocator: &util.DMSTableMappingRuleObjectLocator{
							SchemaName: aws.String("schema"),
							TableName:  aws.String("table1"),
						},
						RuleAction: aws.String("add-suffix"),
						RuleTarget: aws.String("table"),
						Value:      aws.String("_v1"),
					},
					&util.DMSTableMappingRule{
						RuleId:   aws.Int(2),
						RuleName: aws.String("2"),
						RuleType: aws.String("selection"),
						ObjectLocator: &util.DMSTableMappingRuleObjectLocator{
							SchemaName: aws.String("schema"),
							TableName:  aws.String("table1"),
						},
						RuleAction: aws.String("explicit"),
					},
				},
			},
		},
		{
			label: "preserve_ids",
			existing: &util.DMSTableMapping{
				Rules: []*util.DMSTableMappingRule{
					&util.DMSTableMappingRule{
						RuleId:   aws.Int(9),
						RuleName: aws.String("9"),
						RuleType: aws.String("transformation"),
						ObjectLocator: &util.DMSTableMappingRuleObjectLocator{
							SchemaName: aws.String("schema"),
							TableName:  aws.String("table1"),
						},
						RuleAction: aws.String("add-suffix"),
						RuleTarget: aws.String("table"),
						Value:      aws.String("_v0"),
					},
					&util.DMSTableMappingRule{
						RuleId:   aws.Int(8),
						RuleName: aws.String("8"),
						RuleType: aws.String("selection"),
						ObjectLocator: &util.DMSTableMappingRuleObjectLocator{
							SchemaName: aws.String("schema"),
							TableName:  aws.String("table1"),
						},
						RuleAction: aws.String("explicit"),
					},
				},
			},
			targetTables: []util.NewTableDefinition{
				{
					SchemaName: "schema",
					TableName:  "table1",
					Version:    0,
				},
				{
					SchemaName: "schema",
					TableName:  "table0",
					Version:    3,
				},
			},
			expected: &util.DMSTableMapping{
				Rules: []*util.DMSTableMappingRule{
					&util.DMSTableMappingRule{
						RuleId:   aws.Int(1),
						RuleName: aws.String("1"),
						RuleType: aws.String("transformation"),
						ObjectLocator: &util.DMSTableMappingRuleObjectLocator{
							SchemaName: aws.String("schema"),
							TableName:  aws.String("table0"),
						},
						RuleAction: aws.String("add-suffix"),
						RuleTarget: aws.String("table"),
						Value:      aws.String("_v3"),
					},
					&util.DMSTableMappingRule{
						RuleId:   aws.Int(2),
						RuleName: aws.String("2"),
						RuleType: aws.String("selection"),
						ObjectLocator: &util.DMSTableMappingRuleObjectLocator{
							SchemaName: aws.String("schema"),
							TableName:  aws.String("table0"),
						},
						RuleAction: aws.String("explicit"),
					},
					&util.DMSTableMappingRule{
						RuleId:   aws.Int(8),
						RuleName: aws.String("8"),
						RuleType: aws.String("selection"),
						ObjectLocator: &util.DMSTableMappingRuleObjectLocator{
							SchemaName: aws.String("schema"),
							TableName:  aws.String("table1"),
						},
						RuleAction: aws.String("explicit"),
					},
					&util.DMSTableMappingRule{
						RuleId:   aws.Int(9),
						RuleName: aws.String("9"),
						RuleType: aws.String("transformation"),
						ObjectLocator: &util.DMSTableMappingRuleObjectLocator{
							SchemaName: aws.String("schema"),
							TableName:  aws.String("table1"),
						},
						RuleAction: aws.String("add-suffix"),
						RuleTarget: aws.String("table"),
						Value:      aws.String("_v0"),
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		tc := tc // capture variable
		t.Run(tc.label, func(t *testing.T) {
			assert.Equal(t, tc.expected, util.MergeTablesIntoDmsMapping(tc.existing, tc.targetTables))
		})
	}
}
