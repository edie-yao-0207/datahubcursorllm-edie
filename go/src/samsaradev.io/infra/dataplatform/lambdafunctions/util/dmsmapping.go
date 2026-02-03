// Helper structs and functions for managing dms table mapping json

package util

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
)

// structs repeated from awsresource to avoid extra lambda dependencies
type DMSTableMappingRuleObjectLocator struct {
	SchemaName *string `json:"schema-name,omitempty"`
	TableName  *string `json:"table-name,omitempty"`
}

type DMSTableMappingRule struct {
	RuleType      *string                           `json:"rule-type,omitempty"`
	RuleId        *int                              `json:"rule-id,omitempty"`
	RuleName      *string                           `json:"rule-name,omitempty"`
	RuleTarget    *string                           `json:"rule-target,omitempty"`
	ObjectLocator *DMSTableMappingRuleObjectLocator `json:"object-locator,omitempty"`
	RuleAction    *string                           `json:"rule-action,omitempty"`
	Value         *string                           `json:"value,omitempty"`
}

type DMSTableMapping struct {
	Rules []*DMSTableMappingRule `json:"rules,omitempty"`
}

type NewTableDefinition struct {
	SchemaName string
	TableName  string
	Version    int
}

// MergeTablesIntoDmsMapping adds, removes, and replaces rdslake versioned tables in a DmsTableMapping
func MergeTablesIntoDmsMapping(existingMapping *DMSTableMapping, targetTables []NewTableDefinition) *DMSTableMapping {
	tableExistingVersion := make(map[[2]string]int)
	tableExistingRules := make(map[[2]string][]*DMSTableMappingRule)
	for _, rule := range existingMapping.Rules {
		schemaName := aws.StringValue(rule.ObjectLocator.SchemaName)
		tableName := aws.StringValue(rule.ObjectLocator.TableName)
		key := [2]string{schemaName, tableName}
		// rds table "version" is used to modify transformation rules so we can determine
		// the target version for dms by parsing the _vX suffix on a table's transformation
		// rule
		if aws.StringValue(rule.RuleType) == "transformation" {
			version, err := strconv.Atoi(strings.TrimPrefix(aws.StringValue(rule.Value), "_v"))
			if err == nil {
				tableExistingVersion[key] = version
			}
		}
		tableExistingRules[key] = append(tableExistingRules[key], rule)
	}

	usedRuleIds := make(map[int]struct{})
	var rules []*DMSTableMappingRule
	for _, table := range targetTables {
		key := [2]string{table.SchemaName, table.TableName}
		existingVersion, ok := tableExistingVersion[key]
		if ok && table.Version == existingVersion {
			for _, existingRule := range tableExistingRules[key] {
				usedRuleIds[aws.IntValue(existingRule.RuleId)] = struct{}{}
				rules = append(rules, existingRule)
			}
		} else {
			rules = append(rules,
				&DMSTableMappingRule{
					// In our Data Platform, we specify "versions" for rds tables
					// to allow us to rebuild or replace tables in the event of an
					// error without disrupting existing data reads. This is
					// accomplished by appending a _vX to the location where dms
					// writes out table data and then cutting over reads once
					// export has completed.
					RuleType: aws.String("transformation"),
					ObjectLocator: &DMSTableMappingRuleObjectLocator{
						SchemaName: aws.String(table.SchemaName),
						TableName:  aws.String(table.TableName),
					},
					RuleAction: aws.String("add-suffix"),
					RuleTarget: aws.String("table"),
					Value:      aws.String(fmt.Sprintf("_v%d", table.Version)),
				},
				&DMSTableMappingRule{
					RuleType: aws.String("selection"),
					ObjectLocator: &DMSTableMappingRuleObjectLocator{
						SchemaName: aws.String(table.SchemaName),
						TableName:  aws.String(table.TableName),
					},
					RuleAction: aws.String("explicit"),
				},
			)
		}
	}

	nextRuleId := 1
	for i := range rules {
		if rules[i].RuleId != nil {
			continue
		}
		for {
			_, ok := usedRuleIds[nextRuleId]
			if !ok {
				break
			}
			nextRuleId++
		}
		rules[i].RuleId = aws.Int(nextRuleId)
		rules[i].RuleName = aws.String(strconv.Itoa(nextRuleId))
		usedRuleIds[nextRuleId] = struct{}{}
	}
	sort.Slice(rules, func(i, j int) bool {
		return aws.StringValue(rules[i].RuleName) < aws.StringValue(rules[j].RuleName)
	})

	return &DMSTableMapping{
		Rules: rules,
	}
}
