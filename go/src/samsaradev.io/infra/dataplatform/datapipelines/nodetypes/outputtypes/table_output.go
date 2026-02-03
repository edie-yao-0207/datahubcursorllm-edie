package outputtypes

import (
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"
)

type TableOutput struct {
	BaseOutput
	DBName     string   `json:"dbname"`
	TableName  string   `json:"tablename"`
	Partition  []string `json:"partition"`
	PrimaryKey []string `json:"primary_key"`
	Schema     *Schema  `json:"schema"`
}

var _ Output = (*TableOutput)(nil)

type Schema []*Column

type Column struct {
	Name         string      `json:"name"`
	Type         interface{} `json:"type"` // Type can either be the type of the column or a nested field for structs
	NestedFields []Column    `json:"fields"`
	Nullable     bool        `json:"nullable"`
	Metadata     *Metadata   `json:"metadata,omitempty"`
}

type Metadata struct {
	Comment string `json:"comment"`
}

func (to TableOutput) DAGName() string {
	return to.DBName
}

func (to TableOutput) DestinationPrefix() string {
	return fmt.Sprintf("%s/%s/", to.DBName, to.TableName)
}

func (to TableOutput) name() string {
	return fmt.Sprintf("table_output_%s.%s", to.DBName, to.TableName)
}

func (to TableOutput) IsDateSharded() bool {
	for _, p := range to.Partition {
		if strings.Contains(p, "date") {
			return true
		}
	}
	return false
}

// validate for TableOutput validates that the partition columns and primary key columns
// exist in the schema. If either do not, throw error because this will break table creation (parition)
// or merge into (primary key) on dbx
func (to TableOutput) validate() error {
	schema := *to.Schema
	schemaColumns := make(map[string]struct{}, len(schema))
	for _, col := range schema {
		schemaColumns[col.Name] = struct{}{}
	}

	// Check partition columns
	for _, col := range to.Partition {

		// Verify the columns in the partition are not timestamp type
		// Partition by timestamp creates infinite amount of partitions and is not advised
		for _, schemaCol := range *to.Schema {
			columnType, ok := schemaCol.Type.(string)
			if !ok {
				continue
			}

			if schemaCol.Name == col && strings.ToLower(columnType) == "timestamp" {
				return oops.Errorf("Partition column %s is a timestamp and should not be used as a partition. Consider using a date column or casting this column to a date.", col)
			}
			break
		}

		if _, ok := schemaColumns[col]; !ok {
			return oops.Errorf("Partition column %s does not exist in table schema", col)
		}
	}

	// Check primary key columns
	for _, col := range to.PrimaryKey {
		if _, ok := schemaColumns[col]; !ok {
			return oops.Errorf("Primary key column %s does not exist in table schema", col)
		}
	}

	return nil
}
