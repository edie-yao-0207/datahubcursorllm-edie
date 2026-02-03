package dependencytypes

import (
	"fmt"
)

type TableDependency struct {
	BaseDependency
	DBName    string `json:"dbname"`
	TableName string `json:"tablename"`
}

var _ Dependency = (*TableDependency)(nil)

func (td TableDependency) Name() string {
	return fmt.Sprintf("%s.%s", td.DBName, td.TableName)
}

// External returns whether the table dependency is an external dependency.
// An external table dependency is a dependency that is NOT managed by Step Function/Lambda Data Pipelines.
func (td TableDependency) External() bool {
	return td.BaseDependency.External
}
