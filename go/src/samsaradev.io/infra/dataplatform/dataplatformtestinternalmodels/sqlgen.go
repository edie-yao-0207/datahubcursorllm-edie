package dataplatformtestinternalmodels

import (
	"samsaradev.io/infra/thunder/sqlgen"
)

//go:generate go run "$GOPATH/src/samsaradev.io/sqlgengen/cmd/sqlgengen/main.go" -output "../dataplatformtestinternalmodels/generated_sqlgengen.go" -directory "../dataplatformtestinternalmodels/" -index "../dataplatformtestinternalmodels/sqlgen.go" -packageName "dataplatformtestinternalmodels" -structName "DataplatformtestinternalModels" -dbFieldName "sqlgenDB"

func buildSqlgenSchema() *sqlgen.Schema {
	schema := sqlgen.NewSchema()
	// TODO: Register model structs to schema.
	return schema
}

var sqlgenSchema = buildSqlgenSchema()
