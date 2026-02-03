package rewritereports

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"samsaradev.io/infra/dataplatform/rdsdeltalake"
)

// Summary of the node configuration type.
// We can't use the original unfortunately because its
// kind of confusingly created where we can't marshal it back to json.
type Dependency struct {
	Dbname    string `json:"dbname"`
	Tablename string `json:"tablename"`
	External  bool   `json:"external"`
}

type Metadata struct {
	Dependencies []Dependency `json:"dependencies"`
}

type Table struct {
	Dbname    string
	Tablename string
}

type TablePrefix string

const (
	Deprecated TablePrefix = "deprecated_"
	Parquet    TablePrefix = "parquet_"
)

func GetTablesToReplace() []string {
	var tables []string
	for _, db := range rdsdeltalake.AllDatabases() {
		dbname := db.Name
		if db.Sharded {
			dbname += "_shards"
		}
		for _, table := range db.Tables {
			tables = append(tables, dbname+"."+table.TableName)
		}
	}

	return tables
}

func BuildUpdatedSql(sqlPath string, tables []string, newPrefix TablePrefix) (string, error) {
	// Replace the sql file contents
	sqlFileBytes, err := os.ReadFile(sqlPath)
	if err != nil {
		return "", err
	}
	orig := string(sqlFileBytes)
	updatedSql := orig

	// Replace line-by-line only rds tables
	for _, table := range tables {
		updated := string(newPrefix) + table

		r := regexp.MustCompile(fmt.Sprintf(`(\s+)%s(\s+)`, table))
		updatedSql = r.ReplaceAllString(updatedSql, `${1}`+updated+`${2}`)
	}

	return updatedSql, nil
}

func RewriteTestFile(testPath string, tables []string, newPrefix TablePrefix) error {
	jsonBytes, err := os.ReadFile(testPath)
	if err != nil {
		return err
	}
	jsonString := string(jsonBytes)

	// Use string replacement to only update the dependencies section in the original JSON
	for _, table := range tables {
		regexstring := fmt.Sprintf(`"%s"`, table)
		// Regular expression to match the "dbname": "value" pattern
		re := regexp.MustCompile(regexstring)
		jsonString = re.ReplaceAllString(jsonString, fmt.Sprintf(`"%s%s"`, newPrefix, table))
	}

	// Write the updated JSON back to the file
	err = os.WriteFile(testPath, []byte(jsonString), 0644)
	if err != nil {
		return err
	}

	return nil
}

func RewriteJsonFile(jsonPath string, tables []string, newPrefix TablePrefix) error {
	jsonBytes, err := os.ReadFile(jsonPath)
	if err != nil {
		return err
	}
	jsonString := string(jsonBytes)

	// Use string replacement to only update the dependencies section in the original JSON
	for _, table := range tables {
		db := strings.Split(table, ".")[0]
		regexstring := fmt.Sprintf(`("dbname":\s*")(%s")`, db)
		// Regular expression to match the "dbname": "value" pattern
		re := regexp.MustCompile(regexstring)
		jsonString = re.ReplaceAllString(jsonString, fmt.Sprintf(`${1}%s${2}`, newPrefix))
	}

	// Write the updated JSON back to the file
	err = os.WriteFile(jsonPath, []byte(jsonString), 0644)
	if err != nil {
		return err
	}

	return nil
}
