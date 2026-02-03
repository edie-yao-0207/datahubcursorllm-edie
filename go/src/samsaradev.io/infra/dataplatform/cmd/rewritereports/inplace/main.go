package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/dataplatform/cmd/rewritereports"
)

func processDatapipeline(sqlPath, jsonPath string) error {
	prefix := rewritereports.Deprecated
	tables := rewritereports.GetTablesToReplace()

	fmt.Printf("processing %s\n", sqlPath)

	updatedSql, err := rewritereports.BuildUpdatedSql(sqlPath, tables, prefix)
	if err != nil {
		return err
	}

	err = os.WriteFile(sqlPath, []byte(updatedSql), 0644)
	if err != nil {
		return err
	}

	err = rewritereports.RewriteJsonFile(jsonPath, tables, prefix)
	if err != nil {
		return err
	}

	possibleTestName := sqlPath[:len(sqlPath)-3] + "test.json"
	if _, err := os.Stat(possibleTestName); err == nil {
		fmt.Printf("Found test file, will also rewrite %s\n", possibleTestName)
		err = rewritereports.RewriteTestFile(possibleTestName, tables, prefix)
		if err != nil {
			return err
		}
	}

	return nil
}

func processReport(sqlPath string) error {
	prefix := rewritereports.Deprecated
	tables := rewritereports.GetTablesToReplace()

	fmt.Printf("processing %s\n", sqlPath)

	updatedSql, err := rewritereports.BuildUpdatedSql(sqlPath, tables, prefix)
	if err != nil {
		return err
	}

	err = os.WriteFile(sqlPath, []byte(updatedSql), 0644)
	if err != nil {
		return err
	}

	return nil
}

func visitDatapipeline(path string, f os.FileInfo, err error) error {
	if err != nil {
		fmt.Printf("error visiting path %v: %v\n", path, err)
		return nil
	}

	if !f.IsDir() && filepath.Ext(path) == ".sql" && !strings.HasSuffix(path, ".expectation.sql") {
		jsonPath := strings.Replace(path, ".sql", ".json", 1)
		if err := processDatapipeline(path, jsonPath); err != nil {
			fmt.Printf("error processing file %v: %v\n", path, err)
		}
	}
	return nil
}

func visitReport(path string, f os.FileInfo, err error) error {
	if err != nil {
		fmt.Printf("error visiting path %v: %v\n", path, err)
		return nil
	}

	if !f.IsDir() && filepath.Ext(path) == ".sql" {
		if err := processReport(path); err != nil {
			fmt.Printf("error processing file %v: %v\n", path, err)
		}
	}
	return nil

}

func main() {
	datapipelinesRoot := filepath.Join(filepathhelpers.BackendRoot, "dataplatform", "tables", "transformations")
	if err := filepath.Walk(datapipelinesRoot, visitDatapipeline); err != nil {
		fmt.Printf("error walking the path %v: %v\n", datapipelinesRoot, err)
	}

	reportsRoot := filepath.Join(filepathhelpers.BackendRoot, "python3", "samsaradev", "infra", "dataplatform", "reports")
	if err := filepath.Walk(reportsRoot, visitReport); err != nil {
		fmt.Printf("error walking the path %v: %v\n", reportsRoot, err)
	}
}
