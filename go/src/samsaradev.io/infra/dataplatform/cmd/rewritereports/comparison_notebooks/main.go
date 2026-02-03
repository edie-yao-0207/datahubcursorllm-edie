package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/dataplatform/cmd/rewritereports"
)

func processFile(path string) error {
	tables := rewritereports.GetTablesToReplace()

	// Get original sqlfile
	origBytes, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	orig := string(origBytes)

	// Get updated
	updatedSql, err := rewritereports.BuildUpdatedSql(path, tables, rewritereports.Deprecated)
	if err != nil {
		return err
	}

	// Write a notebook to compare
	if updatedSql == orig {
		fmt.Printf("%s path has no changes\n", path)
	} else {
		orig = strings.ReplaceAll(orig, "${start_date}", `"2023-08-01"`)
		orig = strings.ReplaceAll(orig, "${end_date}", `"2023-08-03"`)
		orig = strings.ReplaceAll(orig, "${pipeline_execution_time}", `"2023-08-03T03:59:00Z"`)
		updatedSql = strings.ReplaceAll(updatedSql, "${start_date}", `"2023-08-01"`)
		updatedSql = strings.ReplaceAll(updatedSql, "${end_date}", `"2023-08-03"`)
		updatedSql = strings.ReplaceAll(updatedSql, "${pipeline_execution_time}", `"2023-08-03T03:59:00Z"`)
		output := "# Databricks notebook source\n\n"
		output += `# MAGIC %md` + "\n"
		output += `# MAGIC # ` + strings.ReplaceAll(path, filepathhelpers.BackendRoot, "") + "\n\n"
		output += "# COMMAND ----------\n\n"
		output += `import difflib` + "\n\n"
		output += "# COMMAND ----------\n\n"
		output += fmt.Sprintf(`orig_sql = """%s"""`+"\n\n", orig)
		output += "# COMMAND ----------\n\n"
		output += fmt.Sprintf(`new_sql = """%s"""`+"\n\n", updatedSql)
		output += "# COMMAND ----------\n\n"
		output += `diffs = difflib.ndiff(orig_sql.strip().split("\n"), new_sql.strip().split("\n"))` + "\n"
		output += `print("\n".join(diffs))` + "\n\n"
		output += "# COMMAND ----------\n\n"
		output += `orig_df = spark.sql(orig_sql)` + "\n\n"
		output += `new_df = spark.sql(new_sql)` + "\n\n"
		output += "# COMMAND ----------\n\n"
		output += `orig_count = orig_df.count()` + "\n"
		output += `orig_schema = orig_df.schema` + "\n"
		output += `print(orig_count)` + "\n"
		output += `print(orig_schema)` + "\n"
		output += "# COMMAND ----------\n\n"
		output += `new_count = new_df.count()` + "\n"
		output += `new_schema = new_df.schema` + "\n"
		output += `print(new_count)` + "\n"
		output += `print(new_schema)` + "\n"
		output += "# COMMAND ----------\n\n"
		output += `print("schemas equal: ", orig_schema == new_schema)` + "\n"
		output += `print("counts same: ", orig_count == new_count)` + "\n\n"
		output += "# COMMAND ----------\n\n" // hack for combining

		dir := filepath.Dir(path)
		_, fileName := filepath.Split(path)
		_, folderName := filepath.Split(strings.TrimSuffix(dir, string(filepath.Separator)))
		newpath := fmt.Sprintf("rewritten/%s_%s", folderName, fileName)

		err = os.WriteFile(newpath+".parquet.py", []byte(output), 0644)
		if err != nil {
			return err
		}
	}

	fmt.Println("Processed:", path)
	return nil
}

func visit(path string, f os.FileInfo, err error) error {
	if err != nil {
		fmt.Printf("error visiting path %v: %v\n", path, err)
		return nil
	}

	if !f.IsDir() && filepath.Ext(path) == ".sql" && !strings.HasSuffix(path, ".expectation.sql") {
		if err := processFile(path); err != nil {
			fmt.Printf("error processing file %v: %v\n", path, err)
		}
	}
	return nil
}

func main() {
	root := filepath.Join(filepathhelpers.BackendRoot, "dataplatform", "tables", "transformations")
	if err := filepath.Walk(root, visit); err != nil {
		fmt.Printf("error walking the path %v: %v\n", root, err)
	}
}
