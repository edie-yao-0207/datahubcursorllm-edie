package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/dataplatform/datapipelines/graphs"
	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes/dependencytypes"
)

// This regex will match any tables specified as `from XXX` or `join XXX`
var tableClauseRegexp = regexp.MustCompile(`(?i)(from|join)\s+(\w+)\.(\w+)`)

// This script will tell you what reports and what datapipelines depend on a particular table.
// This table can be either a source table like ks/rds, or can be even a datapipeline node.
// For RDS tables, you'll need to write the table exactly as it is used, e.g. safetydb_shards.safety_events
// not safetydb.safety_events.
// Usage:
//
//	go run . -table safetydb_shards.safety_events
func main() {
	var table string
	flag.StringVar(&table, "table", "", "The full name of the table you want to find dependencies for.")
	flag.Parse()

	if table == "" {
		log.Fatal("please provide a table with the -table argument")
	}

	// Collect a mapping from every table to which reports and which datapipelines
	// depend on it.
	tableToReports, err := findReportsByTable()
	if err != nil {
		log.Fatalf("failed to parse report scripts %v\n", err)
	}

	tableToDags, err := dataPipelineReferences()
	if err != nil {
		log.Fatalf("failed to parse datapipelines %v\n", err)
	}

	// Using them, figure out which reports directly use the provided table,
	// or which reports use a datapipeline which uses the dependent table.
	var dependentDags []string
	var dependentReports []string
	if dags, ok := tableToDags[table]; ok {
		dependentDags = append(dependentDags, dags...)
	}

	if reports, ok := tableToReports[table]; ok {
		dependentReports = append(dependentReports, reports...)
	}

	for _, dag := range dependentDags {
		if reports, ok := tableToReports[dag]; ok {
			dependentReports = append(dependentReports, reports...)
		}
	}

	// Print out and deduplicate the list
	reportset := make(map[string]struct{})
	for _, report := range dependentReports {
		reportset[report] = struct{}{}
	}
	fmt.Printf("Reports depending on table %s:\n", table)
	for report := range reportset {
		fmt.Printf("- %s\n", report)
	}

	dagset := make(map[string]struct{})
	for _, dag := range dependentDags {
		dagset[dag] = struct{}{}
	}
	fmt.Printf("\nData Pipelines depending on table %s:\n", table)
	for dag := range dagset {
		fmt.Printf("- %s\n", dag)
	}
}

// For all precomputed reports, figure out which tables they use
func findReportsByTable() (map[string][]string, error) {
	root := filepathhelpers.BackendRoot
	references := make(map[string][]string)

	// Iterate over the reports directory.
	reportsDir := filepath.Join(root, "python3/samsaradev/infra/dataplatform/reports")
	if err := filepath.Walk(reportsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if strings.HasPrefix(filepath.Base(path), ".") {
			return nil
		}
		if !strings.HasSuffix(path, ".sql") {
			return nil
		}

		b, err := ioutil.ReadFile(path)
		if err != nil {
			return oops.Wrapf(err, "read: %s", path)
		}

		// In each reports sql script, find table references and deduplicate.
		matches := tableClauseRegexp.FindAllStringSubmatch(string(b), -1)

		tables := make(map[string]struct{})
		for _, match := range matches {
			table := strings.ToLower(fmt.Sprintf("%s.%s", match[2], match[3]))
			tables[table] = struct{}{}
		}

		// To get the report name, we need to look at the folder it's in
		// which is the second to last part of the path, e.g.
		// python3/samsaradev/infra/dataplatform/reports/adas_beta_report/report.sql
		parts := strings.Split(path, "/")
		reportname := parts[len(parts)-2]

		// Add this report to the list of reports affected by this table.
		for table := range tables {
			references[table] = append(references[table], reportname)
		}

		return nil
	}); err != nil {
		return nil, oops.Wrapf(err, "walk reports: %s", reportsDir)
	}

	return references, nil
}

func dataPipelineReferences() (map[string][]string, error) {
	dagReferences := make(map[string][]string)

	dags, err := graphs.BuildDAGs()
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	for _, dag := range dags {
		for _, node := range dag.GetNodes() {
			for _, dep := range node.Dependencies() {
				if tableDep, ok := dep.(*dependencytypes.TableDependency); ok {
					table := strings.ToLower(fmt.Sprintf("%s.%s", tableDep.DBName, tableDep.TableName))
					dagReferences[table] = append(dagReferences[table], dag.Name())
				}
			}
		}
	}

	return dagReferences, nil
}
