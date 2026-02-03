package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"samsaradev.io/reports/sparkreportregistry"
)

// This script exists to diff the stored sqlite files for 2 reports.
// This is being used in the migration process for bringing sqlite reports onto data pipelines.
// See: https://paper.dropbox.com/doc/Insert-Report-Name-Here-Spark-Report-Migration-Checklist--BLCVo3u~XvknFUIBr_GUuPCIAg-kQSuyrx8p2jxX0Cg633ms
//
// To use this script:
// 1. Modify the parameters below. Note that we don't really validate any of this, so please be sure it's right.
//    (esp the primary key and non primary key columns)
// 2. After that, run the script, i.e. `go run infra/dataplatform/cmd/sqlitediff/main.go` from the
//    samsaradev.io folder.
// 3. The script should guide you from there on how to download the files / diff them.

// Notes:
//  1. Diffing multiple months will require running it multiple times, once for each month.
//  2. Detailed reports take a little bit of work to get right; I've left some comments
//     as to what you'll need to do, but since we probably won't need to use this again
//     I've opted not to put work into productionizing it.
func main() {
	fmt.Printf("Hello! Beginning the diff process...\n")

	/**
	 * Parameters:
	 * You'll want to change these for your report.
	 */
	// Name of the old and migrated reports
	oldReportName := "ifta_mileage"
	newReportName := "ifta_mileage_temp"

	// Primary and non primary key columns.
	// TODO: I bet we could figure these out just from the report name.
	// For aggregate reports, it's month, duration, interval_start and then the primary keys
	// For detailed reports, its the same as the primary keys of the detailed report.
	pkColumns := []string{"month", "org_id", "duration_ms", "interval_start", "device_id", "jurisdiction"}

	// get the select columns from the report
	// TODO: this won't work for detailed reports, you'll need to use get the report by the regular name,
	// but select the `DetailedSelectColumns` rather than the `SelectColumns`
	report, _ := sparkreportregistry.ByName("ifta_mileage")
	var nonPkColumns []string
	for _, col := range report.SelectColumns {
		nonPkColumns = append(nonPkColumns, col.ColumnName)
	}

	// This is the dir in which the downloaded sqlite files will live.
	baseDir := "ifta_mileage"

	// Start and End ms for comparison.
	var startMs int64 = 1652054400000 // 2022-05-09 00:00:00 UTC
	var endMs int64 = 1652486400000   // 2022-05-14 00:00:00 UTC
	var month = "2022-05-01"
	/**
	 * End of Parameters
	 */

	origDirPath := fmt.Sprintf("%s/original", baseDir)
	migratedDirPath := fmt.Sprintf("%s/migrated", baseDir)

	// 1: Make sure that the folders exist
	// To be 100% correct, we'd actually need to read the manifest to see what the latest files are,
	// but this should work out in most cases as long as you're sure there's no recent job failure that wrote
	// partial files.
	fmt.Printf("To set up the necessary folders and bring down the sqlite data from s3, you can run these commands:\n")
	fmt.Printf("aws s3 cp s3://samsara-partial-report-aggregation/sqlite/%s/data/month=%s ./%s/original/ --recursive\n", oldReportName, month, baseDir)
	fmt.Printf("aws s3 cp s3://samsara-partial-report-aggregation/sqlite/%s/data/month=%s ./%s/migrated/ --recursive\n", newReportName, month, baseDir)
	fmt.Printf("Note that you should only run them once, and this script won't run them for you since they're quite expensive.\n")
	if _, err := os.Stat(baseDir); err != nil {
		log.Fatal(fmt.Printf("Exiting since baseDir %s doesn't exist\n", baseDir))
	}
	if _, err := os.Stat(origDirPath); err != nil {
		log.Fatal(fmt.Printf("Exiting since origDirPath %s doesn't exist\n", origDirPath))
	}
	if _, err := os.Stat(migratedDirPath); err != nil {
		log.Fatal(fmt.Printf("Exiting since migratedDirPath %s doesn't exist\n", migratedDirPath))
	}

	// 2. Set up an error file that we will write out all the error results to.
	errorFile, err := os.Create(fmt.Sprintf("files_with_diff_%d.txt", time.Now().Unix()))
	if err != nil {
		log.Fatal("Couldn't create temporary files to write output to.")
	}

	// 3. We now want to diff the sqlite files from the old/new reports.
	// The baseDir folder is set up as:
	// orig/
	//   org_id=1/
	//     db.sqlite.snappy
	// migrated/
	//   org_id=1/
	//     db.sqlite.snappy
	//
	// So to diff the reports we need to:
	// - go through every folder in `original`
	// - find the corresponding folder in `migrated`
	// - unzip both sqlite files
	// - run a sqlite command to load both dbs and diff the tables
	filesInOriginal, err := ioutil.ReadDir(origDirPath)
	if err != nil {
		log.Fatalf("Failed to read the original directory %s\n", origDirPath)
	}

	for idx, file := range filesInOriginal {
		if !file.IsDir() {
			fmt.Printf("Not sure what this file is %s. Skipping...\n", file.Name())
		}

		orgDir := file.Name() // this will look like `org_id=728`.
		origDbFile := fmt.Sprintf("%s/%s/db.sqlite.snappy", origDirPath, orgDir)
		migratedDbFile := fmt.Sprintf("%s/%s/db.sqlite.snappy", migratedDirPath, orgDir)

		// See if we can find the corresponding file in the migrated version
		if _, err := os.Stat(migratedDbFile); err == nil {
			// If it exists, unzip the dbs, and compare them.
			origOutputDb := origDirPath + "/" + orgDir + "/db.sqlite"
			if _, oserr := os.Stat(origOutputDb); os.IsNotExist(oserr) {
				err = runSimpleCmd(fmt.Sprintf("snzip -c -d -t framing2 %s > %s", origDbFile, origOutputDb))
				if err != nil {
					writeToFile(errorFile, "%s", "org "+orgDir+" failed to unzip original sqlifile\n\n")
					continue
				}
			}
			migratedOutputDb := migratedDirPath + "/" + orgDir + "/db.sqlite"
			if _, oserr := os.Stat(migratedOutputDb); os.IsNotExist(oserr) {
				err = runSimpleCmd(fmt.Sprintf("snzip -c -d -t framing2 %s > %s", migratedDbFile, migratedOutputDb))
				if err != nil {
					writeToFile(errorFile, "%s", "org "+orgDir+" failed to unzip original sqlifile\n\n")
					continue
				}
			}

			sqliteCommand := makeSqliteCommands(baseDir, oldReportName, newReportName, startMs, endMs, pkColumns, nonPkColumns, orgDir)
			if idx == 0 {
				fmt.Printf("Heres the general query we are running\n%s\n", sqliteCommand)
			}
			output, err := runSqliteCmd("sqlite3", sqliteCommand)
			if err != nil {
				writeToFile(errorFile, "%s", "org "+orgDir+" failed to run sqlite commands\n\n")
				continue
			}

			// Let's delete the unzipped file if querying was successful, since we don't need it anymore
			// (and it can take up a lot of disk space).
			err = os.Remove(origOutputDb)
			if err != nil {
				fmt.Printf("failed to remove %s. %v\n", origOutputDb, err)
			}
			err = os.Remove(migratedOutputDb)
			if err != nil {
				fmt.Printf("failed to remove %s. %v\n", migratedOutputDb, err)
			}
			// If there were no errors, our sqlite commands will just print "done" otherwise there'll be more.
			if strings.TrimSpace(output) != "done" {
				writeToFile(errorFile, "%s", output)
				writeToFile(errorFile, "\n\n")
			}
		} else if os.IsNotExist(err) {
			writeToFile(errorFile, "Could not find matching file in migrated dir for %s\n", orgDir)
			fmt.Printf("Could not find matching file\n")
		} else {
			writeToFile(errorFile, "Something went wrong when trying to read %s from the original dir: %v\n", orgDir, err)
		}

		// Print some status indicator every 100 orgs.
		if idx%100 == 0 {
			percentage := idx * 100 / len(filesInOriginal)
			fmt.Printf("Completed %d%%\n", percentage)
		}
	}

	// 4. Now, just double check that the new migrated dir doesn't have any *extra* folders.
	fmt.Printf("Finished diffing original dir with migrated. Now checking that migrated has no extra folders...")
	filesInMigrated, err := ioutil.ReadDir(migratedDirPath)
	if err != nil {
		log.Fatalf("Failed to read the migrated directory %s\n", migratedDirPath)
	}
	for _, file := range filesInMigrated {
		if !file.IsDir() {
			fmt.Printf("Not sure what this file is %s. Skipping...\n", file.Name())
		}

		orgDir := file.Name() // this looks like `org_id=728`.
		dirInOriginal := fmt.Sprintf("%s/original/%s", baseDir, orgDir)
		if _, err := os.Stat(dirInOriginal); err != nil {
			writeToFile(errorFile, "Found folder %s in the new dir which doesn't exist in the old dir\n", orgDir)
		}
	}
	fmt.Printf("Done! Check %s for any diffs.\n", errorFile.Name())
	_ = errorFile.Close()
}

func runSimpleCmd(cmdString string) error {
	cmd := exec.Command("bash", "-c", cmdString)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func runSqliteCmd(cmdString string, sqliteCommands string) (string, error) {
	cmd := exec.Command("bash", "-c", cmdString)
	cmd.Stdin = strings.NewReader(sqliteCommands)
	outputBytes, err := cmd.CombinedOutput()
	if err != nil {
		return "", err
	}
	outputString := string(outputBytes)
	return outputString, nil
}

func writeToFile(file *os.File, format string, a ...interface{}) {
	output := fmt.Sprintf(format, a...)
	_, err := file.WriteString(output)
	if err != nil {
		fmt.Printf("Failed to write %s to the output file, error %v\n", output, err)
	}
}

// Build the sqlite commands to compare the tables in two DBs.
func makeSqliteCommands(
	baseDir string,
	oldReportName string,
	newReportName string,
	startMs int64,
	endMs int64,
	pkColumns []string,
	nonPkColumns []string,
	orgFolder string) string {

	var buf strings.Builder
	buf.Grow(2000) // i think its always less than this
	buf.WriteString(fmt.Sprintf("attach '%s/original/%s/db.sqlite' as db_orig;\n", baseDir, orgFolder))
	buf.WriteString(fmt.Sprintf("attach '%s/migrated/%s/db.sqlite' as db_migrated;\n\n", baseDir, orgFolder))
	buf.WriteString(".headers ON\n")

	// build a string thats like select {all_pk_columns}, a.nonpkcolumn1, b.nonpkcolumn1, a.nonpkcolumn2....
	selectStatement := "SELECT " + strings.Join(pkColumns, ", ") + ", "
	nonPkSelects := make([]string, 0, len(nonPkColumns)*2)
	for _, col := range nonPkColumns {
		nonPkSelects = append(nonPkSelects, "a."+col+" AS "+col+"_orig")
		nonPkSelects = append(nonPkSelects, "b."+col+" AS "+col+"_migrated")
	}
	selectStatement += strings.Join(nonPkSelects, ", ")

	// build the using join part, looks like using(col1, col2)
	usingStatement := fmt.Sprintf("USING(%s)", strings.Join(pkColumns, ", "))

	// build the where string, which has a condition (where a.col is null or b.col is null or a.col != b.col)
	// for every column
	whereStatement := "WHERE ("
	clauses := []string{}
	for _, col := range nonPkColumns {
		clauses = append(clauses, fmt.Sprintf("((a.%s IS NULL AND b.%s IS NOT NULL) OR (a.%s IS NOT NULL AND b.%s IS NULL) OR a.%s != b.%s)", col, col, col, col, col, col))
	}
	whereStatement += strings.Join(clauses, " OR ")
	whereStatement += ")\n"
	whereStatement += "AND duration_ms = 3600000\n" // we only compare hourly rows
	whereStatement += fmt.Sprintf("AND interval_start >= %d\n", startMs)
	whereStatement += fmt.Sprintf("AND interval_start < %d", endMs)

	// For detailed reports, the above 3 clauses are wrong; you'll want to use the following instead:
	// (replace `start_ms` with the time column of the detailed report if necessary)
	//whereStatement += fmt.Sprintf("AND start_ms >= %d\n", startMs)
	//whereStatement += fmt.Sprintf("AND start_ms < %d", endMs)

	// Sqlite does not support full outer join, which is what we really want here.
	// So we are forced to do `a left join b` and `b left join a`, and union the results.
	buf.WriteString(fmt.Sprintf(`
%s
FROM db_orig.%s a
LEFT JOIN db_migrated.%s b
  %s
%s

UNION

%s
FROM db_migrated.%s b
LEFT JOIN db_orig.%s a
  %s
%s;

.headers OFF

select 'done';
`,
		selectStatement, oldReportName, newReportName, usingStatement, whereStatement,
		selectStatement, newReportName, oldReportName, usingStatement, whereStatement),
	)
	return buf.String()
}
