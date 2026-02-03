package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/dataplatform/datapipelines/configvalidator"
	"samsaradev.io/infra/dataplatform/datastreamlake"
	"samsaradev.io/infra/dataplatform/dynamodbdeltalake"
	"samsaradev.io/infra/dataplatform/ksdeltalake"
	"samsaradev.io/infra/dataplatform/rdsdeltalake"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/unitycatalog/databaseregistries"
)

type TableType string

const (
	DataPipelineNodeTableType    TableType = "data_pipeline_node"
	DatastreamsTableType         TableType = "datastreams"
	DatastreamsHistoryTableType  TableType = "datastreams_history"
	DatastreamsSchemaTableType   TableType = "datastreams_schema"
	DynamodbTableType            TableType = "dynamodb"
	KinesisStatsTableType        TableType = "kinesisstats"
	KinesisstatsHistoryTableType TableType = "kinesisstats_history"
	RDSTableType                 TableType = "rds"
	RDSPostgresTableType         TableType = "rds_postgres"
	S3BigStatsTableType          TableType = "s3bigstats"
	S3BigStatsHistoryTableType   TableType = "s3bigstats_history"
)

var CsvHeader = []string{"db", "table", "production", "table_type", "refresh_schedule"}

func main() {
	folder := filepath.Join(filepathhelpers.BackendRoot, "dataplatform/tables/s3data/dataplatform/")
	err := os.MkdirAll(folder, 0777)
	if err != nil {
		log.Fatal(err, "failed to create directories")
	}

	createTableClassifications(folder)
	createDatabaseOwners(folder)

}
func createDatabaseOwners(baseFolder string) {
	// Create rows with the following schema:
	// db, owner

	rows := [][]string{}

	config, err := dataplatformconfig.DatabricksProviderConfig(dataplatformconfig.DatabricksDevUsProviderGroup)
	if err != nil {
		log.Fatal(err, "failed to get databricks config")
	}

	for _, db := range databaseregistries.GetAllDatabases(config, databaseregistries.AllUnityCatalogDatabases) {
		team := "Unknown"
		if db.OwnerTeamName != "" {
			team = db.OwnerTeamName
		} else if db.OwnerTeam.TeamName != "" {
			team = db.OwnerTeam.TeamName
		}
		rows = append(rows, []string{db.Name, team})
	}

	// Sort the entries by database for consistency
	sort.Slice(rows, func(i, j int) bool {
		return strings.Compare(rows[i][0], rows[j][0]) <= 0
	})

	// Write generated file
	file, err := os.Create(filepath.Join(baseFolder, "database_owners.csv"))
	if err != nil {
		log.Fatal(err, "failed to create csv file")
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	for _, record := range rows {
		err := writer.Write(record)
		if err != nil {
			log.Fatal(err, "failed to write row to csv file")
		}
	}

	// Flush any buffered data to ensure all data is written to the file
	writer.Flush()

	if err := writer.Error(); err != nil {
		panic(err)
	}
}

func createTableClassifications(baseFolder string) {
	// Create rows with the following schema:
	// db, table, production, table_type, refresh_schedule.
	rows := [][]string{}

	for _, table := range ksdeltalake.AllTables() {
		row := []string{"kinesisstats", table.Name, fmt.Sprintf("%v", table.Production), string(KinesisStatsTableType), fmt.Sprintf("%v", table.GetReplicationFrequencyHour())}
		historyRow := []string{"kinesisstats_history", table.Name, fmt.Sprintf("%v", table.Production), string(KinesisstatsHistoryTableType), fmt.Sprintf("%v", table.GetReplicationFrequencyHour())}
		rows = append(rows, row, historyRow)
	}

	for _, table := range ksdeltalake.AllS3BigStatTables() {
		row := []string{"s3bigstats", table.Name, fmt.Sprintf("%v", table.Production), string(S3BigStatsTableType), fmt.Sprintf("%v", table.GetReplicationFrequencyHour())}
		historyRow := []string{"s3bigstats_history", table.Name, fmt.Sprintf("%v", table.Production), string(S3BigStatsHistoryTableType), fmt.Sprintf("%v", table.GetReplicationFrequencyHour())}
		rows = append(rows, row, historyRow)
	}

	var dbs = rdsdeltalake.AllDatabases()
	for _, db := range dbs {
		dbname := db.Name
		if db.Sharded {
			dbname = dbname + "_shards"
		}

		var tables []rdsdeltalake.RegistryTable
		for _, table := range db.Tables {
			tables = append(tables, table)
		}
		for _, table := range tables {
			table_type := string(RDSTableType)
			if db.InternalOverrides.AuroraDatabaseEngine.IsPostgres() {
				table_type = string(RDSPostgresTableType)
			}
			row := []string{dbname, table.TableName, fmt.Sprintf("%v", table.Production), table_type, fmt.Sprintf("%v", table.GetReplicationFrequencyHour())}
			rows = append(rows, row)
		}
	}

	dataPipelineNodes, err := configvalidator.ReadNodeConfigurations()
	if err != nil {
		log.Fatal(err, "failed to read data pipeline nodes")
	}
	for _, node := range dataPipelineNodes {
		parts := strings.Split(node.Name, ".")
		if len(parts) != 2 {
			log.Fatal("node name looks incorrect", parts)
		}
		dbName := parts[0]
		tableName := parts[1]

		// datapipeline nodes are scheduled every 3 hours and replication_schedule is set to that.
		row := []string{dbName, tableName, fmt.Sprintf("%v", !node.NonProduction), string(DataPipelineNodeTableType), "3"}
		rows = append(rows, row)
	}

	for _, table := range dynamodbdeltalake.AllTables() {
		rows = append(
			rows,
			[]string{
				"dynamodb",
				table.TableName,
				fmt.Sprintf("%v", table.Production),
				string(DynamodbTableType),
				fmt.Sprintf("%v", table.GetReplicationFrequencyHour()),
			},
		)
	}

	for _, table := range datastreamlake.Registry {
		row := []string{"datastreams", table.StreamName, "false", string(DatastreamsTableType), "0.5"}
		historyRow := []string{"datastreams_history", table.StreamName, "false", string(DatastreamsHistoryTableType), "0.5"}
		rawRow := []string{"datastreams_schema", table.StreamName, "false", string(DatastreamsSchemaTableType), "0.5"}
		rows = append(rows, row, historyRow, rawRow)
	}

	// Write generated file
	file, err := os.Create(filepath.Join(baseFolder, "table_classifications.csv"))
	if err != nil {
		log.Fatal(err, "failed to create csv file")
	}
	defer file.Close()

	// Create a CSV writer object
	writer := csv.NewWriter(file)

	// sort the rows by db.tbl for consistent output
	sort.Slice(rows, func(i, j int) bool {
		iRowDb := rows[i][0]
		iRowTbl := rows[i][1]
		iRowName := iRowDb + iRowTbl

		jRowDb := rows[j][0]
		jRowTbl := rows[j][1]
		jRowName := jRowDb + jRowTbl

		return iRowName < jRowName
	})

	// Write header to the CSV file.
	// Add the header.
	err = writer.Write(CsvHeader)
	if err != nil {
		log.Fatal(err, "failed to write header row to csv file")
	}
	// Write the data to the CSV file
	for _, record := range rows {
		err := writer.Write(record)
		if err != nil {
			log.Fatal(err, "failed to write row to csv file")
		}
	}

	// Flush any buffered data to ensure all data is written to the file
	writer.Flush()

	if err := writer.Error(); err != nil {
		panic(err)
	}
}
