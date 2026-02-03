package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/samsarahq/go/oops"
	"go.uber.org/fx"

	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dataplatform/godatalake"
	"samsaradev.io/system"
)

type schemaEntry struct {
	Name     string `json:"name"`
	DataType string `json:"type"`
}

type jsonMetadata struct {
	Table       string        `json:"table"`
	Owner       string        `json:"owner"`
	Description string        `json:"description"`
	Database    string        `json:"database"`
	Schema      []schemaEntry `json:"schema"`
}

func pathExists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

func constructMetdata(rows godatalake.Result, tableName string, dbName string, tableOwner string, tableDescr string) jsonMetadata {
	var schemas []schemaEntry
	for rows.Next() {
		var colName string
		var dataType string
		var comment sql.NullString
		if err := rows.Scan(&colName, &dataType, &comment); err != nil {
			log.Fatal(oops.Wrapf(err, "Unable to scan row for colName, dataType, and comment columns."))
		}
		if colName != "" && colName != "# Partitioning" && colName != "Not partitioned" {
			newSchema := schemaEntry{
				Name:     colName,
				DataType: dataType,
			}
			schemas = append(schemas, newSchema)
		}
	}

	metadataJson := jsonMetadata{
		Table:       tableName,
		Owner:       tableOwner,
		Description: tableDescr,
		Database:    dbName,
		Schema:      schemas,
	}

	return metadataJson
}

func main() {
	dbTableName := os.Args[1]
	dbName := strings.Split(dbTableName, ".")[0]
	tableName := strings.Split(dbTableName, ".")[1]
	tableOwner := os.Args[2]
	tableDescr := os.Args[3]

	// Set up development environment
	os.Setenv("RUN_IN_DEV", "true")

	type jsonOwnerAndDescription struct {
		Owner       string `json:"owner"`
		Description string `json:"description"`
	}

	var ownerAndDescr jsonOwnerAndDescription
	if pathExists(os.ExpandEnv(fmt.Sprintf("$BACKEND_ROOT/dataplatform/tables/external_tables/%s/%s.json", dbName, tableName))) == true {
		data, err := ioutil.ReadFile(os.ExpandEnv(fmt.Sprintf("$BACKEND_ROOT/dataplatform/tables/external_tables/%s/%s.json", dbName, tableName)))
		if err != nil {
			log.Fatal(oops.Wrapf(err, "Error reading from existing json file for table"))
		}
		err = json.Unmarshal(data, &ownerAndDescr)
		if err != nil {
			log.Fatal(oops.Wrapf(err, "Error unmarhsalling json from file"))
		}
	}

	if tableOwner == "" {
		tableOwner = ownerAndDescr.Owner
		if tableOwner == "" {
			fmt.Println("The table owner information is empty, please provide owner information as a string using the TABLE_OWNER flag!")
			return
		}
	}

	if tableDescr == "" {
		tableDescr = ownerAndDescr.Description
		if tableDescr == "" {
			fmt.Println("The table description information is empty, please provide description information as a string using the TABLE_DESCRIPTION flag!")
			return
		}
	}

	/* TODO: Uncomment after initial testing phase
	This checks to see if the schema files for a table already exist in sqlview and s3tables because,
	in the future, we dont want the schema files for the same table in multiple locations.
	It is commented out right now initially because there are still some frameworks that use the schemas in s3data and sqlview and it will take a while to migrate.

	if pathExists(os.ExpandEnv(fmt.Sprintf("$BACKEND_ROOT/dataplatform/tables/sqlview/%s/%s.json", dbName, tableName))) == true ||
		pathExists(os.ExpandEnv(fmt.Sprintf("$BACKEND_ROOT/dataplatform/tables/s3tables/%s/%s.json", dbName, tableName))) == true {
			fmt.Println("Json file already exists for this table.")
			return
	}
	*/

	var godatalake *godatalake.GoDataLake
	ctx := context.Background()

	app := system.NewFx(
		&config.ConfigParams{},
		fx.Populate(&godatalake),
	)

	if err := app.Start(ctx); err != nil {
		log.Fatal(oops.Wrapf(err, "Failed to start Fx app"))
	}
	defer app.Stop(ctx)

	query := fmt.Sprintf("describe %s", dbTableName)
	rows, err := godatalake.Client.Query(ctx, query)
	if err != nil {
		log.Fatal(oops.Wrapf(err, "Error querying databricks for describing table. Check your connection to databricks and then the table you are querying. If you're running locally, check that your databricks_api_token is set in your secrets file."))
	}
	defer rows.Close()

	metadataJson := constructMetdata(rows, tableName, dbName, tableOwner, tableDescr)
	directoryName := os.ExpandEnv(fmt.Sprintf("$BACKEND_ROOT/dataplatform/tables/external_tables/%s", dbName))
	if !pathExists(directoryName) {
		err = os.Mkdir(directoryName, 0777)
		if err != nil {
			log.Fatal(oops.Wrapf(err, "Error creating database directory in external_tables"))
		}
	}

	f, err := os.Create(os.ExpandEnv(fmt.Sprintf("$BACKEND_ROOT/dataplatform/tables/external_tables/%s/%s.json", dbName, tableName)))
	if err != nil {
		log.Fatal(oops.Wrapf(err, "Failed to create/open json file."))
	}
	defer f.Close()

	jsonDump, err := json.MarshalIndent(metadataJson, "", "  ")
	if err != nil {
		log.Fatal(oops.Wrapf(err, "MarshalIndent on json metadata failed"))
	}
	_, err = f.WriteString(fmt.Sprintf("%s\n", strings.Replace(string(jsonDump), "\"int\"", "\"bigint\"", -1)))

	if err != nil {
		log.Fatal(oops.Wrapf(err, "Failed to write metadata to json file."))
	}
}
