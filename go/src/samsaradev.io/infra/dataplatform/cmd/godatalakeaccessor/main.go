package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"go.uber.org/fx"

	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dataplatform/godatalake"
	"samsaradev.io/system"
)

// godatalakeaccessor is a test script for validating the godatalake client
// functionality. It connects to the godatalake Databricks SQL warehouse (ID:
// 504682c42035cf05) and executes a sample query against our internal test
// database (dataplatformtestinternal_shard_5db).
//
// To run this application:
//   cd go/src/samsaradev.io/infra/dataplatform/cmd/godatalakeaccessor
//   DBX_TOKEN=<your-databricks-token> go run .
//
// The app will:
// 1. Connect to the warehouse using the godatalake client
// 2. Execute a query against the test 'notes' table
// 3. Parse and print the results to stdout, showing note records with their
// IDs, org IDs, content and creation timestamps.
//
// You can monitor query execution and performance in the Databricks SQL UI at:
// https://samsara-dev-us-west-2.cloud.databricks.com/sql/warehouses/504682c42035cf05/monitoring?o=5924096274798303

type params struct {
	fx.In

	GodataLake *godatalake.GoDataLake
}

func run(p params) error {
	rows, err := p.GodataLake.Client.Query(context.Background(), "SELECT id, org_id, note, created_at FROM dataplatformtestinternal_shard_5db.notes WHERE org_id = ? and note LIKE ?", 33026, "random note 2%")
	if err != nil {
		fmt.Println("Error querying:", err)
		return err
	}
	defer rows.Close()

	type note struct {
		Id        int
		OrgId     int
		Note      string
		CreatedAt time.Time
	}
	results := []*note{}
	for rows.Next() {
		var r note
		if err := rows.Scan(&r.Id, &r.OrgId, &r.Note, &r.CreatedAt); err != nil {
			errMsg := fmt.Sprintf("%v. couldn't parse row %+v", err, rows)
			fmt.Println(errMsg)
			continue
		}
		results = append(results, &r)
	}
	if len(results) == 0 {
		return nil
	}

	for _, note := range results {
		fmt.Printf("{Id: %d, OrgId: %d, Note: %q, CreatedAt: %v}\n",
			note.Id, note.OrgId, note.Note, note.CreatedAt)
	}
	return nil
}

func main() {
	// If the user doesn't specify an AWS region, use us-west-2.
	awsRegion := flag.String("AWS_REGION", "us-west-2", "AWS region to use (default: us-west-2)")
	flag.Parse()
	os.Setenv("AWS_REGION", *awsRegion)

	// If DBX_TOKEN is set, use it as the Databricks token.
	if token := os.Getenv("DBX_TOKEN"); token != "" {
		// Set DBX_TOKEN as SECRET_app_databricks_e2_api_token, which is what
		// secrets manager expects when we run in dev mode.
		os.Setenv("SECRET_app_databricks_e2_api_token", token)
	}
	os.Setenv("RUN_IN_DEV", "true")

	app := system.NewFx(
		&config.ConfigParams{},
		// Optional: Override the warehouse ID.
		// fx.Provide(func() *godatalake.ConfigOverrides {
		// 	return &godatalake.ConfigOverrides{
		// 		WarehouseID: "custom-warehouse-id", // Replace with your warehouse ID
		// 	}
		// }),
		fx.Invoke(run),
	)

	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		fmt.Println("Error starting app:", err)
		return
	}
	if err := app.Stop(ctx); err != nil {
		fmt.Println("Error stopping app:", err)
	}
}
