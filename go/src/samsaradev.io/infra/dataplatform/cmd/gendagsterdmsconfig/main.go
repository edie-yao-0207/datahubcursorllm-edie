package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"samsaradev.io/infra/dataplatform/rdsdeltalake/dagsterconfig"
)

func main() {
	// Define command-line flag
	target := flag.String(
		"target",
		"both",
		"Type of DMS config to generate: 'parquet', 'kinesis', or 'both' (default: both)",
	)

	flag.Usage = func() {
		helpText := `%s - Generate Dagster configuration files for RDS replication

USAGE:
  %s [options]

OPTIONS:
`
		fmt.Fprintf(flag.CommandLine.Output(), helpText, "gendagsterdmsconfig", "gendagsterdmsconfig")
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(), `
EXAMPLES:
  # Generate both Parquet and Kinesis DMS configs (default)
  go run go/src/samsaradev.io/infra/dataplatform/cmd/gendagsterdmsconfig/main.go

  # Generate only Parquet DMS configs
  go run go/src/samsaradev.io/infra/dataplatform/cmd/gendagsterdmsconfig/main.go -target=parquet

  # Generate only Kinesis DMS configs
  go run go/src/samsaradev.io/infra/dataplatform/cmd/gendagsterdmsconfig/main.go -target=kinesis

OUTPUT:
  The generated configs are written to:
  - Parquet: go/src/samsaradev.io/infra/dataplatform/rdsdeltalake/dagsterconfig/generated/{region}/dagster_config.json
  - Kinesis: go/src/samsaradev.io/infra/dataplatform/rdsdeltalake/dagsterconfig/generated/{region}/kinesis_dms_dagster_config.json

  Note: Kinesis configs are only generated for databases with EnableKinesisStreamDestination enabled.
`)
	}

	flag.Parse()

	targetLower := strings.ToLower(strings.TrimSpace(*target))

	var genParquet, genKinesis bool
	switch targetLower {
	case "parquet":
		genParquet = true
		genKinesis = false
	case "kinesis":
		genParquet = false
		genKinesis = true
	case "both":
		genParquet = true
		genKinesis = true
	default:
		flag.Usage()
		log.Fatalf("Invalid target value: %s. Must be 'parquet', 'kinesis', or 'both'", *target)
	}

	if genParquet {
		fmt.Println("Generating Parquet DMS config...")
		if err := dagsterconfig.GenerateParquetDagsterConfig(); err != nil {
			log.Fatalf("Failed to generate Parquet DMS config: %v", err)
		}
		fmt.Println("Parquet DMS config generated successfully!")
	}

	if genKinesis {
		fmt.Println("Generating Kinesis DMS config...")
		if err := dagsterconfig.GenerateKinesisDagsterConfig(); err != nil {
			log.Fatalf("Failed to generate Kinesis DMS config: %v", err)
		}
		fmt.Println("Kinesis DMS config generated successfully!")
	}
}
