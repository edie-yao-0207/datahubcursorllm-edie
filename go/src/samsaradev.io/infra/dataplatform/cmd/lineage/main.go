package main

import (
	"encoding/json"
	"fmt"
	"log"

	"samsaradev.io/infra/dataplatform/amundsen/metadatagenerator"
)

// This tool can be used to generate our lineage data and output the JSON locally
// Helpful if you don't want to download from S3, or you want to view the lineage locally
// with your branch's changes
//
//	go run .
func main() {
	lineage, err := metadatagenerator.LoadDataLineageMetadata()
	if err != nil {
		log.Fatal("error generating lineage", err)
	}

	lineageBytes, err := json.Marshal(lineage)
	if err != nil {
		log.Fatal("error marshaling lineage", err)
	}

	fmt.Print(string(lineageBytes))
}
