// Package main provides the entry point for the dataplatops CLI tool.
//
// dataplatops is the Data Platform operational automation CLI. It provides
// a thin wrapper around the reusable dataplatops library, adding:
//   - Command-line flag parsing
//   - Interactive confirmation prompts
//   - Dry-run visualization
//
// For programmatic access, import the library directly:
//
//	import "samsaradev.io/infra/dataplatform/dataplatops/aws"
package main

import (
	"samsaradev.io/infra/dataplatform/cmd/dataplatops/cmd"
)

func main() {
	cmd.Execute()
}
