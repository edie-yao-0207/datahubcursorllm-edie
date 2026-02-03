package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"runtime"

	"github.com/samsarahq/go/oops"
	"github.com/samsarahq/taskrunner/shell"

	"samsaradev.io/infra/dataplatform/datapipelines/graphs"
)

var pipelineName string

func init() {
	flag.StringVar(&pipelineName, "pipeline", "", "The name of pipeline to visualize")
	flag.Parse()
}

func main() {
	ctx := context.Background()

	if pipelineName == "" {
		log.Fatal(oops.Errorf("Please specify a pipeline name using PIPELINE_NAME=<pipeline_name>. If you want a full list of all the pipelines you can visualize run `$ taskrunner datapipelines/list` and pick one from there!"))
	}

	graphVizDotString, err := getGraphVizDotString(pipelineName)
	if err != nil {
		log.Fatal(oops.Wrapf(err, ""))
	}

	if err := shell.Run(ctx, "mkdir -p ./tmp"); err != nil {
		log.Fatal(oops.Wrapf(err, ""))
	}

	dotStdin := shell.Stdin(bytes.NewBufferString(graphVizDotString))
	if err := shell.Run(ctx, "dot -T png -o ./tmp/pipeline.png", dotStdin); err != nil {
		log.Fatal(oops.Wrapf(err, ""))
	}

	openCmd := "xdg-open"
	if runtime.GOOS == "darwin" {
		openCmd = "open"
	}

	err = shell.Run(ctx, fmt.Sprintf("%s ./tmp/pipeline.png", openCmd))
	if err != nil {
		log.Fatal(oops.Wrapf(err, "Error opening ./tmp/pipeline.png with `%s`", openCmd))
	}
}

func getGraphVizDotString(pipelineName string) (string, error) {
	dags, err := graphs.BuildDAGs()
	if err != nil {
		return "", oops.Wrapf(err, "")
	}

	var graphvizStr string
	for _, dag := range dags {
		if dag.Name() == pipelineName {
			graphvizStr = dag.String()
		}
	}

	if graphvizStr == "" {
		return "", oops.Errorf("Pipeline with name %s does not exist", pipelineName)
	}

	return fmt.Sprintf("digraph pipeline {%s}", graphvizStr), nil
}
