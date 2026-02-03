package main

import (
	"fmt"
	"log"
	"sort"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/datapipelines/graphs"
)

func main() {
	dags, err := graphs.BuildDAGs()
	if err != nil {
		log.Fatal(oops.Wrapf(err, ""))
	}

	// Sort them alphabetically.
	sort.SliceStable(dags, func(i, j int) bool {
		return dags[i].Name() < dags[j].Name()
	})

	for _, dag := range dags {
		fmt.Println(dag.Name())
	}
}
