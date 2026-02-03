package registrytest

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/dataplatform/datapipelines/graphs"
	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes/dependencytypes"
)

var tableClauseRegexp = regexp.MustCompile(`(?i)(from|join)\s+(\w+)\.(\w+)`)

func FindProductionTableReferences() (map[string][]string, error) {
	root := filepathhelpers.BackendRoot
	references := make(map[string][]string)

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

		matches := tableClauseRegexp.FindAllStringSubmatch(string(b), -1)

		tables := make(map[string]struct{})
		for _, match := range matches {
			table := strings.ToLower(fmt.Sprintf("%s.%s", match[2], match[3]))
			tables[table] = struct{}{}
		}

		for table := range tables {
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return oops.Wrapf(err, "filepath.Rel: %s, %s", root, path)
			}
			references[table] = append(references[table], rel)
		}

		return nil
	}); err != nil {
		return nil, oops.Wrapf(err, "walk reports: %s", reportsDir)
	}

	dags, err := graphs.BuildDAGs()
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	for _, dag := range dags {
		for _, node := range dag.GetNodes() {

			// We don't care about tables that are only used in non production nodes
			if node.NonProduction() {
				continue
			}

			for _, dep := range node.Dependencies() {
				if !dep.IsExternalDep() {
					// RDS and KS tables must be external dependencies.
					continue
				}
				if tableDep, ok := dep.(*dependencytypes.TableDependency); ok {
					table := strings.ToLower(fmt.Sprintf("%s.%s", tableDep.DBName, tableDep.TableName))

					// Guess the metadata file path based on node name.
					path := fmt.Sprintf("dataplatform/tables/transformations/%s/%s.json",
						strings.SplitN(node.Name(), ".", 2)[0],
						strings.SplitN(node.Name(), ".", 2)[1],
					)
					references[table] = append(references[table], path)
				}
			}
		}
	}

	return references, nil
}
