package configvalidator

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/samsarahq/go/oops"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes/dependencytypes"
)

func TestReadAndValidateAllNodeConfigurationFiles(t *testing.T) {
	_ = filepath.Walk(TransformationDir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() || filepath.Ext(path) != ".json" || strings.HasSuffix(path, ".test.json") {
			return nil
		}

		testName, err := filepath.Rel(fmt.Sprintf("%s/%s", filepathhelpers.BackendRoot, "go/src/samsaradev.io/infra/dataplatform/datapipelines/"), path)
		require.NoError(t, err)

		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			_, err = loadAndValidateNodeConfigurationFile(path)
			assert.NoError(t, err)
		})

		return nil
	})
}

func TestEnsureNoRdsTablesReferenced(t *testing.T) {
	err := filepath.Walk(TransformationDir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() || filepath.Ext(path) != ".json" || strings.HasSuffix(path, ".test.json") {
			return nil
		}

		config, err := loadAndValidateNodeConfigurationFile(path)
		if err != nil {
			return oops.Wrapf(err, "failed to load config file %s", path)
		}
		for _, dependency := range config.Dependencies {

			// There are no other types of dependencies except for table dependencies, so cast this
			// here so we can access the db/table name.
			parsed := dependency.(*dependencytypes.TableDependency)

			// Check that NO rds dependency has the deprecated prefix. This is helping prove that our script works.
			if strings.Contains(parsed.DBName, "db") && strings.HasPrefix(parsed.DBName, "deprecated_") {
				return oops.Errorf("found RDS table dependency in %s without deprecated prefix. Dependency: %v", path, parsed)
			}
		}

		return nil
	})
	assert.NoError(t, err)
}
