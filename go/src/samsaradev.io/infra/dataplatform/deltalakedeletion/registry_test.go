package deltalakedeletion

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

var TestCases = map[string][]TableDeletionSpec{
	"us-west-2": AllUSDeletionRegistry,
	"eu-west-1": AllEUDeletionRegistry,
}

func TestValidateDatabase(t *testing.T) {

	var invalidDatabases = map[string]struct{}{
		"playground": struct{}{},
	}

	for name, testCase := range TestCases {
		t.Run(name, func(t *testing.T) {
			for _, spec := range testCase {
				if _, ok := invalidDatabases[spec.Database]; ok {
					assert.Fail(t, fmt.Sprintf("Database is not valid. cannot be any of: %v.", reflect.ValueOf(invalidDatabases).MapKeys()))
				}
			}
		})
	}
}

func TestValidateCustomerIdentificationColumn(t *testing.T) {

	var validCustomerIdentificationColumns = map[string]struct{}{
		"org_id":          struct{}{},
		"orgId":           struct{}{},
		"OrgId":           struct{}{},
		"organization_id": struct{}{},
		"group_id":        struct{}{},
		"sam_number":      struct{}{},
		"customerid":      struct{}{},
	}

	for name, testCase := range TestCases {
		t.Run(name, func(t *testing.T) {
			for _, spec := range testCase {
				if _, ok := validCustomerIdentificationColumns[spec.CustomerIdentificationColumn]; !ok {
					assert.Fail(t, fmt.Sprintf("CustomerIdentificationColumn is not valid. must be one of: %v.", reflect.ValueOf(validCustomerIdentificationColumns).MapKeys()))
				}
			}
		})
	}
}

func TestValidateGroup(t *testing.T) {
	var validGroups = map[string]struct{}{
		"datapipelines": struct{}{},
		"kinesisstats":  struct{}{},
		"s3bigstats":    struct{}{},
		"datastreams":   struct{}{},
		"notebooks":     struct{}{},
	}

	for name, testCase := range TestCases {
		t.Run(name, func(t *testing.T) {
			for _, spec := range testCase {
				if _, ok := validGroups[spec.Group]; !ok {
					assert.Fail(t, fmt.Sprintf("Group is not valid. must be one of: %v.", reflect.ValueOf(validGroups).MapKeys()))
				}
			}
		})
	}
}

func TestValidateFormat(t *testing.T) {
	var validFormats = map[string]struct{}{
		"delta":   struct{}{},
		"parquet": struct{}{},
		"json":    struct{}{},
		"csv":     struct{}{},
	}

	for name, testCase := range TestCases {
		t.Run(name, func(t *testing.T) {
			for _, spec := range testCase {
				if _, ok := validFormats[spec.Format]; !ok {
					assert.Fail(t, fmt.Sprintf("Format is not valid. must be one of: %v", reflect.ValueOf(validFormats).MapKeys()))
				}
			}
		})
	}
}
