package datastreamlake

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"samsaradev.io/infra/dataplatform/datastreamlake/datastreamtime"
)

type validTestStruct struct {
	Val1 int                 `json:"val1" datalakemetadata:"Represents the ID of the organization."`
	Val2 string              `json:"val2"`
	Val3 datastreamtime.Time `json:"val3"`
	Val4 int64               `json:"val4"`
	Val5 float32             `json:"val5"`
	Val6 []string            `json:"val6"`
	Val7 []byte              `json:"val7"`
	Val8 map[string]string   `json:"val8"`
}

type invalidTestStructWithUnsupportedType struct {
	Val1 int                   `json:"val1"`
	Val2 string                `json:"val2"`
	Val3 randomUnsupportedType `json:"val3"`
}

type randomUnsupportedType struct{}

type invalidTestStructWithMissingJsonTag struct {
	Val1 int `json:"val1"`
	Val2 string
}

type invalidTestStructWithEmptyJsonTag struct {
	Val1 int    `json:"val1"`
	Val2 string `json:",omitempty"`
}

type invalidTestStructWithTimeType struct {
	Val1 int       `json:"val1"`
	Val2 string    `json:"val2"`
	Val3 time.Time `json:"val3"`
	Val4 int64     `json:"val4"`
	Val5 float32   `json:"val5"`
}

type invalidTestStructWithDateJSONTag struct {
	Val1 int                 `json:"val1"`
	Val2 string              `json:"val2"`
	Val3 datastreamtime.Time `json:"date"`
}

type invalidTestStructWithDuplicateJSONTags struct {
	Val1 int    `json:"val1"`
	Val2 string `json:"val1"`
}

func TestGoStructToGlueSchema(t *testing.T) {
	testCases := map[string]struct {
		testStruct  interface{}
		shouldErr   bool
		errContains string
	}{
		"validStruct": {
			testStruct: validTestStruct{},
			shouldErr:  false,
		},
		"invalidTestStructWithUnsupportedType": {
			testStruct: invalidTestStructWithUnsupportedType{},
			shouldErr:  true,
		},
		"invalidTestStructWithMissingJsonTag": {
			testStruct: invalidTestStructWithMissingJsonTag{},
			shouldErr:  true,
		},
		"invalidTestStructWithEmptyJsonTag": {
			testStruct: invalidTestStructWithEmptyJsonTag{},
			shouldErr:  true,
		},
		"invalidTestStructWithTimeType": {
			testStruct:  invalidTestStructWithTimeType{},
			shouldErr:   true,
			errContains: "time.Time type found in Record struct. Please use datastreamtime.Time when declaring timestamp types. This is needed for proper timestamp serialization by Firehose.",
		},
		"invalidTestStructWithDateJSONTag": {
			testStruct:  invalidTestStructWithDateJSONTag{},
			shouldErr:   true,
			errContains: "Invalid JSON tag date. Date can not be a JSON tag because it will map to a data lake column name and Firehose partitions the data by date so you will get date for free.",
		},
		"invalidTestStructWithDuplicateJSONTags": {
			testStruct:  invalidTestStructWithDuplicateJSONTags{},
			shouldErr:   true,
			errContains: "JSON tag val1 appears multiple times which will cause collisions in the data lake table column names. Please use unique JSON tags on all fields.",
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			res, err := GoStructToGlueSchema(testCase.testStruct)
			if testCase.shouldErr {
				assert.Nil(t, res)
				assert.Error(t, err)
				if testCase.errContains != "" {
					assert.Contains(t, err.Error(), testCase.errContains)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, res)
			}
		})
	}
}
