package datastreamlake

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/dataplatform/datastreamlake/datastreamtime"
	"samsaradev.io/infra/dataplatform/sparktypes"
)

func TestDescriptions(t *testing.T) {
	// This test verifies all new streams are populated with a description to enforce proper metadata management
	// The goal of this test is that is should be removed at some point
	grandfatheredStreams := map[string]struct{}{
		"cable_selection_logs": {},
	}

	for _, stream := range Registry {
		t.Run(fmt.Sprintf("Data Stream %s description check", stream.StreamName), func(t *testing.T) {
			if _, ok := grandfatheredStreams[stream.StreamName]; !ok {
				if stream.Description == "" {
					t.Fatalf("Please populate a Description field for the stream entry in the registry for %s", stream.StreamName)
				}
			} else {
				if stream.Description != "" {
					t.Fatalf("Looks like %s has a description! Please remove it from grandfatheredStreams in datastreamlake/registry_test.go::TestDescriptions.", stream.StreamName)
				}
			}
		})
	}
}

func TestValidateRegistry(t *testing.T) {
	assert.NoError(t, validateRegistry())
}

func TestDataStreamTimeMarshal(t *testing.T) {
	testCases := []struct {
		timestamp          time.Time
		expectedMarshalStr string
	}{
		{
			timestamp:          time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
			expectedMarshalStr: `"2000-01-01T00:00:00Z"`,
		},
		{
			timestamp:          time.Date(2020, 6, 25, 12, 35, 18, 123456789, time.UTC),
			expectedMarshalStr: `"2020-06-25T12:35:18.123456789Z"`,
		},
		{
			timestamp:          time.Date(2010, 9, 3, 22, 49, 1, 123450000, time.UTC),
			expectedMarshalStr: `"2010-09-03T22:49:01.12345Z"`,
		},
	}

	for idx, testCase := range testCases {
		t.Run(fmt.Sprintf("test_%d", idx), func(t *testing.T) {
			bytes, err := json.Marshal(datastreamtime.Time(testCase.timestamp))
			require.NoError(t, err)
			assert.Equal(t, testCase.expectedMarshalStr, string(bytes))
		})
	}
}

func TestIsValidStreamName(t *testing.T) {
	testCases := []struct {
		inputStr          string
		isValidStreamName bool
	}{
		{
			inputStr:          "1234567_invalid_stream_name_starting_with_digits",
			isValidStreamName: false,
		},
		{
			inputStr:          "InValid_sTream_NamE_wITH_capITaL_LeTTers",
			isValidStreamName: false,
		},
		{
			inputStr:          "invalid..snakecase??with!!puncuntuation--",
			isValidStreamName: false,
		},
		{
			inputStr:          "validsnakecasewithnounderscores",
			isValidStreamName: true,
		},
		{
			inputStr:          "valid_snake_case_with_under_scores",
			isValidStreamName: true,
		},
		{
			inputStr:          "valid_stream_name_99999_with_digits",
			isValidStreamName: true,
		},
		{
			inputStr:          "a_stream_name_with_the_right_amount_of_chars",
			isValidStreamName: true,
		},
		{
			inputStr:          "stream_name_that_has_just_too_many_characters",
			isValidStreamName: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.inputStr, func(t *testing.T) {
			assert.Equal(t, testCase.isValidStreamName, IsValidStreamName(testCase.inputStr))
		})
	}
}

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

// This translates strings from camel to lower case
// eg deviceId --> device_id
func toSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

func TestPartitionColumns(t *testing.T) {
	for _, datastream := range Registry {
		sparkType, err := sparktypes.JsonTypeToSparkType(reflect.TypeOf(datastream.Record), sparktypes.ConversionParams{})
		assert.NoError(t, err)
		dataStreamColumnNames := make(map[string]struct{}, len(sparkType.Fields))
		for _, col := range sparkType.Fields {
			dataStreamColumnNames[col.Name] = struct{}{}
		}

		hasDatePartition := false
		if datastream.PartitionStrategy != nil {
			for _, partitionCol := range datastream.PartitionStrategy.PartitionColumns {
				if partitionCol == "date" {
					hasDatePartition = true
					continue
				}
				// Error if the partition column isn't a column on the table
				if _, ok := dataStreamColumnNames[partitionCol]; !ok {
					fmt.Println(dataStreamColumnNames)
					assert.Fail(t, fmt.Sprintf("Partition column %s does not exist on the Data Stream %s", partitionCol, datastream.StreamName))
				}
			}

			// Error if the data stream is not partitioned by date.
			if !hasDatePartition {
				assert.Fail(t, fmt.Sprintf("Missing date partition in partition strategy columns for data stream %s", datastream.StreamName))
			}

			dateOverride := datastream.PartitionStrategy.DateColumnOverride
			dateExpression := datastream.PartitionStrategy.DateColumnOverrideExpression
			if dateOverride != "" && dateExpression != "" {
				assert.Fail(t, fmt.Sprintf("Date override %s and date override expression cant be both set on the Data Stream %s", dateOverride, datastream.StreamName))
			}

			if dateOverride != "" {
				if _, ok := dataStreamColumnNames[dateOverride]; !ok {
					assert.Fail(t, fmt.Sprintf("Date override %s for the date partition columns does not exist on the Data Stream %s", dateOverride, datastream.StreamName))
				}
			}

		}
	}
}
