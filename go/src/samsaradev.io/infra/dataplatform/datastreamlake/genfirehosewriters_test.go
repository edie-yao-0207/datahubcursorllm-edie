package datastreamlake

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"samsaradev.io/helpers/samtime"
	"samsaradev.io/infra/dataplatform/ksdeltalake"
	"samsaradev.io/infra/dataplatform/sparktypes"
	"samsaradev.io/mobile/mobileappmetrics"
	"samsaradev.io/platform/alerts/alertevents"
)

func TestPascalCase(t *testing.T) {
	testCases := map[string]struct {
		input  string
		output string
	}{
		"noUnderScores": {
			input:  "randomstream",
			output: "Randomstream",
		},
		"oneUnderScore": {
			input:  "random_stream",
			output: "RandomStream",
		},
		"mulitpleUnderScores": {
			input:  "random_stream_for_the_test",
			output: "RandomStreamForTheTest",
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testCase.output, pascalCase(testCase.input))
		})
	}
}

func TestRecordPackagePath(t *testing.T) {
	testCases := []struct {
		inputRecord interface{}
		outputPath  string
	}{
		{
			inputRecord: ksdeltalake.Table{},
			outputPath:  "samsaradev.io/infra/dataplatform/ksdeltalake",
		},
		{
			inputRecord: sparktypes.Type{},
			outputPath:  "samsaradev.io/infra/dataplatform/sparktypes",
		},
		{
			inputRecord: mobileappmetrics.MobileTroyLogEventMetric{},
			outputPath:  "samsaradev.io/mobile/mobileappmetrics",
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("%s.%s", testCase.outputPath, reflect.TypeOf(testCase.inputRecord).Name()), func(t *testing.T) {
			assert.Equal(t, testCase.outputPath, recordPackagePath(testCase.inputRecord))
		})
	}
}

func TestRecordType(t *testing.T) {
	testCases := []struct {
		inputRecord            interface{}
		outputRecordTypeString string
	}{
		{
			inputRecord:            mobileappmetrics.MobileTroyLogEventMetric{},
			outputRecordTypeString: "mobileappmetrics.MobileTroyLogEventMetric",
		},
		{
			inputRecord:            mobileappmetrics.MobileTroyDeviceInfoMetric{},
			outputRecordTypeString: "mobileappmetrics.MobileTroyDeviceInfoMetric",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.outputRecordTypeString, func(t *testing.T) {
			assert.Equal(t, testCase.outputRecordTypeString, recordType(testCase.inputRecord))
		})
	}
}

func TestSortImports(t *testing.T) {
	testCases := map[string]struct {
		inputRecord interface{}
		outputSlice []string
	}{
		"recordPathBeforeStaticImports": {
			inputRecord: alertevents.AlertEventCondition{},
			outputSlice: []string{
				"samsaradev.io/helpers/appenv",
				"samsaradev.io/helpers/dogtag/apptag",
				"samsaradev.io/helpers/monitoring",
				"samsaradev.io/helpers/slog",
				"samsaradev.io/infra/config",
				"samsaradev.io/infra/fxregistry",
				"samsaradev.io/infra/monitoring/datadogutils",
				"samsaradev.io/infra/samsaraaws/awshelpers/awserror",
				"samsaradev.io/infra/samsaraaws/firehoseiface",
				"samsaradev.io/infra/samsaraaws/ni/awsecs",
				"samsaradev.io/libs/ni/pointer",
				"samsaradev.io/platform/alerts/alertevents",
			},
		},
		"recordPathInsideStaticImports": {
			inputRecord: samtime.CivilDate{},
			outputSlice: []string{
				"samsaradev.io/helpers/appenv",
				"samsaradev.io/helpers/dogtag/apptag",
				"samsaradev.io/helpers/monitoring",
				"samsaradev.io/helpers/samtime",
				"samsaradev.io/helpers/slog",
				"samsaradev.io/infra/config",
				"samsaradev.io/infra/fxregistry",
				"samsaradev.io/infra/monitoring/datadogutils",
				"samsaradev.io/infra/samsaraaws/awshelpers/awserror",
				"samsaradev.io/infra/samsaraaws/firehoseiface",
				"samsaradev.io/infra/samsaraaws/ni/awsecs",
				"samsaradev.io/libs/ni/pointer",
			},
		},
		"recordPathAfterStaticImports": {
			inputRecord: mobileappmetrics.MobileTroyLogEventMetric{},
			outputSlice: []string{
				"samsaradev.io/helpers/appenv",
				"samsaradev.io/helpers/dogtag/apptag",
				"samsaradev.io/helpers/monitoring",
				"samsaradev.io/helpers/slog",
				"samsaradev.io/infra/config",
				"samsaradev.io/infra/fxregistry",
				"samsaradev.io/infra/monitoring/datadogutils",
				"samsaradev.io/infra/samsaraaws/awshelpers/awserror",
				"samsaradev.io/infra/samsaraaws/firehoseiface",
				"samsaradev.io/infra/samsaraaws/ni/awsecs",
				"samsaradev.io/libs/ni/pointer",
				"samsaradev.io/mobile/mobileappmetrics",
			},
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testCase.outputSlice, sortImports(testCase.inputRecord))
		})
	}
}
