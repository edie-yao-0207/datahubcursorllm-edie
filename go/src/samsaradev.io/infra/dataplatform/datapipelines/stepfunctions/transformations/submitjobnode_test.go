package transformations

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildParamStringIntrinsic(t *testing.T) {
	testCases := map[string]struct {
		Params   []string
		Expected string
	}{
		"nil": {
			Expected: "",
		},
		"empty": {
			Params:   []string{},
			Expected: "",
		},
		"single_param": {
			Params:   []string{"$.myparam"},
			Expected: "States.Array($.myparam)",
		},
		"multiple_params": {
			Params:   []string{"--start-date", "$.start_date", "a", "b", ""},
			Expected: "States.Array('--start-date', $.start_date, 'a', 'b', '')",
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, testCase.Expected, buildParamStringWithIntrinsic(testCase.Params))
		})
	}
}
