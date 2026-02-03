package metadatahelpers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEnumDescription(t *testing.T) {
	type MyFunEnum int64

	const (
		NotFun MyFunEnum = iota
		VeryFun
		ExtremelyFun
	)

	testCases := map[string]struct {
		enumInput           interface{}
		expectedDescription string
	}{
		"map[int32]string": {
			enumInput: map[int32]string{
				int32(1): "Rick Sanchez",
				int32(2): "Morty Smith",
				int32(3): "Jerry Smith",
			},
			expectedDescription: "dummy enum description \n\n 1: Rick Sanchez \n\n 2: Morty Smith \n\n 3: Jerry Smith \n\n ",
		},
		"map[int]string": {
			enumInput: map[int]string{
				1: "Rick Sanchez",
				2: "Morty Smith",
				3: "Jerry Smith",
			},
			expectedDescription: "dummy enum description \n\n 1: Rick Sanchez \n\n 2: Morty Smith \n\n 3: Jerry Smith \n\n ",
		},
		"map[string]int": {
			enumInput: map[string]int{
				"Rick Sanchez": 1,
				"Morty Smith":  2,
				"Jerry Smith":  3,
			},
			expectedDescription: "dummy enum description \n\n Jerry Smith: 3 \n\n Morty Smith: 2 \n\n Rick Sanchez: 1 \n\n ",
		},
		"map[MyFunEnum]string": {
			enumInput: map[MyFunEnum]string{
				NotFun:       "ew",
				VeryFun:      "whoo",
				ExtremelyFun: "poggy woggy",
			},
			expectedDescription: "dummy enum description \n\n 0: ew \n\n 1: whoo \n\n 2: poggy woggy \n\n ",
		},
	}

	for _, testCase := range testCases {
		actualDescription := EnumDescription("dummy enum description", testCase.enumInput)
		require.Equal(t, testCase.expectedDescription, actualDescription)
	}
}

func TestDeprecatedDescription(t *testing.T) {
	deprecatedDesc := DeprecatedDescription("my dummy deprecated description")
	require.Equal(t, "[DEPRECATED] DO NOT USE. my dummy deprecated description", deprecatedDesc)
}
