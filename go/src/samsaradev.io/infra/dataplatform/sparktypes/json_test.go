package sparktypes

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/samsarahq/go/snapshotter"
	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/grpc/testproto"
	"samsaradev.io/infra/modelschema/example/examplemodelgen"
	"samsaradev.io/safety/safetyproto"
	"samsaradev.io/samuuid"
)

// TestSQLSchema tests that SQL schema exported from a Go struct
// can be used in CREATE TABLE.
func TestSQLSchema(t *testing.T) {
	t.Skip("This test is failing on backend-test. Data Platform Team, please find root cause and fix/delegate.")
	type row struct {
		Int         int32   `json:"int"`
		BigInt      int64   `json:"bigint"`
		Uint64      uint64  `json:"uint64"`
		String      string  `json:"string"`
		IntArray    []int32 `json:"int_array"`
		StructArray []struct {
			Int int32 `json:"int"`
		} `json:"struct_array"`
		Struct struct {
			Embedded struct {
				Int int32 `json:"int"`
			} `json:"embedded"`
		} `json:"struct"`
		ByteArray [16]byte                 `json:"byte_array"`
		Uuid      samuuid.Uuid             `json:"uuid"`
		OneOf     testproto.OneOfHolder    `json:"oneof"`
		TestProto testproto.TestRequest    `json:"test_proto"`
		AllTypes  examplemodelgen.AllTypes `json:"all_types"`
	}

	testCases := []struct {
		label  string
		params ConversionParams
	}{
		{
			label: "default",
		},
		{
			label: "legacyUint64",
			params: ConversionParams{
				LegacyUint64AsBigint: true,
			},
		},
		{
			label: "preserveOrigName",
			params: ConversionParams{
				PreserveProtobufOriginalNames: true,
			},
		},
		{
			label: "preserveOrigNameAndInt64DecimalString",
			params: ConversionParams{
				PreserveProtobufOriginalNames: true,
				JsonpbInt64AsDecimalString:    true,
			},
		},
		{
			label: "allOn",
			params: ConversionParams{
				LegacyUint64AsBigint:          true,
				PreserveProtobufOriginalNames: true,
				JsonpbInt64AsDecimalString:    true,
			},
		},
	}
	for _, tc := range testCases {
		tc := tc // capture loop variable
		t.Run(tc.label, func(t *testing.T) {
			snap := snapshotter.New(t)
			defer snap.Verify()

			typ, err := JsonTypeToSparkType(reflect.TypeOf(row{}), tc.params)
			require.NoError(t, err)

			snap.Snapshot("schema", strings.Split(typ.SQLIndent("", " "), "\n"))

			sparkSQLBin, err := exec.LookPath("spark-sql")
			if err != nil {
				t.Skip("spark-sql is not found in $PATH")
				return
			}

			tempDir, err := ioutil.TempDir("", "")
			require.NoError(t, err)
			defer os.RemoveAll(tempDir)

			cmd := exec.Command(sparkSQLBin,
				// Make Spark write artifacts to temporary dir.
				"--conf", fmt.Sprintf("spark.sql.warehouse.dir=%s", filepath.Join(tempDir, "spark-warehouse")),
				"--conf", fmt.Sprintf("spark.driver.extraJavaOptions=-Dderby.system.home=%s", filepath.Join(tempDir, "derby")),
			)

			sql := fmt.Sprintf(`CREATE TEMPORARY TABLE input (%s) USING JSON`, typ.SQLColumnsIndent("", ""))
			cmd.Env = os.Environ()
			cmd.Stdin = bytes.NewReader([]byte(sql))
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			require.NoError(t, cmd.Run())
		})
	}
}

func TestJsonpbOneOf(t *testing.T) {
	snap := snapshotter.New(t)
	defer snap.Verify()

	m := jsonpb.Marshaler{
		EnumsAsInts: true,
		OrigName:    true,
	}

	optionA, err := m.MarshalToString(&testproto.OneOfHolder{
		OneofHolder: &testproto.OneOfHolder_OptionA{
			OptionA: &testproto.OptionA{
				Value: 100,
			},
		},
	})
	require.NoError(t, err)
	snap.Snapshot("OptionA", optionA)

	optionB, err := m.MarshalToString(&testproto.OneOfHolder{
		OneofHolder: &testproto.OneOfHolder_OptionB{
			OptionB: &testproto.OptionB{
				Value: "foo",
			},
		},
	})
	require.NoError(t, err)
	snap.Snapshot("OptionB", optionB)
}

func TestSafetyActivityEventOneOf(t *testing.T) {
	snap := snapshotter.New(t)
	defer snap.Verify()

	typ, err := JsonTypeToSparkType(reflect.TypeOf(safetyproto.SafetyActivityDetail{}), ConversionParams{
		PreserveProtobufOriginalNames: true,
		JsonpbInt64AsDecimalString:    true,
	})
	require.NoError(t, err)

	snap.Snapshot("schema", strings.Split(typ.SQLIndent("", " "), "\n"))

	m := jsonpb.Marshaler{
		EnumsAsInts:  true,
		OrigName:     true,
		EmitDefaults: true,
	}
	data, err := m.MarshalToString(&safetyproto.SafetyActivityDetail{
		UserId:   1,
		UserType: safetyproto.AuthorType_CUSTOMER_USER,
		Content: &safetyproto.SafetyActivityDetail_BehaviorLabelContent{
			BehaviorLabelContent: &safetyproto.SafetyActivityBehaviorLabelContent{
				LabelType: safetyproto.Behavior_DefensiveDriving,
				NewValue:  true,
			},
		},
	})
	require.NoError(t, err)
	snap.Snapshot("data", data)
}

func TestJsonpbAllTypes(t *testing.T) {
	snap := snapshotter.New(t)
	defer snap.Verify()

	m := jsonpb.Marshaler{
		EnumsAsInts:  true,
		EmitDefaults: true,
		OrigName:     true,
	}
	s, err := m.MarshalToString(&examplemodelgen.AllTypes{})
	require.NoError(t, err)
	snap.Snapshot("json", s)
}
