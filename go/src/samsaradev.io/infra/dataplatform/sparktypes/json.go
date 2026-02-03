package sparktypes

import (
	"reflect"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/datastreamlake/datastreamtime"
	"samsaradev.io/samuuid"
)

var protoMessageType = reflect.TypeOf((*proto.Message)(nil)).Elem()

type ConversionParams struct {
	// LegacyUint64AsBigint converts uint64 to bigint instead of decimal(20,0) for legacy compatibility.
	LegacyUint64AsBigint bool

	// PreserveProtobufOriginalNames preserves original Protobuf field names
	// regardless of their gogoproto JSON overrides. This is in sync with
	// jsonpb.Marshaler{OrigName: true}.
	//
	// samsaradev.io/industrial/industrialcore/industrialcoreproto.AssetConfig_LocationConfig.DataInputId
	// is an example where both `json` and `protobuf:"json="` tags specify
	// camelCase name. It's unclear why it's generated this way. This seems to
	// affect Protobuf packages generated with `modelschema` only. This option
	// would turn them into snake_case.
	PreserveProtobufOriginalNames bool

	// JsonpbInt64AsDecimalString converts int64 and uint64 to string in
	// accordance to Protobuf JSON specification. This is the behavior of
	// gogo/protobuf/jsonpb. The original conversion type is stored in column
	// comment. This overrides LegacyUint64AsBigint. See
	// https://developers.google.com/protocol-buffers/docs/proto3#json.
	JsonpbInt64AsDecimalString bool
}

type conversionState struct {
	// inGogoProtoMessage is true if current type is a field inside a
	// gogo/proto.Message, where jsonpb could be used for serialization.
	inGogoProtoMessage bool

	// depth tracks current recursion depth.
	depth int

	// typs tracks unique types seen so far to prevent loops.
	typs map[reflect.Type]struct{}
}

func jsonTypeToSparkType(t reflect.Type, params ConversionParams, state conversionState) (*Type, error) {
	if state.depth > 1024 {
		return nil, oops.Errorf("too deep")
	}
	state.depth++

	if _, ok := state.typs[t]; ok {
		// Do not fail on recursive protobuf message type, e.g. industrialhubproto.ManualEntryEnumOption.
		return nil, nil
	}
	state.typs[t] = struct{}{}
	defer delete(state.typs, t)

	switch t {
	case reflect.TypeOf(samuuid.Uuid{}):
		// samuuid.Uuid reflects as t.Elem().Kind() == reflect.Uint8 but unmarshals from json as UUID string
		return StringType(), nil
	}

	switch t.Kind() {
	case reflect.Ptr:
		return jsonTypeToSparkType(t.Elem(), params, state)
	case reflect.Bool:
		return BooleanType(), nil
	case reflect.Int32:
		return IntegerType(), nil
	case reflect.Int64:
		if state.inGogoProtoMessage && params.JsonpbInt64AsDecimalString {
			return StringType(), nil
		}
		return LongType(), nil
	case reflect.Uint32:
		// No unsigned type in Hive/Spark. See https://issues.apache.org/jira/browse/SPARK-7697
		// Uint32 will fit inside a signed BIGINT
		return LongType(), nil
	case reflect.Uint64:
		if state.inGogoProtoMessage && params.JsonpbInt64AsDecimalString {
			return StringType(), nil
		}

		// No unsigned type in Hive/Spark. See https://issues.apache.org/jira/browse/SPARK-7697
		// Uint64 will not fit inside a signed BIGINT so we must use Decimal instead (while preserving legacy behavior is specified).
		if params.LegacyUint64AsBigint {
			return LongType(), nil
		}
		return DecimalType(20, 0), nil
	case reflect.Float32:
		return FloatType(), nil
	case reflect.Float64:
		return DoubleType(), nil
	case reflect.String:
		return StringType(), nil
	case reflect.Interface:
		return nil, nil
	case reflect.Slice, reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			return BinaryType(), nil
		}

		typ, err := jsonTypeToSparkType(t.Elem(), params, state)
		if err != nil {
			return nil, oops.Wrapf(err, "%s[]", t.Elem().Name())
		}
		return ArrayType(typ), nil
	case reflect.Map:
		keyTyp, err := jsonTypeToSparkType(t.Key(), params, state)
		if err != nil {
			return nil, oops.Wrapf(err, "map[%s]%s key error", t.Key().Name(), t.Elem().Name())
		}
		valueTyp, err := jsonTypeToSparkType(t.Elem(), params, state)
		if err != nil {
			return nil, oops.Wrapf(err, "map[%s]%s value error", t.Key().Name(), t.Elem().Name())
		}
		return MapType(keyTyp, valueTyp), nil
	case reflect.Struct:

		// Spark, more specifically parquet, does not like empty structs
		// It can't write them and will throw:
		// Caused by: org.apache.parquet.schema.InvalidSchemaException: Cannot write a schema with an empty group: optional group field_name {}
		// https://issues.apache.org/jira/browse/SPARK-20593
		// To handle this, if a struct is empty we don't render the schema for it since we assume it once had fields but no longer does
		// and is deprecated
		if t.NumField() == 0 {
			return nil, nil
		}

		// Check if the struct is a proto message.
		if reflect.PtrTo(t).Implements(protoMessageType) {
			state.inGogoProtoMessage = true
		}

		var fields []*Field
		for i := 0; i < t.NumField(); i++ {
			// HACK: data streams Go Structs use a custom Time struct for marshalling purposes
			// When we hit this struct we know it is a TimestampType() so add it to fields and continue
			if t.Field(i).Type == reflect.TypeOf(datastreamtime.Time{}) {
				fields = append(fields, StructField(t.Field(i).Tag.Get("json"), TimestampType()))
				continue
			}

			// Special handling for protobuf oneof types.
			if _, ok := t.Field(i).Tag.Lookup("protobuf_oneof"); ok {
				// Infer valid oneof types using the XXX_OneofFuncs method generated for
				// every oneof message type. This is technically an internal, unstable
				// API. If the API ever changes, it will likely break this logic and
				// either cause an runtime error during generation, or change generated
				// outputs and fail CI checks. The API signature is defined in
				// https://github.com/samsara-dev/backend/blob/51fb4ceeb1bc40ab3f25aa7341cb1c327e2a7f5a/go%2Fsrc%2Fsamsaradev.io%2Fvendor%2Fgithub.com%2Fgolang%2Fprotobuf%2Fproto%2Fproperties.go#L346.
				// The 4th return value includes all possible concrete types.
				method := reflect.New(t).MethodByName("XXX_OneofFuncs")
				if method.IsZero() {
					return nil, oops.Errorf("XXX_OneofFuncs() not defined on %s", t.String())
				}

				returnValues := method.Call([]reflect.Value{})
				if len(returnValues) != 4 {
					return nil, oops.Errorf("expect 4 return values from XXX_OneofFuncs(), got %d", len(returnValues))
				}

				optionNilPtrs := returnValues[3].Interface().([]interface{})
				for _, ptr := range optionNilPtrs {
					t := reflect.TypeOf(ptr).Elem()
					protobufTag := t.Field(0).Tag.Get("protobuf")

					var name string
					for _, tag := range strings.Split(protobufTag, ",") {
						if strings.HasPrefix(tag, "name=") {
							name = strings.TrimPrefix(tag, "name=")
							break
						}
					}
					if name == "" {
						return nil, oops.Errorf("could not find protobuf field name in type %s", t.String())
					}

					typ, err := jsonTypeToSparkType(t.Field(0).Type, params, state)
					if err != nil {
						return nil, oops.Wrapf(err, "%s.%s", t.Name(), t.Field(0).Name)
					}
					fields = append(fields, StructField(name, typ))
				}
				continue
			}

			name := strings.Split(t.Field(i).Tag.Get("json"), ",")[0]
			if name == "" {
				return nil, oops.Errorf("%s.%s json tag not set", t.Name(), t.Field(i).Name)
			}

			if params.PreserveProtobufOriginalNames {
				if protobufTag, ok := t.Field(i).Tag.Lookup("protobuf"); ok {
					// Override name with original protobuf field names. This is jsonpb's
					// behavior with OrigName option set to true.
					for _, tag := range strings.Split(protobufTag, ",") {
						if strings.HasPrefix(tag, "name=") {
							name = strings.TrimPrefix(tag, "name=")
							break
						}
					}
				}
			}

			if _, ok := t.Field(i).Tag.Lookup("protobuf"); ok {
				// Special handling for Protobuf timestamps.
				if (t.Field(i).Type == reflect.TypeOf(time.Time{})) ||
					(t.Field(i).Type.Kind() == reflect.Ptr && t.Field(i).Type.Elem() == reflect.TypeOf(time.Time{})) {
					fields = append(fields, StructField(name, TimestampType()))
					continue
				}
			}

			typ, err := jsonTypeToSparkType(t.Field(i).Type, params, state)
			if err != nil {
				return nil, oops.Wrapf(err, "%s.%s", t.Name(), t.Field(i).Name)
			}
			if typ != nil {
				fields = append(fields, StructField(name, typ))
			}
		}
		return StructType(fields...), nil

	default:
		return nil, oops.Errorf("unhandled type: %s", t.String())
	}
}

// JsonTypeToSparkType maps certain reflect.Types to spark types.
func JsonTypeToSparkType(t reflect.Type, params ConversionParams) (*Type, error) {
	return jsonTypeToSparkType(t, params, conversionState{
		typs: make(map[reflect.Type]struct{}),
	})
}
