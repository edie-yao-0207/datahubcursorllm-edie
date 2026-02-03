package sparktypes

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/datastreamlake/datastreamtime"
)

// List of protobuf message types that are known to be recursive to
// be excluded from RDS schema generation
var recursiveProtobufTypes = []string{
	"dispatchproto.DispatchRouteEventResolvingEvent",
}

func contains(arr []string, key string) bool {
	for _, item := range arr {
		if item == key {
			return true
		}
	}
	return false
}

func protobufTypeToSparkType(t reflect.Type, params ConversionParams, state conversionState, excludeJsonTags []string) (*Type, error) {
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

	switch t.Kind() {
	case reflect.Ptr:
		return protobufTypeToSparkType(t.Elem(), params, state, excludeJsonTags)
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

		typ, err := protobufTypeToSparkType(t.Elem(), params, state, excludeJsonTags)
		if err != nil {
			return nil, oops.Wrapf(err, "%s[]", t.Elem().Name())
		}
		return ArrayType(typ), nil
	case reflect.Map:
		keyTyp, err := protobufTypeToSparkType(t.Key(), params, state, excludeJsonTags)
		if err != nil {
			return nil, oops.Wrapf(err, "map[%s]%s key error", t.Key().Name(), t.Elem().Name())
		}
		valueTyp, err := protobufTypeToSparkType(t.Elem(), params, state, excludeJsonTags)
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

		if contains(recursiveProtobufTypes, fmt.Sprintf("%v", t)) {
			return nil, nil
		}

		var fields []*Field
		for i := 0; i < t.NumField(); i++ {

			// If a jsontag of a proto field is provided in the exclude list,
			// then dont generate schema for it and skip it in replication.
			if contains(excludeJsonTags, t.Field(i).Tag.Get("json")) {
				continue
			}

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

					typ, err := protobufTypeToSparkType(t.Field(0).Type, params, state, excludeJsonTags)
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

			typ, err := protobufTypeToSparkType(t.Field(i).Type, params, state, excludeJsonTags)
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

// ProtobufTypeToSparkType maps certain reflect.Types to spark types. The conversion method is consistent with how
// protobuf parsing in our fork of spark-protobuf is implemented.
func ProtobufTypeToSparkType(t reflect.Type, params ConversionParams, excludeJsonTags []string) (*Type, error) {
	return protobufTypeToSparkType(t, params, conversionState{
		typs: make(map[reflect.Type]struct{}),
	}, excludeJsonTags)
}
