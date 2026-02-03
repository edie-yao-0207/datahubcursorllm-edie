package metadatahelpers

import (
	"fmt"
	"log"
	"reflect"
	"sort"
)

func EnumDescription(description string, enumNameMap interface{}) string {
	enumMapValue := enumNameMap
	mapValue := reflect.ValueOf(enumMapValue)
	if mapValue.Kind() != reflect.Map {
		panic("enumNameMap argument needs to be of type map")
	}

	keys := make([]reflect.Value, len(mapValue.MapKeys()))
	for idx, key := range mapValue.MapKeys() {
		if !key.Type().Comparable() {
			log.Panicf("key type %T for enum map is not comparable.", key.Type())
		}
		keys[idx] = key
	}

	sort.Slice(keys, func(i, j int) bool {
		switch keys[i].Kind() {
		case reflect.String:
			return keys[i].String() < keys[j].String()
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return keys[i].Int() < keys[j].Int()
		default:
			log.Panicf("unknown key type for enum map: %T. Please add the key type to the switch statement in 'amundsen/metadatahelpers/helpers.go'", keys[i].Kind().String())
			return false
		}
	})

	returnStr := description + " \n\n "
	for _, key := range keys {
		returnStr += fmt.Sprintf("%v: %v \n\n ", key, mapValue.MapIndex(key))
	}

	return returnStr
}

func DeprecatedDescription(description string) string {
	return fmt.Sprintf("%s. %s", DeprecatedDefaultDescription, description)
}

func DiagnosticFeatureDataColumns(value string, enumNameMap interface{}) map[string]string {
	return map[string]string{
		"value.proto_value.diagnostic_feature_data.obd_value":             EnumDescription(fmt.Sprintf("Method in use to determine %s.", value), enumNameMap),
		"value.proto_value.diagnostic_feature_data.method_data.value":     fmt.Sprintf("Value of %s using the associated tracking_method.", value),
		"value.proto_value.diagnostic_feature_data.method_data.obd_value": EnumDescription(fmt.Sprintf("Lower priority method in use to determine %s.", value), enumNameMap),
	}
}
