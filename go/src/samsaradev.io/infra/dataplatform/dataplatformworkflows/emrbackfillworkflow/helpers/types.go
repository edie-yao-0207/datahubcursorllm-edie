package helpers

// FilterOptions represents a filter option for EMR data queries.
type FilterOptions struct {
	Field      string
	Comparator string
}

type FilterFieldToValueMap map[string]int64

// FilterAllowedComparators is a list of comparators that are allowed to be used
// in the filter fields. These are the comparators that are supported by the EMR
// API.
var FilterAllowedComparators = []string{"LessThan", "GreaterThanOrEqual"}
