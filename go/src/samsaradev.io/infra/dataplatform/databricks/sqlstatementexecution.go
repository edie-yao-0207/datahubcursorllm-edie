package databricks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"time"

	"github.com/samsarahq/go/oops"
)

type ExecuteStatementInput struct {
	WarehouseId   string         `json:"warehouse_id"`
	Statement     string         `json:"statement"`
	Catalog       *string        `json:"catalog"`
	Schema        *string        `json:"schema"`
	Parameters    []Parameter    `json:"parameters"`
	RowLimit      *int64         `json:"row_limit"`
	ByteLimit     *int64         `json:"byte_limit"`
	Disposition   *Disposition   `json:"disposition"`
	Format        *Format        `json:"format"`
	WaitTimeout   *string        `json:"wait_timeout"`
	OnWaitTimeout *OnWaitTimeout `json:"on_wait_timeout"`
}

type Parameter struct {
	Name  string  `json:"name"`
	Value *string `json:"value"`
	Type  *string `json:"type"`
}

type Disposition string

const (
	DispositionInline        Disposition = "INLINE"
	DispositionExternalLinks Disposition = "EXTERNAL_LINKS"
)

type Format string

const (
	FormatJsonArray   Format = "JSON_ARRAY"
	FormatArrowStream Format = "ARROW_STREAM"
	FormatCsv         Format = "CSV"
)

type OnWaitTimeout string

const (
	OnWaitTimeoutContinue OnWaitTimeout = "CONTINUE"
	OnWaitTimeoutCancel   OnWaitTimeout = "CANCEL"
)

type ExecuteStatementOutput struct {
	StatementId string    `json:"statement_id"`
	Status      Status    `json:"status"`
	Manifest    *Manifest `json:"manifest,omitempty"`
	Result      *Result   `json:"result,omitempty"`
}

type Manifest struct {
	Format          string  `json:"format"`
	Schema          Schema  `json:"schema"`
	TotalChunkCount *int64  `json:"total_chunk_count,omitempty"`
	Chunks          []Chunk `json:"chunks,omitempty"`
	TotalRowCount   *int64  `json:"total_row_count,omitempty"`
	TotalByteCount  *int64  `json:"total_byte_count,omitempty"`
	Truncated       *bool   `json:"truncated,omitempty"`
}

type Chunk struct {
	ChunkIndex int64 `json:"chunk_index"`
	RowOffset  int64 `json:"row_offset"`
	RowCount   int64 `json:"row_count"`
}

type Schema struct {
	ColumnCount *int64   `json:"column_count"`
	Columns     []Column `json:"columns"`
}

type Column struct {
	Name             string   `json:"name"`
	Position         int64    `json:"position"`
	TypeName         TypeName `json:"type_name"`
	TypeText         string   `json:"type_text"`
	TypePrecision    *int32   `json:"type_precision,omitempty"`
	TypeScale        *int32   `json:"type_scale,omitempty"`
	TypeIntervalType *string  `json:"type_interval_type,omitempty"`
}

type TypeName string

const (
	TypeNameBoolean         TypeName = "BOOLEAN"
	TypeNameByte            TypeName = "BYTE"
	TypeNameShort           TypeName = "SHORT"
	TypeNameInt             TypeName = "INT"
	TypeNameLong            TypeName = "LONG"
	TypeNameFloat           TypeName = "FLOAT"
	TypeNameDouble          TypeName = "DOUBLE"
	TypeNameDate            TypeName = "DATE"
	TypeNameTimestamp       TypeName = "TIMESTAMP"
	TypeNameString          TypeName = "STRING"
	TypeNameBinary          TypeName = "BINARY"
	TypeNameDecimal         TypeName = "DECIMAL"
	TypeNameInterval        TypeName = "INTERVAL"
	TypeNameArray           TypeName = "ARRAY"
	TypeNameStruct          TypeName = "STRUCT"
	TypeNameMap             TypeName = "MAP"
	TypeNameChar            TypeName = "CHAR"
	TypeNameNull            TypeName = "NULL"
	TypeNameUserDefinedType TypeName = "USER_DEFINED_TYPE"
)

type Result struct {
	ChunkIndex            *int64         `json:"chunk_index,omitempty"`
	DataArray             [][]*string    `json:"data_array,omitempty"`
	NextChunkIndex        *int           `json:"next_chunk_index,omitempty"`
	NextChunkInternalLink *string        `json:"next_chunk_internal_link,omitempty"`
	RowCount              *int64         `json:"row_count,omitempty"`
	RowOffset             *int64         `json:"row_offset,omitempty"`
	ExternalLinks         []ExternalLink `json:"external_links,omitempty"`
	ExpirationTime        *time.Time     `json:"expiration_time,omitempty"`
}

type ExternalLink struct {
	ChunkIndex   int64     `json:"chunk_index"`
	RowOffset    int64     `json:"row_offset"`
	RowCount     int64     `json:"row_count"`
	ByteCount    int64     `json:"byte_count"`
	ExternalLink string    `json:"external_link"`
	Expiration   time.Time `json:"expiration"`
}
type Status struct {
	State State  `json:"state"`
	Error *Error `json:"error,omitempty"`
}

type State string

const (
	StatePending   State = "PENDING"
	StateRunning   State = "RUNNING"
	StateSucceeded State = "SUCCEEDED"
	StateFailed    State = "FAILED"
	StateCanceled  State = "CANCELED"
	StateClosed    State = "CLOSED"
)

type Error struct {
	ErrorCode ErrorCode `json:"error_code"`
	Message   string    `json:"message"`
}

type ErrorCode string

const (
	ErrorCodeUnknown                         ErrorCode = "UNKNOWN"
	ErrorCodeInternalError                   ErrorCode = "INTERNAL_ERROR"
	ErrorCodeTemporarilyUnavailable          ErrorCode = "TEMPORARILY_UNAVAILABLE"
	ErrorCodeIOError                         ErrorCode = "IO_ERROR"
	ErrorCodeBadRequest                      ErrorCode = "BAD_REQUEST"
	ErrorCodeServiceUnderMaintenance         ErrorCode = "SERVICE_UNDER_MAINTENANCE"
	ErrorCodeWorkspaceTemporarilyUnavailable ErrorCode = "WORKSPACE_TEMPORARILY_UNAVAILABLE"
	ErrorCodeDeadlineExceeded                ErrorCode = "DEADLINE_EXCEEDED"
	ErrorCodeCancelled                       ErrorCode = "CANCELLED"
	ErrorCodeResourceExhausted               ErrorCode = "RESOURCE_EXHAUSTED"
	ErrorCodeAborted                         ErrorCode = "ABORTED"
	ErrorCodeNotFound                        ErrorCode = "NOT_FOUND"
	ErrorCodeAlreadyExists                   ErrorCode = "ALREADY_EXISTS"
	ErrorCodeUnauthenticated                 ErrorCode = "UNAUTHENTICATED"
)

type ResultChunk struct {
	ChunkIndex            int64       `json:"chunk_index"`
	RowOffset             int64       `json:"row_offset"`
	RowCount              int64       `json:"row_count"`
	NextChunkIndex        *int64      `json:"next_chunk_index,omitempty"`
	NextChunkInternalLink *string     `json:"next_chunk_internal_link,omitempty"`
	DataArray             [][]*string `json:"data_array"`
}

// ScanRows scans through the DataArray in the ExecuteStatementOutput based on the Schema and populates the provided struct slice.
func (e *ExecuteStatementOutput) ScanRows(v interface{}) error {
	// Check if there are results to process
	if e.Result == nil {
		return errors.New("no results to scan")
	}

	// Check if there is manifest with schema provided
	if e.Manifest == nil {
		return errors.New("no schema provided")
	}

	return scanDataArray(v, e.Result.DataArray, *e.Manifest)
}

// ScanRows scans through the DataArray in the ResultChunk based on the Schema and populates the provided struct slice.
func (r *ResultChunk) ScanRows(v interface{}, manifest Manifest) error {
	return scanDataArray(v, r.DataArray, manifest)
}

// scanDataArray scans through the DataArray in the Result based on the Schema and populates the provided struct slice.
func scanDataArray(v interface{}, dataArray [][]*string, manifest Manifest) error {
	// Ensure the provided v is a slice of structs
	ptrVal := reflect.ValueOf(v)
	if ptrVal.Kind() != reflect.Ptr || ptrVal.Elem().Kind() != reflect.Slice {
		return errors.New("provided argument must be a pointer to a slice")
	}

	// Ensure the slice is a pointer to a struct
	elemType := ptrVal.Elem().Type().Elem()
	if elemType.Kind() != reflect.Struct {
		return errors.New("slice must be a slice of structs")
	}

	// Scan each row in the DataArray
	for _, row := range dataArray {
		// Create a new instance of the struct from the slice
		newElem := reflect.New(elemType).Elem()

		for i, column := range manifest.Schema.Columns {
			if len(manifest.Schema.Columns) < len(row) {
				return errors.New("row length doesn't match schema")
			}

			// Get the field name from the struct. If there is JSON tag available , use it,
			// otherwise convert the column name from manifest to CamelCase.
			var fieldName string
			field, err := getFieldByJSONTag(newElem.Interface(), column.Name)
			if err != nil {
				fieldName = toCamelCase(column.Name)
			} else {
				fieldName = field.Name
			}

			fieldValue := newElem.FieldByName(fieldName)
			if !fieldValue.IsValid() || !fieldValue.CanSet() {
				continue
			}

			if err := setFieldValue(fieldValue, column.TypeName, row[i]); err != nil {
				return err
			}
		}

		// Append the new struct to the slice
		ptrVal.Elem().Set(reflect.Append(ptrVal.Elem(), newElem))
	}

	return nil
}

// getFieldByJSONTag returns the struct field with the given JSON tag.
func getFieldByJSONTag(v interface{}, tagName string) (reflect.StructField, error) {
	val := reflect.ValueOf(v)
	if val.Kind() != reflect.Struct {
		return reflect.StructField{}, errors.New("provided value is not a struct")
	}

	typ := val.Type()

	// Iterate over the struct's fields
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		// Extract json tag from struct field
		jsonTag := field.Tag.Get("json")
		if jsonTag == tagName {
			return field, nil
		}
	}

	return reflect.StructField{}, fmt.Errorf("no field found with json tag: %s", tagName)
}

// setFieldValue sets a field's value based on the TypeName and the string value.
func setFieldValue(field reflect.Value, columnType TypeName, value *string) error {
	// Check for "null" string or empty string, treat them as nil for pointer fields
	if value == nil {
		// If the field is a pointer, set it to nil (zero value for pointer type)
		if field.Kind() == reflect.Ptr {
			field.Set(reflect.Zero(field.Type()))
		}
		return nil
	}

	switch columnType {
	case TypeNameBoolean:
		boolVal, err := strconv.ParseBool(*value)
		if err != nil {
			return err
		}
		if field.Kind() == reflect.Ptr {
			field.Set(reflect.ValueOf(&boolVal))
		} else {
			field.SetBool(boolVal)
		}
	case TypeNameInt, TypeNameLong:
		intVal, err := strconv.ParseInt(*value, 10, 64)
		if err != nil {
			return err
		}
		if field.Kind() == reflect.Ptr {
			field.Set(reflect.ValueOf(&intVal))
		} else {
			field.SetInt(intVal)
		}
	case TypeNameFloat, TypeNameDouble, TypeNameDecimal:
		floatVal, err := strconv.ParseFloat(*value, 64)
		if err != nil {
			return err
		}
		if field.Kind() == reflect.Ptr {
			field.Set(reflect.ValueOf(&floatVal))
		} else {
			field.SetFloat(floatVal)
		}
	case TypeNameDate:
		dateVal, err := time.Parse("2006-01-02", *value)
		if err != nil {
			return err
		}
		if field.Kind() == reflect.Ptr {
			field.Set(reflect.ValueOf(&dateVal))
		} else {
			field.Set(reflect.ValueOf(dateVal))
		}
	case TypeNameTimestamp:
		timestampVal, err := time.Parse(time.RFC3339, *value)
		if err != nil {
			return err
		}
		if field.Kind() == reflect.Ptr {
			field.Set(reflect.ValueOf(&timestampVal))
		} else {
			field.Set(reflect.ValueOf(timestampVal))
		}
	case TypeNameString:
		if field.Kind() == reflect.Ptr {
			field.Set(reflect.ValueOf(value))
		} else {
			field.SetString(*value)
		}
	case TypeNameArray:
		// For arrays, we expect the field to be a slice
		if field.Kind() != reflect.Slice && field.Kind() != reflect.Ptr {
			return fmt.Errorf("field %s must be a slice or pointer to handle array type", field.Type().String())
		}

		// Parse the array string (expected format is JSON array)
		var arrayValues []interface{}
		if err := json.Unmarshal([]byte(*value), &arrayValues); err != nil {
			return fmt.Errorf("failed to unmarshal array value: %v", err)
		}

		// If it's a pointer to a slice, we need to create the slice first
		if field.Kind() == reflect.Ptr {
			sliceType := field.Type().Elem()
			newSlice := reflect.MakeSlice(sliceType, len(arrayValues), len(arrayValues))
			ptrToSlice := reflect.New(sliceType)
			ptrToSlice.Elem().Set(newSlice)
			field.Set(ptrToSlice)
			field = ptrToSlice.Elem()
		} else {
			// Create a new slice with the appropriate length
			field.Set(reflect.MakeSlice(field.Type(), len(arrayValues), len(arrayValues)))
		}

		elemType := field.Type().Elem()
		for i, val := range arrayValues {
			elemValue := reflect.New(elemType).Elem()

			switch elemType.Kind() {
			case reflect.String:
				if strVal, ok := val.(string); ok {
					elemValue.SetString(strVal)
				}
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				if numVal, ok := val.(float64); ok {
					elemValue.SetInt(int64(numVal))
				}
			case reflect.Float32, reflect.Float64:
				if numVal, ok := val.(float64); ok {
					elemValue.SetFloat(numVal)
				}
			case reflect.Bool:
				if boolVal, ok := val.(bool); ok {
					elemValue.SetBool(boolVal)
				}
			default:
				return fmt.Errorf("unsupported array element type: %s", elemType.String())
			}

			field.Index(i).Set(elemValue)
		}
	default:
		return errors.New("unsupported column type: " + string(columnType))
	}

	return nil
}

// Helper function to convert snake_case to CamelCase
func toCamelCase(str string) string {
	result := ""
	upperNext := false
	for i, r := range str {
		if i == 0 {
			result = string(r - 32)
		} else if r == '_' {
			upperNext = true
		} else {
			if upperNext {
				result += string(r - 32)
				upperNext = false
			} else {
				result += string(r)
			}
		}
	}
	return result
}

// Databricks Statement Execution API documentation: https://docs.databricks.com/api/workspace/statementexecution
type SqlStatementExecutionAPI interface {
	ExecuteStatement(context.Context, *ExecuteStatementInput) (*ExecuteStatementOutput, error)
	GetStatement(context.Context, string) (*ExecuteStatementOutput, error)
	GetResultChunk(ctx context.Context, statementId string, chunkIndex int64) (*ResultChunk, error)
	CancelStatement(ctx context.Context, statementId string) error
}

/*
Execute a SQL statement and optionally await its results for a specified time.

Use case: small result sets with Disposition:INLINE + Format:JSON_ARRAY

	For flows that generate small and predictable result sets (<= 25 MiB), INLINE responses of
	JSON_ARRAY result data are typically the simplest way to execute and fetch result data.

Use case: large result sets with Disposition:EXTERNAL_LINKS

	Using EXTERNAL_LINKS to fetch result data allows you to fetch large result sets efficiently.
	The main differences from using INLINE disposition are that the result data is accessed with presigned URLs, and that there
	are 3 supported formats: JSON_ARRAY, ARROW_STREAM and CSV compared to only JSON_ARRAY with INLINE.
*/
func (c *Client) ExecuteStatement(ctx context.Context, input *ExecuteStatementInput) (*ExecuteStatementOutput, error) {
	var output ExecuteStatementOutput
	if err := c.do(ctx, http.MethodPost, "/api/2.0/sql/statements", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return &output, nil
}

// This request can be used to poll for the statement's status. When the status.state field is SUCCEEDED it will also return
// the result manifest and the first chunk of the result data. When the statement is in the terminal states CANCELED,
// CLOSED or FAILED, it returns HTTP 200 with the state set. After at least 12 hours in terminal state, the statement is
// removed from the warehouse and further calls will receive an HTTP 404 response.
func (c *Client) GetStatement(ctx context.Context, statementId string) (*ExecuteStatementOutput, error) {
	var output ExecuteStatementOutput
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf("/api/2.0/sql/statements/%s", statementId), nil, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return &output, nil
}

// After the statement execution has SUCCEEDED, this request can be used to fetch any chunk by index.
// Whereas the first chunk with chunk_index=0 is typically fetched with ExecuteStatement or GetStatement,
// this request can be used to fetch subsequent chunks. The response structure is identical to the nested result element described in the
// GetStatement request, and similarly includes the next_chunk_index and next_chunk_internal_link fields for simple iteration through the result set.
func (c *Client) GetResultChunk(ctx context.Context, statementId string, chunkIndex int64) (*ResultChunk, error) {
	var output ResultChunk
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf("/api/2.0/sql/statements/%s/result/chunks/%d", statementId, chunkIndex), nil, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return &output, nil
}

// Requests that an executing statement be canceled. Callers must poll for status to see the terminal state.
func (c *Client) CancelStatement(ctx context.Context, statementId string) error {
	return c.do(ctx, http.MethodPost, fmt.Sprintf("/api/2.0/sql/statements/%s/cancel", statementId), nil, nil)
}
