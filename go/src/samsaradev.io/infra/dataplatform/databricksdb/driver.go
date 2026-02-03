package databricksdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"math"
	"net/http"
	"net/url"
	"time"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/samsara_thrift/thrift"

	"samsaradev.io/infra/dataplatform/databricksdb/gen-go/tcliservice"
	"samsaradev.io/libs/ni/pointer"
)

func init() {
	sql.Register(dbName, &Driver{})
}

const dbName = "databricksdb"

// pollInterval specifies how often driver polls for query status.
const pollInterval = time.Second

// resultBatchSize specifies how many rows to retrieve from databricks at a time.
// Databricks recommends a value of at least 100,000: https://kb.databricks.com/bi/jdbc-odbc-troubleshooting.html#fetching-result-set-is-slow-after-statement-execution
const resultBatchSize = 100000

// checkStatus checks if a return status is a success state.
func checkStatus(status *tcliservice.TStatus) error {
	switch status.StatusCode {
	case tcliservice.TStatusCode_SUCCESS_STATUS, tcliservice.TStatusCode_SUCCESS_WITH_INFO_STATUS:
		return nil
	default:
		return oops.Errorf("bad status: %s", status.String())
	}
}

// isNull checks the null bit for a given position in bitmap.
// https://github.com/beltran/gohive/blob/master/hive.go#L712
func isNull(nulls []byte, position int) bool {
	index := position / 8
	if len(nulls) > index {
		b := nulls[index]
		return (b & (1 << (uint)(position%8))) != 0
	}
	return false
}

// convertColumnValues converts result set of a column to a slice of values.
func convertColumnValues(col *tcliservice.TColumn) ([]interface{}, error) {
	switch {
	case col.IsSetStringVal():
		out := make([]interface{}, len(col.GetStringVal().GetValues()))
		for i, v := range col.GetStringVal().GetValues() {
			if isNull(col.GetStringVal().GetNulls(), i) {
				out[i] = nil
			} else {
				out[i] = v
			}
		}
		return out, nil
	case col.IsSetBoolVal():
		out := make([]interface{}, len(col.GetBoolVal().GetValues()))
		for i, v := range col.GetBoolVal().GetValues() {
			if isNull(col.GetBoolVal().GetNulls(), i) {
				out[i] = nil
			} else {
				out[i] = v
			}
		}
		return out, nil
	case col.IsSetByteVal():
		out := make([]interface{}, len(col.GetByteVal().GetValues()))
		for i, v := range col.GetByteVal().GetValues() {
			if isNull(col.GetByteVal().GetNulls(), i) {
				out[i] = nil
			} else {
				out[i] = v
			}
		}
		return out, nil
	case col.IsSetI16Val():
		out := make([]interface{}, len(col.GetI16Val().GetValues()))
		for i, v := range col.GetI16Val().GetValues() {
			if isNull(col.GetI16Val().GetNulls(), i) {
				out[i] = nil
			} else {
				out[i] = v
			}
		}
		return out, nil
	case col.IsSetI32Val():
		out := make([]interface{}, len(col.GetI32Val().GetValues()))
		for i, v := range col.GetI32Val().GetValues() {
			if isNull(col.GetI32Val().GetNulls(), i) {
				out[i] = nil
			} else {
				out[i] = v
			}
		}
		return out, nil
	case col.IsSetI64Val():
		out := make([]interface{}, len(col.GetI64Val().GetValues()))
		for i, v := range col.GetI64Val().GetValues() {
			if isNull(col.GetI64Val().GetNulls(), i) {
				out[i] = nil
			} else {
				out[i] = v
			}
		}
		return out, nil
	case col.IsSetDoubleVal():
		out := make([]interface{}, len(col.GetDoubleVal().GetValues()))
		for i, v := range col.GetDoubleVal().GetValues() {
			if isNull(col.GetDoubleVal().GetNulls(), i) {
				out[i] = nil
			} else {
				out[i] = v
			}
		}
		return out, nil
	default:
		return nil, oops.Errorf("unknown column type")
	}
}

// waitForOperation waits for an operation to end.
// It returns error if the terminal state is not FINISHED_STATE.
func waitForOperation(ctx context.Context, client tcliservice.TCLIService, operation *tcliservice.TOperationHandle) error {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			resp, err := client.GetOperationStatus(ctx, &tcliservice.TGetOperationStatusReq{
				OperationHandle: operation,
			})
			if err != nil {
				return oops.Wrapf(err, "GetOperationStatus: %s", operation.String())
			}
			if err := checkStatus(resp.Status); err != nil {
				return oops.Wrapf(err, "")
			}

			switch resp.GetOperationState() {
			case tcliservice.TOperationState_CANCELED_STATE,
				tcliservice.TOperationState_CLOSED_STATE,
				tcliservice.TOperationState_ERROR_STATE,
				tcliservice.TOperationState_TIMEDOUT_STATE:
				return oops.Errorf("bad operation state %s: %s", resp.OperationState.String(), resp.Status.GetErrorMessage())
			case tcliservice.TOperationState_FINISHED_STATE:
				return nil
			}
		}
	}
}

type rowReaderBatch struct {
	columns [][]interface{}
	size    int
	offset  int
}

type databricksRowReader struct {
	ctx         context.Context
	client      tcliservice.TCLIService
	operation   *tcliservice.TOperationHandle
	columnNames []string
	batch       rowReaderBatch
}

var _ driver.Rows = &databricksRowReader{}

func newDatabricksRowReader(ctx context.Context, client tcliservice.TCLIService, operation *tcliservice.TOperationHandle) (*databricksRowReader, error) {
	metadata, err := client.GetResultSetMetadata(ctx, &tcliservice.TGetResultSetMetadataReq{
		OperationHandle: operation,
	})
	if err != nil {
		return nil, oops.Wrapf(err, "GetResultSetMetadata: %s", operation.String())
	}
	if err := checkStatus(metadata.Status); err != nil {
		return nil, err
	}

	columnNames := make([]string, len(metadata.Schema.Columns))
	for i, col := range metadata.Schema.Columns {
		columnNames[i] = col.ColumnName
	}

	return &databricksRowReader{
		ctx:         ctx,
		client:      client,
		operation:   operation,
		columnNames: columnNames,
	}, nil
}

func (r *databricksRowReader) Columns() []string {
	return r.columnNames
}

func (r *databricksRowReader) Close() error {
	if r.batch.columns == nil {
		return nil
	}

	// Don't use ctx from QueryContext, because ctx is likely canceled already.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := r.client.CloseOperation(ctx, &tcliservice.TCloseOperationReq{
		OperationHandle: r.operation,
	})
	if err != nil {
		return oops.Wrapf(err, "close operation")
	}
	if err := checkStatus(resp.Status); err != nil {
		return oops.Wrapf(err, "close operation")
	}
	r.batch.columns = nil
	r.batch.size = -1
	r.batch.offset = 0
	return nil
}

func (r *databricksRowReader) Next(dest []driver.Value) error {
	if r.batch.offset >= r.batch.size {
		// Call FetchResults to grab a new page of results.
		resp, err := r.client.FetchResults(r.ctx, &tcliservice.TFetchResultsReq{
			OperationHandle: r.operation,
			Orientation:     tcliservice.TFetchOrientation_FETCH_NEXT,
			MaxRows:         resultBatchSize,
		})
		if err != nil {
			return oops.Wrapf(err, "FetchResults: %s", r.operation.String())
		}
		if err := checkStatus(resp.Status); err != nil {
			return err
		}

		r.batch.columns = make([][]interface{}, len(resp.Results.Columns))
		for columnIdx, column := range resp.Results.Columns {
			values, err := convertColumnValues(column)
			if err != nil {
				return oops.Wrapf(err, "")
			}
			if len(values) == 0 {
				// End of result set.
				return io.EOF
			}
			r.batch.columns[columnIdx] = values
		}
		r.batch.offset = 0
		r.batch.size = len(r.batch.columns[0])
	}
	for columnIdx, column := range r.batch.columns {
		dest[columnIdx] = column[r.batch.offset]
	}
	r.batch.offset++
	return nil
}

type Driver struct{}

var _ driver.Driver = &Driver{}
var _ driver.DriverContext = &Driver{}

func (d *Driver) Open(name string) (driver.Conn, error) {
	connector, err := d.OpenConnector(name)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return connector.Connect(context.Background())
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	u, err := url.Parse(name)
	if err != nil {
		return nil, oops.Wrapf(err, "parse: %s", name)
	}

	transport, err := thrift.NewTHttpClientWithOptions(u.String(), thrift.THttpClientOptions{
		Client: &http.Client{
			Transport: &ErrorLoggingHttpRoundTripper{inner: http.DefaultTransport},
		},
	})
	if err != nil {
		return nil, oops.Wrapf(err, "NewTHttpClient: %s", u.String())
	}

	// This is required for SQL Endpoints only. Databricks rejects unknown clients
	// with errors like this: "thrift.Thrift.TApplicationException:
	// MALFORMED_REQUEST: Client SimbaJDBCDriver 2.6.14 is not supported for SQL
	// Endpoints.SimbaJDBCDriver 2.6.14, SimbaODBCDriver 2.6.15 or above is
	// required."
	transport.(*thrift.THttpClient).SetHeader("User-Agent", "SimbaSparkJDBCDriver/2.6.14")

	// THttpClient.Open() actually does nothing.
	if err := transport.Open(); err != nil {
		return nil, oops.Wrapf(err, "")
	}

	protocol := thrift.NewTBinaryProtocolTransport(transport)
	client := tcliservice.NewTCLIServiceClient(thrift.NewTStandardClient(protocol, protocol))

	return &Connector{
		driver: d,
		client: client,
	}, nil
}

type Connector struct {
	driver *Driver
	client tcliservice.TCLIService
}

var _ driver.Connector = &Connector{}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	resp, err := c.client.OpenSession(ctx, &tcliservice.TOpenSessionReq{
		ClientProtocol: tcliservice.TProtocolVersion_HIVE_CLI_SERVICE_PROTOCOL_V6,
		Configuration:  map[string]string{},
		// Databricks only authenticates using API tokens supplied in Authorization
		// header in the HTTP layer. We set username to "token" because historically
		// Databricks suggests using "token" as username when prompted in Tableau
		// configurations.
		Username: pointer.StringPtr("token"),
	})
	if err != nil {
		return nil, oops.Wrapf(err, "open session")
	}
	if err := checkStatus(resp.Status); err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return &Conn{
		client:  c.client,
		session: resp.SessionHandle,
	}, nil
}

func (c *Connector) Driver() driver.Driver {
	return c.driver
}

type Conn struct {
	client  tcliservice.TCLIService
	session *tcliservice.TSessionHandle

	// operation is set when there is inflight operation.
	operation *tcliservice.TOperationHandle
}

var _ driver.Conn = &Conn{}
var _ driver.QueryerContext = &Conn{}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return nil, oops.Errorf("not implemented")
}

func (c *Conn) Begin() (driver.Tx, error) {
	return nil, oops.Errorf("not implemented")
}

func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) != 0 {
		return nil, oops.Errorf("query argument not supported")
	}

	// Pass through timeout if set.
	var timeoutSeconds int64
	if deadline, ok := ctx.Deadline(); ok {
		timeoutSeconds = int64(math.Ceil(deadline.Sub(time.Now()).Seconds()))
	}

	resp, err := c.client.ExecuteStatement(ctx, &tcliservice.TExecuteStatementReq{
		SessionHandle: c.session,
		Statement:     query,
		QueryTimeout:  timeoutSeconds,
		RunAsync:      true,
	})
	if err != nil {
		return nil, oops.Wrapf(err, "Execute: %s", query)
	}
	if err := checkStatus(resp.Status); err != nil {
		return nil, oops.Wrapf(err, "")
	}

	// Record current operation.
	c.operation = resp.OperationHandle

	if err := waitForOperation(ctx, c.client, c.operation); err != nil {
		return nil, err
	}

	return newDatabricksRowReader(ctx, c.client, c.operation)
}

func (c *Conn) ResetSession(ctx context.Context) error {
	if c.operation == nil {
		return nil
	}
	defer func() {
		c.operation = nil
	}()

	resp, err := c.client.CloseOperation(ctx, &tcliservice.TCloseOperationReq{
		OperationHandle: c.operation,
	})
	if err != nil {
		return oops.Wrapf(err, "CancelOperation: %s", c.operation.String())
	}
	if err := checkStatus(resp.Status); err != nil {
		return oops.Wrapf(err, "")
	}

	return nil
}

func (c *Conn) Close() error {
	if c.session == nil {
		return nil
	}
	defer func() {
		c.session = nil
	}()

	// The comment for Close() says "Drivers must ensure all network calls made by
	// Close do not block indefinitely (e.g. apply a timeout)."
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.client.CloseSession(ctx, &tcliservice.TCloseSessionReq{
		SessionHandle: c.session,
	})
	if err != nil {
		return oops.Wrapf(err, "close session")
	}
	if err := checkStatus(resp.Status); err != nil {
		return oops.Wrapf(err, "close session")
	}
	return nil
}
