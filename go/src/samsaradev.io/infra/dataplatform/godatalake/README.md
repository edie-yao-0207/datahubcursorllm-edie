# GoDataLake - Data Lake Query Client

## Overview

The GoDataLake package provides a secure client for executing SQL queries against data lake compute platforms like Databricks. It supports parameterized queries to help prevent SQL injection attacks and provides a clean interface for query execution.

## Setup with Fx Dependency Injection

### Dependencies Structure

The recommended way to use GoDataLake is through fx dependency injection. First, define your dependencies:

```go
type Params struct {
    fx.In
    GoDataLake *godatalake.GoDataLake
}

type MyService struct {
    goDataLake *godatalake.GoDataLake
}

func NewMyService(params Params) *MyService {
    return &MyService{
        goDataLake: params.GoDataLake,
    }
}
```

### Registration

Register your service with fx:

```go
func init() {
    fxregistry.MustRegisterDefaultConstructor(NewMyService)
}
```

## Basic Usage

### Executing Queries

#### Simple Queries

For queries without parameters:

```go
func (s *MyService) GetAllTripsForOrg(ctx context.Context, orgID int64, date string) error {
    query := "SELECT count(1) FROM trips2db_shards.trips"
    result, err := s.goDataLake.Client.Query(ctx, query)
    if err != nil {
        return err
    }
    defer result.Close()
    
    // Process results...
    return nil
}
```

#### Parameterized Queries

For queries with parameters (recommended for dynamic values):

```go
func (s *MyService) GetTripsByOrgAndDevice(ctx context.Context, orgID int64, deviceID int64, date string) error {
    query := "SELECT * FROM trips2db_shards.trips WHERE org_id = ? AND device_id = ? AND date = ?"
    result, err := s.goDataLake.Client.Query(ctx, query, orgID, deviceID, date)
    if err != nil {
        return err
    }
    defer result.Close()
    
    // Process results...
    return nil
}
```

You can also pass in a slice of arguments and use variadic argument/slice expansion like this:
```go
func (s *MyService) GetTripsByOrgAndDevice(ctx context.Context, orgID int64, deviceID int64, date string) error {
    query := "SELECT * FROM trips2db_shards.trips WHERE org_id = ? AND device_id = ? AND date = ?"
    args := []interface{}{orgId, deviceId, date} // or dynamically build this args slice in your code
    result, err := s.goDataLake.Client.Query(ctx, query, args...)

    // ... same as above
}
```

#### Queries with Options

For queries that need custom configuration:

```go
func (s *MyService) GetTripsWithCustomWarehouse(ctx context.Context, orgID int64, startMs int64, date string) error {
    opts := &godatalake.QueryOptions{
        WarehouseID: "custom-warehouse-id",
    }

    query := "SELECT * FROM trips2db_shards.trips WHERE org_id = ? AND start_ms > ? AND date = ?"
    args := []interface{}{orgID, startMs, date}
    result, err := s.goDataLake.Client.QueryWithOpts(ctx, query, args, opts)
    if err != nil {
        return err
    }
    defer result.Close()
    
    // Process results...
    return nil
}
```

## Processing Results

### Iterating Through Results

```go
func (s *MyService) ProcessTripsForOrg(ctx context.Context, orgID int64, date string) error {
    query := "SELECT org_id, device_id, driver_id, proto.distance_meters FROM trips2db_shards.trips WHERE org_id = ? AND date = ?"
    result, err := s.goDataLake.Client.Query(ctx, query, orgID, date)
    if err != nil {
        return err
    }
    defer result.Close()

    var tripOrgID int64
    var deviceID int64
    var driverID int64
    var distanceMeters float64

    for result.Next() {
        err := result.Scan(&tripOrgID, &deviceID, &driverID, &distanceMeters)
        if err != nil {
            return err
        }
        fmt.Printf("Trip - Org: %d, Device: %d, Driver: %d, Distance: %.2f meters\n", tripOrgID, deviceID, driverID, distanceMeters)
    }
    
    return nil
}
```

### Getting Column Information

```go
func (s *MyService) InspectQueryColumnsForOrg(ctx context.Context, orgID int64, date string) error {
    query := "SELECT * FROM trips2db_shards.trips WHERE org_id = ? AND date = ? LIMIT 1"
    result, err := s.goDataLake.Client.Query(ctx, query, orgID, date)
    if err != nil {
        return err
    }
    defer result.Close()

    columns, err := result.Columns()
    if err != nil {
        return err
    }
    fmt.Printf("Query returned %d columns: %v\n", len(columns), columns)
    
    return nil
}
```

## Security Best Practices

### _Almost_ Always Filter by org_id

**Critical**: You should probably always filter your queries by `org_id` to ensure data isolation. This prevents accidental cross-organization data access and maintains proper data boundaries.

```go
// ✅ GOOD - Always filter by org_id for tables that have it
func (s *MyService) GetTripsForOrg(ctx context.Context, orgID int64, date string) error {
    query := "SELECT * FROM trips2db_shards.trips WHERE org_id = ? AND date = ?"
    result, err := s.goDataLake.Client.Query(ctx, query, orgID, date)
    // ...
}
```

#### Exception: Aggregated Tables

Only skip `org_id` filtering when working with pre-aggregated tables that don't contain `org_id` (because they aggregate across organizations):

```go
// ✅ ACCEPTABLE - Aggregated table without org_id
func (s *MyService) GetCohortBenchmarks(ctx context.Context, cohortID string, date string) error {
    query := "SELECT metric_type, mean FROM product_analytics_staging.stg_cohort_benchmarks WHERE cohort_id = ? AND date = ?"
    result, err := s.goDataLake.Client.Query(ctx, query, cohortID, date)
    if err != nil {
        return err
    }
    defer result.Close()
    
    var metricType string
    var mean float64
    for result.Next() {
        err := result.Scan(&metricType, &mean)
        if err != nil {
            return err
        }
        fmt.Printf("Metric: %s, Mean: %.2f\n", metricType, mean)
    }
    
    return nil
}
```

### Always Filter by Partition Keys

**Performance**: Always filter your queries by the table's partition keys to ensure optimal query performance. Most Samsara tables are partitioned by `date`, so always include date filters in your WHERE clauses, but look at the table you are querying to be sure.

```go
// ✅ GOOD - Filter by partition key (date) for optimal performance
func (s *MyService) GetOptimalTrips(ctx context.Context, orgID int64, date string) error {
    query := "SELECT * FROM trips2db_shards.trips WHERE org_id = ? AND date = ?"
    result, err := s.goDataLake.Client.Query(ctx, query, orgID, date)
    // ...
}

// ❌ BAD - No partition filtering leads to slow queries
func (s *MyService) GetSlowTrips(ctx context.Context, orgID int64) error {
    // This will scan all partitions - very slow!
    query := "SELECT * FROM trips2db_shards.trips WHERE org_id = ?"
    result, err := s.goDataLake.Client.Query(ctx, query, orgID)
    // ...
}
```

**Important**: Always understand how your target table is partitioned. While most tables use `date` partitioning, some may use different schemes. Check the table's partitioning strategy and always include those partition keys in your WHERE clause for optimal performance.

### Always Use Parameterized Queries

**Critical**: When incorporating user input or external data, always use parameterized queries with `?` placeholders:

```go
// ✅ SECURE - Use parameterized queries
func (s *MyService) GetTripSecurely(ctx context.Context, orgID int64, date string) error {
    query := "SELECT * FROM trips2db_shards.trips WHERE org_id = ? AND date = ?"
    result, err := s.goDataLake.Client.Query(ctx, query, orgID, date)
    if err != nil {
        return err
    }
    defer result.Close()
    
    // Process results...
    return nil
}
```

### Never Concatenate User Input

**Never** build SQL queries by concatenating strings with user input:

```go
// ❌ DANGEROUS - Vulnerable to SQL injection
func (s *MyService) GetTripUnsafely(ctx context.Context, orgID string, date string) error {
    // DON'T DO THIS - orgID could be "1 OR 1=1" or "1; DROP TABLE trips2db_shards.trips"
    query := fmt.Sprintf("SELECT * FROM trips2db_shards.trips WHERE org_id = %s AND date = '%s'", orgID, date)
    result, err := s.goDataLake.Client.Query(ctx, query) // DANGEROUS!
    // ...
}
```

### Parameter Types

The client supports various parameter types:

```go
func (s *MyService) GetTripsWithVariousTypes(ctx context.Context, orgID int64, minDistance float64, hasDriver bool, afterTime time.Time, date string) error {
    query := "SELECT * FROM trips2db_shards.trips WHERE org_id = ? AND proto.distance_meters > ? AND driver_id IS NOT NULL = ? AND start_ms > ? AND date = ?"
    result, err := s.goDataLake.Client.Query(ctx, query, orgID, minDistance, hasDriver, afterTime.Unix()*1000, date)
    if err != nil {
        return err
    }
    defer result.Close()
    
    // Process results...
    return nil
}
```

### Input Validation

Even with parameterized queries, validate input before using it:

```go
func (s *MyService) GetTripWithValidation(ctx context.Context, orgID int64, date string) error {
    if orgID <= 0 {
        return errors.New("invalid org ID")
    }
    if date == "" {
        return errors.New("date is required")
    }
    
    query := "SELECT * FROM trips2db_shards.trips WHERE org_id = ? AND date = ?"
    result, err := s.goDataLake.Client.Query(ctx, query, orgID, date)
    if err != nil {
        return err
    }
    defer result.Close()
    
    // Process results...
    return nil
}
```

## Complete Example

```go
package main

import (
    "context"
    "fmt"
    "log"
    
    "go.uber.org/fx"
    "samsaradev.io/infra/dataplatform/godatalake"
    "samsaradev.io/infra/fxregistry"
)

// Define your service dependencies
type TripServiceParams struct {
    fx.In
    GoDataLake *godatalake.GoDataLake
}

// Define your service struct
type TripService struct {
    goDataLake *godatalake.GoDataLake
}

// Constructor function
func NewTripService(params TripServiceParams) *TripService {
    return &TripService{
        goDataLake: params.GoDataLake,
    }
}

// Business logic method
func (s *TripService) GetRecentTrips(ctx context.Context, orgID int64, deviceID int64, date string) error {
    // Validate input
    if orgID <= 0 {
        return fmt.Errorf("invalid org ID: %d", orgID)
    }
    if deviceID <= 0 {
        return fmt.Errorf("invalid device ID: %d", deviceID)
    }
    if date == "" {
        return fmt.Errorf("date is required")
    }
    
    // Use parameterized query for security
    query := `
        SELECT org_id, device_id, driver_id, start_ms, proto.distance_meters
        FROM trips2db_shards.trips 
        WHERE org_id = ? AND device_id = ? AND date = ?
        ORDER BY start_ms DESC
        LIMIT 10
    `
    
    result, err := s.goDataLake.Client.Query(ctx, query, orgID, deviceID, date)
    if err != nil {
        return fmt.Errorf("failed to execute query: %w", err)
    }
    defer result.Close()
    
    // Process results
    var tripOrgID int64
    var tripDeviceID int64
    var driverID int64
    var startMs int64
    var distanceMeters float64
    
    fmt.Printf("Recent trips for org %d, device %d:\n", orgID, deviceID)
    for result.Next() {
        err := result.Scan(&tripOrgID, &tripDeviceID, &driverID, &startMs, &distanceMeters)
        if err != nil {
            return fmt.Errorf("failed to scan result: %w", err)
        }
        fmt.Printf("  Trip - Driver: %d, Start: %d, Distance: %.2f meters\n", driverID, startMs, distanceMeters)
    }
    
    return nil
}

// Register the service with fx
func init() {
    fxregistry.MustRegisterDefaultConstructor(NewTripService)
}

// Example usage in a workflow activity or other service
func ExampleUsage() {
    // In real code, this would be injected via fx dependency injection
    // You would receive a *TripService instance through your constructor
    var tripService *TripService
    
    ctx := context.Background()
    err := tripService.GetRecentTrips(ctx, 123, 456, "2024-01-15")
    if err != nil {
        log.Fatal(err)
    }
}
```

## Configuration Options

### QueryOptions

The `QueryOptions` struct allows you to customize query execution:

```go
type QueryOptions struct {
    // WarehouseID optionally overrides the default warehouse ID for this query
    WarehouseID string
}
```

### Usage with Custom Warehouse

```go
func (s *MyService) QueryTripsWithCustomWarehouse(ctx context.Context, orgID int64, startMs int64, date string) error {
    opts := &godatalake.QueryOptions{
        WarehouseID: "high-performance-warehouse",
    }

    query := "SELECT COUNT(*) FROM trips2db_shards.trips WHERE org_id = ? AND start_ms >= ? AND date = ?"
    args := []interface{}{orgID, startMs, date}
    result, err := s.goDataLake.Client.QueryWithOpts(ctx, query, args, opts)
    if err != nil {
        return err
    }
    defer result.Close()
    
    // Process results...
    return nil
}
```

## Error Handling

Always handle errors appropriately:

```go
func (s *MyService) QueryTripsWithProperErrorHandling(ctx context.Context, orgID int64, date string) error {
    query := "SELECT org_id, device_id, driver_id FROM trips2db_shards.trips WHERE org_id = ? AND date = ?"
    result, err := s.goDataLake.Client.Query(ctx, query, orgID, date)
    if err != nil {
        return fmt.Errorf("database query failed: %w", err)
    }
    defer result.Close()

    for result.Next() {
        var tripOrgID int64
        var deviceID int64
        var driverID int64
        err := result.Scan(&tripOrgID, &deviceID, &driverID)
        if err != nil {
            return fmt.Errorf("failed to scan row: %w", err)
        }
        // Process row
    }
    
    return nil
}
```

## Important Notes

- Always call `result.Close()` to free resources
- Use `defer result.Close()` immediately after checking for query errors
- Parameters are automatically escaped to prevent SQL injection
- The client supports various data types for parameters (int, string, bool, time.Time, etc.)
- Use fx dependency injection for proper service setup and testability
- Register your constructors with `fxregistry.MustRegisterDefaultConstructor`
- **_Almost_ Always filter by `org_id`** for data isolation (except for pre-aggregated tables)
- **Always filter by partition keys** (usually `date`) for optimal query performance
- **Understand your table's partitioning scheme** to write efficient queries

## Datadog metrics emitted

There are datadog metrics emitted automatically that you can use to help identify performance bottlenecks/issues.

If for example a query takes 2.5 seconds in Databricks but 7 seconds when using `godatalake`:

1. `godatalake.query.response_size_bytes` : how many bytes were in the response
2. `godatalake.query.total_duration_ms` : how long the query takes end to end (7s in example)
3. `godatalake.query.time_to_first_row_ms` : how long it takes for the first set of results to be streamed to the go side
4. `godatalake.query.next_duration_ms` : Network I/O + data parsing from Databricks format to Go driver values
5. `godatalake.query.scan_duration_ms` : Converting driver values to your specific Go types (string, int64, etc.)
6. `godatalake.query.implied_db_execution_ms` : approximately how long the query itself takes (should match the 2.5s in example)
7. `godatalake.query.implied_overhead_duration_ms` : how long the go level overhead is (basically 4 + 5; should be 4.5s in example)

Note, you should run `QueryWithOpts` and set `QueryIdentifier` if you'd like to get a tag on the datadog metric that let you filter for your query specifically. Otherwise, you can filter on the automatically added tags like: `warehouse_id`, `app`, and `service`