# Bugbot review guide: dataweb dependencies → Go registries

## Purpose

The Dataweb Dagster code under this directory declares dependencies on upstream source tables (KinesisStats and RDS tables). Whenever a new upstream dependency is added here, the corresponding Go deltalake registries must be updated so the ingestion/replication layer correctly treats the table as part of the data model.

## What Bugbot must detect

When reviewing PRs that change files under `dataplatform/dataweb/`, Bugbot must check whether the PR introduces **new upstream dependencies** on:

- **KinesisStats tables**:
  - `kinesisstats.<table>`
  - `kinesisstats_history.<table>` (treat as `kinesisstats.<table>`)
- **RDS tables** (tables sourced from relational DBs replicated via `rdsdeltalake`)
  - Often appear as `spark.table("<db>.<table>")` or `FROM <db>.<table>` (DB name varies, e.g. `clouddb`, `productsdb`, etc.)

Bugbot should treat any of the following as a dependency addition:

- **SQL strings** containing `FROM`/`JOIN` against these databases
- **`spark.table("...")` calls** referencing these databases
- **Dagster/table decorator dependency declarations** (`upstreams=[...]`, `AssetKey([...])`, etc.)
- **String literals** containing `kinesisstats...` / `kinesisstats_history...` / `kinesisstats_window...` / `<db>...`

## Required follow-up when a new dependency is added

### KinesisStats

If a PR adds a new dependency on any `kinesisstats*` table, Bugbot must ensure it is marked as a data model stat in:

- **Registry location**:
  - `go/src/samsaradev.io/infra/dataplatform/ksdeltalake/registry.go`
  - and potentially other files in `go/src/samsaradev.io/infra/dataplatform/ksdeltalake/` (some stats are defined via reflection in `registrystat*.go`)
- **Field to verify/set**: `DataModelStat: true`

Notes:
- `kinesisstats_history.<table>` and `kinesisstats_window.<table>` should map to the base stat name `<table>` for registry purposes.

### RDS

If a PR adds a new dependency on an RDS table that is replicated via `rdsdeltalake`, Bugbot must ensure it is marked as a data model table in:

- **Registry location**:
  - `go/src/samsaradev.io/infra/dataplatform/rdsdeltalake/registry.go`
- **Field to verify/set**: `DataModelTable: true`

## What Bugbot should comment on GitHub

If Bugbot finds a new `kinesisstats*` dependency and cannot find a corresponding `DataModelStat: true` entry in `ksdeltalake`, it should leave a blocking comment like:

> This PR adds a new Dataweb dependency on `kinesisstats.<table>` (or `kinesisstats_history.<table>` / `kinesisstats_window.<table>`). Please update `go/src/samsaradev.io/infra/dataplatform/ksdeltalake/` to set `DataModelStat: true` for this stat (check `registry.go` and `registrystat*.go`).

If Bugbot finds a new RDS dependency and cannot find a corresponding `DataModelTable: true` entry in `rdsdeltalake`, it should leave a blocking comment like:

> This PR adds a new Dataweb dependency on `<db>.<table>` (RDS source). Please update `go/src/samsaradev.io/infra/dataplatform/rdsdeltalake/registry.go` to set `DataModelTable: true` for this table.

