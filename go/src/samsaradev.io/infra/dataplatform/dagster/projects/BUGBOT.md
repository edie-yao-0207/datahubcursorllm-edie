# Bugbot review guide: Dagster data model dependencies → Go registries

## Purpose

This directory contains Dagster projects (e.g. `datamodel/`, `dataweb/`, `ml/`) that declare dependencies on upstream source tables (KinesisStats and RDS tables). These dependencies must be reflected in the corresponding Go “deltalake” registries so ingestion/replication scheduling and metadata stay correct.

## What Bugbot must detect

When reviewing PRs that change any files under this directory, Bugbot must check whether the PR introduces **new upstream dependencies** on:

- **KinesisStats tables**:
  - `kinesisstats.<table>`
  - `kinesisstats_history.<table>` (treat as `kinesisstats.<table>`)
- **RDS tables** (tables sourced from relational DBs that are replicated via `rdsdeltalake`)
  - Commonly appear as `spark.table("clouddb.<table>")`, `FROM clouddb.<table>`, etc. (DB name varies)

Bugbot should treat any of the following as a dependency addition:

- **SQL strings** containing `FROM`/`JOIN` against these databases
- **`spark.table("...")` calls** referencing these databases
- **Dagster dependency declarations** that embed table names (e.g. `AssetKey([...])`, `upstreams=[...]`)
- **String literals** containing `kinesisstats...` / `kinesisstats_history...` / `kinesisstats_window...` / `clouddb...` etc.

## Required follow-up when a new dependency is added

### KinesisStats

If a PR adds a new dependency on any `kinesisstats*` table, Bugbot must ensure it is marked as a data model table in the KinesisStats deltalake registry:

- **Registry location**:
  - `go/src/samsaradev.io/infra/dataplatform/ksdeltalake/registry.go`
  - and potentially other files in `go/src/samsaradev.io/infra/dataplatform/ksdeltalake/` (some stats are defined via reflection in `registrystat*.go`)
- **Field to verify/set**: `DataModelStat: true`

If the stat is defined in a `registrystat*.go` file (reflection-defined), ensure the returned `Stat{...}` includes `DataModelStat: true` as well.

### RDS

If a PR adds a new dependency on an RDS table that is replicated via `rdsdeltalake`, Bugbot must ensure it is marked as a data model table in the RDS deltalake registry:

- **Registry location**:
  - `go/src/samsaradev.io/infra/dataplatform/rdsdeltalake/registry.go`
- **Field to verify/set**: `DataModelTable: true`

## What Bugbot should comment on GitHub

If Bugbot finds a new `kinesisstats*` dependency and cannot find a corresponding `DataModelStat: true` in `ksdeltalake`, it should leave a blocking comment like:

> This PR adds a new Dagster dependency on `kinesisstats.<table>` (or `kinesisstats_history.<table>`). Please update `go/src/samsaradev.io/infra/dataplatform/ksdeltalake/` to set `DataModelStat: true` for this table in the registry (or `registrystat*.go` if reflection-defined).

If Bugbot finds a new RDS dependency and cannot find a corresponding `DataModelTable: true` in `rdsdeltalake`, it should leave a blocking comment like:

> This PR adds a new Dagster dependency on `<db>.<table>` (RDS source). Please update `go/src/samsaradev.io/infra/dataplatform/rdsdeltalake/registry.go` to set `DataModelTable: true` for this table.

