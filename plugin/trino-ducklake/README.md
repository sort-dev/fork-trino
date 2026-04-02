# Trino Ducklake Connector

Read-only Trino connector for the [Ducklake](https://ducklake.select/) table format.

Reads Ducklake metadata from a JDBC catalog database (SQLite or PostgreSQL) and data from Parquet files via Trino's native Parquet reader. Supports data inlined in the metadata catalog (DuckLake's default for small tables).

## Documentation

- [STATUS.md](STATUS.md) — Current implementation state, gaps, and concerns.
- [REUSE.md](REUSE.md) — What we reuse from Trino/Iceberg and what's custom.
- [REPORT_DUCKLAKE_PARTITION_PROB.md](REPORT_DUCKLAKE_PARTITION_PROB.md) — Open issue: temporal partition values in DuckDB don't match spec.
- [Ducklake Specification](ducklake-spec/index.md) - includes ducklake spec and duckdb ducklake extension docs

## Configuration

Example `etc/catalog/ducklake.properties`:

```properties
connector.name=ducklake
ducklake.catalog.database-url=jdbc:sqlite:/path/to/catalog.db
ducklake.data-path=/path/to/data
ducklake.catalog.max-connections=10
```

PostgreSQL catalog configuration:

```properties
connector.name=ducklake
ducklake.catalog.database-url=jdbc:postgresql://postgres-host:5432/ducklake
ducklake.catalog.database-user=ducklake
ducklake.catalog.database-password=ducklake
ducklake.data-path=/path/to/data
ducklake.catalog.max-connections=10
```

Optional catalog-level snapshot pinning:

```properties
# Use exactly one of these
ducklake.default-snapshot-id=123
ducklake.default-snapshot-timestamp=2024-01-15T12:00:00Z
```

`ducklake.default-snapshot-id` and `ducklake.default-snapshot-timestamp` are mutually exclusive.

Temporal partition encoding options:

```properties
# default: calendar
ducklake.temporal-partition-encoding=calendar

# default: true
ducklake.temporal-partition-encoding-read-leniency=true
```

Semantics:

- `ducklake.temporal-partition-encoding` sets the strict encoding (`calendar` or `epoch`).
- `ducklake.temporal-partition-encoding-read-leniency=true` enables safe dual-interpretation reads:
  prune only when it is safe, i.e. both encodings exclude a file (or one encoding is impossible and the other excludes).
- `ducklake.temporal-partition-encoding-read-leniency=false` uses strict pruning under the configured encoding only.

## Snapshot Reads (Point In Time)

DuckLake read snapshot precedence is:

1. Query override (`FOR VERSION AS OF ...` / `FOR TIMESTAMP AS OF ...`)
2. Session property (`ducklake.read_snapshot_id` / `ducklake.read_snapshot_timestamp`)
3. Catalog default (`ducklake.default-snapshot-id` / `ducklake.default-snapshot-timestamp`)
4. Current snapshot

Query-level time travel:

```sql
SELECT * FROM test_schema.schema_evolution_table FOR VERSION AS OF 123;

SELECT * FROM test_schema.schema_evolution_table
FOR TIMESTAMP AS OF from_iso8601_timestamp('2024-01-15T12:00:00Z');
```

Session-level snapshot pinning:

```sql
SET SESSION ducklake.read_snapshot_id = 123;
SELECT count(*) FROM test_schema.schema_evolution_table;
RESET SESSION ducklake.read_snapshot_id;
```

```sql
SET SESSION ducklake.read_snapshot_timestamp = '2024-01-15T12:00:00Z';
SELECT count(*) FROM test_schema.schema_evolution_table;
RESET SESSION ducklake.read_snapshot_timestamp;
```

`ducklake.read_snapshot_id` and `ducklake.read_snapshot_timestamp` are mutually exclusive.

## Metadata Tables

The connector supports Iceberg-style `$` metadata tables:

- `$files`
- `$snapshots`
- `$current_snapshot`
- `$snapshot_changes`

In Trino SQL, `$` names must be quoted:

```sql
DESCRIBE test_schema."simple_table$files";
SELECT data_file_id, path, file_format, record_count
FROM test_schema."simple_table$files";
```

```sql
SELECT snapshot_id, snapshot_time
FROM test_schema."simple_table$snapshots"
ORDER BY snapshot_id DESC;

SELECT *
FROM test_schema."simple_table$current_snapshot";

SELECT snapshot_id, changes_made, author, commit_message
FROM test_schema."simple_table$snapshot_changes"
ORDER BY snapshot_id DESC;
```

Column shapes:

- `$files`: `data_file_id`, `path`, `file_format`, `record_count`, `file_size_bytes`, `row_id_start`, `partition_id`, `delete_file_path`
- `$snapshots`: `snapshot_id`, `snapshot_time`, `schema_version`, `next_catalog_id`, `next_file_id`
- `$current_snapshot`: same columns as `$snapshots`
- `$snapshot_changes`: `snapshot_id`, `changes_made`, `author`, `commit_message`, `commit_extra_info`

## Build and Test

```bash
cd plugin/trino-ducklake
mvn test
```

Run the same test suite against PostgreSQL catalog backend:

```bash
cd plugin/trino-ducklake
mvn test -Dducklake.test.catalog-backend=postgresql
```

Notes:
- `ducklake.test.catalog-backend` defaults to `sqlite`.
- PostgreSQL test mode uses Testcontainers and requires a working Docker environment.
- `TestDucklakeDeleteFileHandling` is SQLite-only (it mutates SQLite catalog files directly).
- Current local workflow is module-scoped: run from `plugin/trino-ducklake` using `../../mvnw ...`.
- This module disables `ReportLeakedContainers` by default during tests to avoid Podman Docker API compatibility noise
  (`unknown container state: restarting`).
  Re-enable it explicitly with `-DReportLeakedContainers.disabled=false` if you want leaked-container checks.
- For faster local loops, skip expensive checks:

```bash
cd plugin/trino-ducklake
../../mvnw -Dair.check.skip-all -Dtest=TestDucklakeIntegration test
```

`-Dair.check.skip-all` skips checkstyle/sortpom/dependency/modernizer/enforcer-style validation phases and runs tests directly.

Targeted runs:

```bash
# Full integration tests
mvn test -Dtest=TestDucklakeIntegration

# Catalog metadata
mvn test -Dtest=TestDucklakeCatalog

# Split pruning + partition pruning
mvn test -Dtest=TestDucklakeSplitManager,TestDucklakePartitionPruning

# Page source + delete handling
mvn test -Dtest=TestDucklakePageSourceProvider,TestDucklakeDeleteFileHandling

# Targeted PostgreSQL backend runs
mvn test -Dducklake.test.catalog-backend=postgresql -Dtest=TestDucklakeCatalog,TestDucklakeSplitManager

# Targeted DuckDB backend runs
mvn test -Dducklake.test.catalog-backend=duckdb -Dtest=TestDucklakeCatalog,TestDucklakeSplitManager
```
