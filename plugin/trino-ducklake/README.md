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

Targeted runs:

```bash
# Full integration tests (136 methods)
mvn test -Dtest=TestDucklakeIntegration

# Catalog metadata
mvn test -Dtest=TestDucklakeCatalog

# Split pruning + partition pruning
mvn test -Dtest=TestDucklakeSplitManager,TestDucklakePartitionPruning

# Page source + delete handling
mvn test -Dtest=TestDucklakePageSourceProvider,TestDucklakeDeleteFileHandling

# Targeted PostgreSQL backend runs
mvn test -Dducklake.test.catalog-backend=postgresql -Dtest=TestDucklakeCatalog,TestDucklakeSplitManager
```

197 tests across 7 test classes, 0 failures (SQLite backend).
