# Ducklake Connector Status

Last updated: 2026-04-07

## Read Side — Complete

The read path is fully implemented and tested.

### Data Access
- Catalog reads from Ducklake SQL metadata tables (JDBC/HikariCP, validated with SQLite and PostgreSQL backends).
- Snapshot-scoped reads for current snapshot and table-version queries (`FOR VERSION AS OF`, `FOR TIMESTAMP AS OF`), with optional session/catalog pinning.
- Parquet data files read through Trino's native Parquet reader.
- Inlined data read directly from the metadata catalog (DuckLake's default for tables with <=10 rows).
- Merge-on-read delete file filtering.
- Schema evolution: missing columns return NULLs.

### Query Optimization
- **File-level pruning**: eliminates whole Parquet files via `ducklake_file_column_stats` min/max.
- **Partition pruning**: identity and temporal transforms via `ducklake_partition_info` / `ducklake_file_partition_value`.
- **Row-group pruning**: Parquet footer statistics checked via `getFilteredRowGroups()`.
- **Page-level filtering**: Parquet page indexes passed to `ParquetReader` when available.
- **Dynamic filters**: intersected with file stats domain at page source creation.
- **Table statistics**: exposed via `ducklake_table_stats` + typed aggregated column min/max for cost-based optimization.
- `applyFilter` planner hook splits predicates into enforced (partition) and unenforced (engine-verified).

### Type Support
- Full: boolean, tinyint, smallint, integer, bigint, real, double, decimal, varchar, varbinary, date, time, timestamp, timestamptz, uuid.
- Full: arrays, structs/rows, maps, nested combinations.
- Degraded: `json` -> VARCHAR, `variant` -> VARCHAR, geometry family -> VARBINARY.
  These types are readable but lack type-specific operators and functions.

### Test Coverage
- 220 tests, 0 failures (1 skipped SQLite-only test) across 9 test classes.
- `TestDucklakeIntegration`: 142 end-to-end SQL tests via `DucklakeQueryRunner`.
- 15 test tables covering primitives, arrays, structs, maps, partitioning (identity/temporal/daily), schema evolution, NULLs, empty tables, delete files, multi-file scans, complex NULL patterns, and inlined data.
- Unit tests for catalog, split manager, partition pruning, page source provider, delete file handling, plugin wiring.
- Test backend matrix:
  - Default: SQLite (`mvn test`)
  - PostgreSQL: `mvn test -Dducklake.test.catalog-backend=postgresql` (uses Testcontainers, requires Docker)
  - `TestDucklakeDeleteFileHandling` is SQLite-only because it edits SQLite catalog files directly.

## Known Gaps and Concerns

### Temporal partition transform values (open issue)
DuckDB's ducklake extension writes literal calendar values (e.g., year=2023, month=6) to `ducklake_file_partition_value` instead of the epoch-based values described in the spec (e.g., year=53, month=641). Our implementation follows DuckDB's actual behavior. If the spec is updated or DuckDB changes behavior in a future release, this code will need reconciliation. See [REPORT_DUCKLAKE_PARTITION_PROB.md](REPORT_DUCKLAKE_PARTITION_PROB.md) and [duckdb/ducklake-web#312](https://github.com/duckdb/ducklake-web/issues/312).

### Time travel
`FOR VERSION AS OF` and `FOR TIMESTAMP AS OF` are implemented through Trino table-version arguments in `DucklakeMetadata#getTableHandle(...)`.
Supported version pointer types:
- `TARGET_ID`: `tinyint`, `smallint`, `integer`, `bigint` (mapped to snapshot ID lookup)
- `TEMPORAL`: `date`, `timestamp`, `timestamp with time zone` (resolved to latest snapshot at or before pointer time)

Precise errors are returned for unsupported pointer types, missing snapshot IDs, and timestamps earlier than first snapshot.
Session/catalog snapshot pinning remains implemented (`ducklake.read_snapshot_id`, `ducklake.read_snapshot_timestamp`, `ducklake.default-snapshot-id`, `ducklake.default-snapshot-timestamp`) with precedence `query > session > catalog > current`.

### Data inlining
Trino now supports mixed-mode reads when a snapshot has both active Parquet files and active inlined rows: split planning emits both split types and reads them as a union. Merge-on-read delete filtering remains in place for Parquet splits, while inlined row visibility is still governed by `begin_snapshot`/`end_snapshot` filtering in metadata reads.

Stale inlined metadata pointers remain non-fatal. If metadata references an inlined table that is missing or has no active rows at the snapshot, the connector does not fail and does not emit dead inlined splits for mixed Parquet scans.

### Degraded type semantics
`json`, `variant`, and geometry types are stored as VARCHAR/VARBINARY. No type-specific functions or operators. Variant shredding is not implemented.

### Catalog backend
Catalog implementation now routes by JDBC URL (`jdbc:sqlite:` / `jdbc:postgresql:`) with a shared JDBC code path. SQLite and PostgreSQL are both covered by tests. DuckDB-as-catalog-backend remains unverified.

## Write Side — Data Writes Implemented

### DDL (metadata-only writes)

Implemented through snapshot-scoped catalog commits:

- Views: `CREATE VIEW`, `DROP VIEW`
- Schemas: `CREATE SCHEMA`, `DROP SCHEMA` (non-empty schema drop rejected)
- Tables: `CREATE TABLE`, `DROP TABLE`
  - supports nested type metadata mapping (`ARRAY`, `ROW`, `MAP`)
  - supports partition spec parsing from `WITH (partitioned_by = ARRAY[...])`
  - `SAVE MODE REPLACE` remains unsupported

### DML (data writes)

`INSERT` and `CREATE TABLE AS SELECT` are now implemented for unpartitioned tables:

- `DucklakePageSink` writes Parquet data files via Trino's `ParquetWriter` (ZSTD compression, configurable block/page size).
- `DucklakePageSinkProvider` handles both INSERT and CTAS through a shared `DucklakeWritableTableHandle`.
- `DucklakeStatsExtractor` extracts per-column statistics from Parquet footer metadata.
- `DucklakePathResolver` resolves table data paths from catalog/schema/table hierarchy.
- `DucklakeMetadata` implements `beginInsert`/`finishInsert` and `beginCreateTable`/`finishCreateTable`.
- `JdbcDucklakeCatalog.commitInsert()` atomically commits data file rows, file column stats, and table stats updates in a single write transaction.
- Abort path deletes written files on failure.
- File rotation by target file size (configurable via `ParquetWriterConfig`).

### Cross-engine compatibility

Catalog metadata format has been aligned with DuckDB's implementation for interoperability:
- Column metadata: 1-based `column_order`, `default_value='NULL'`, `default_value_type='literal'`, `default_value_dialect='duckdb'`
- Stats: `ducklake_table_stats` created on first INSERT (not CREATE TABLE), `contains_nan` stored as NULL for non-floating types
- Footer size, schema_versions.table_id, and UUID type handling are all DuckDB-compatible
- See [REPORT_CROSS_ENGINE_WRITE.md](REPORT_CROSS_ENGINE_WRITE.md) for open issues filed with the DuckDB team

### Not yet implemented

- Partitioned data writes (partition value computation + `ducklake_file_partition_value` rows)
- Commit-failure file cleanup (abort cleanup exists, commit-failure cleanup does not)
- `DELETE`, `UPDATE`, `MERGE`
- `ALTER TABLE` family
- Maintenance operations (optimize, rewrite, expire snapshots, etc.)

### Test coverage

- Default test backend: **PostgreSQL** (via Testcontainers, requires Docker)
- Override with `-Dducklake.test.catalog-backend=sqlite` or `duckdb`
- DDL: `TestDucklakeDDLIntegration` (12 tests)
- Data writes: `TestDucklakeWriteIntegration` (19 tests), `TestDucklakeWriteFragment`, `TestDucklakeStatsExtractor`
- Cross-engine: `TestDucklakeCrossEngineCompatibility` (partial — DuckDB crash fixed, SQLite visibility issue remains for data reads)
- All tests pass on PostgreSQL, SQLite, and DuckDB backends (192+ tests each)
