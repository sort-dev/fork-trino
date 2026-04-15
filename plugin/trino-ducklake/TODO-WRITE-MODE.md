# DuckLake Write Mode Plan (Trino Connector)

Last updated: 2026-04-11

## Objective

Implement write support in `trino-ducklake` in a compatibility-first order, with clear milestones and a full validation matrix across:

- Catalog backends: `postgresql` (primary; JDBC code path is generic)
- Engine interoperability scenarios:
  - DuckDB-created -> Trino-read
  - Trino-created -> Trino-read
  - Trino-created -> DuckDB-read

This plan is intentionally reuse-heavy: reuse existing Trino writer infrastructure wherever possible; keep DuckLake-specific code limited to catalog semantics and snapshot logic.

## Scope Order

### MVP (Write Mode v1) — Complete

1. `CREATE SCHEMA`, `DROP SCHEMA`
2. `CREATE TABLE`, `DROP TABLE`
3. `INSERT` into existing tables (unpartitioned and partitioned)
4. `CREATE TABLE AS SELECT` (CTAS, unpartitioned and partitioned)
5. Cross-engine compatibility tests green (10/10)

### Next

6. `DELETE`
7. `UPDATE` (as delete+insert)
8. `MERGE`
9. `ALTER TABLE` family (add/drop/rename column, partition evolution)

### Views (Implemented Baseline)

Views were the first write-side milestone and are now implemented:

- Read side: `listViews`, `getView`, `isView`, `getViews` in `DucklakeMetadata`
- Catalog model/API: `DucklakeView` + JDBC view queries
- Dialect handling: expose Trino dialect views, skip non-Trino dialects with logging
- Write side: `createView` and `dropView` commit snapshot-scoped catalog mutations

Still deferred for views:

- `renameView`
- view comment operations

## Completed Milestones

### M0: Catalog Write Contract + Transaction Primitive

- [x] `JdbcDucklakeCatalog` has shared `executeWriteTransaction(...)` helper and `DucklakeWriteTransaction` object.
- [x] JDBC-backed write transaction: single connection, `autoCommit=false`, explicit `commit`/`rollback`.

### M1: Connector Write Plumbing

- [x] `DucklakeWritableTableHandle` (implements both `ConnectorInsertTableHandle` and `ConnectorOutputTableHandle`)
- [x] `DucklakeWriteFragment` (serialized file metadata + stats + partition values)
- [x] `DucklakePageSinkProvider` with `PageIndexerFactory` injection
- [x] `DucklakePageSink` with multi-writer model, file rotation, abort rollback
- [x] `ClassLoaderSafeConnectorPageSinkProvider` wrapper in `DucklakeModule`
- [x] `ParquetWriterConfig` bound

### M2: DDL Metadata Writes

- [x] `CREATE SCHEMA` / `DROP SCHEMA` with snapshot commit
- [x] `CREATE TABLE` / `DROP TABLE` with full column metadata, partition specs, and DuckDB-compatible change strings

### M3: INSERT (Core Data Write)

- [x] Parquet file writing via `ParquetWriter` with ZSTD compression
- [x] Parquet `field_id` annotations from DuckLake `column_id` via `DucklakeParquetSchemaBuilder`
- [x] File-level column statistics extraction via `DucklakeStatsExtractor`
- [x] Metadata commit: `ducklake_data_file`, `ducklake_file_column_stats`, `ducklake_table_stats`, `ducklake_table_column_stats`
- [x] Abort path deletes written files
- [ ] Commit-failure file cleanup (relies on DuckLake orphan file maintenance)

### M4: CTAS

- [x] `beginCreateTable` / `finishCreateTable` with write fragments
- [x] Works for both unpartitioned and partitioned tables

### M5: Partitioning + Encoding

- [x] Partition spec persistence (`ducklake_partition_info`, `ducklake_partition_column`)
- [x] `DucklakePagePartitioner` routes rows to per-partition writers via `PageIndexerFactory`
- [x] `DucklakePartitionComputer` computes partition values for IDENTITY, YEAR, MONTH, DAY, HOUR transforms
- [x] Calendar encoding (DuckDB-compatible default) and epoch encoding support
- [x] `ducklake_file_partition_value` rows committed with data files
- [x] Null partition values supported
- [x] Partitioned file paths use subdirectories (`col=value/`)
- [x] `DucklakeTypeConverter.toDucklakeType()` handles nested types

## Remaining Milestones

### M6: Row-Level Mutations — Complete

- [x] `DELETE`: write delete parquet files + metadata rows in `ducklake_delete_file`.
- [x] `UPDATE`: implement as delete+insert in one atomic snapshot commit via `commitMerge`.
- [x] `MERGE`: compose insert/delete fragments in one atomic snapshot; supports WHEN MATCHED THEN UPDATE/DELETE and WHEN NOT MATCHED THEN INSERT.

### M7: ALTER + Hardening

- [ ] `ALTER TABLE ADD/DROP/RENAME COLUMN` with snapshot-versioned `ducklake_column`.
- [ ] Schema evolution metadata alignment (`ducklake_schema_versions`, stats resilience).
- [ ] Concurrency/conflict handling (optimistic commit conflict detection using snapshot lineage and changes).
- [ ] Performance pass (writer scaling, file size tuning, stats cost).

### M8: Maintenance Operations (Post-v1)

- [ ] Add maintenance verbs that map cleanly to Trino conventions:
  - `ALTER TABLE ... EXECUTE optimize` (DuckDB `merge_adjacent_files` equivalent)
  - `ALTER TABLE ... EXECUTE rewrite_data_files`
- [ ] Add connector procedures in `ducklake.system`:
  - `expire_snapshots`
  - `cleanup_old_files`
  - `remove_orphan_files`
  - `flush_inlined_data`
- [ ] Add result tables from procedures (rows affected/files deleted/bytes reclaimed).

## Trino Write SQL/API Surface

### Core SQL

- `CREATE SCHEMA`, `DROP SCHEMA`
- `CREATE TABLE`, `DROP TABLE`
- `INSERT`
- `CREATE TABLE AS SELECT`
- `DELETE`, `UPDATE`, `MERGE` (M6)

### Commit Context (DuckDB `set_commit_message` equivalent)

Session properties:
- `ducklake.commit_author`
- `ducklake.commit_message`
- `ducklake.commit_extra_info`

Optional convenience procedures:
- `CALL ducklake.system.set_commit_context(author => 'Pedro', message => 'Inserting myself', extra_info => '{\"foo\":7}')`
- `CALL ducklake.system.clear_commit_context()`

## Reality Check: Spec vs Actual Catalog Shape

Known differences between markdown spec and DuckDB-generated catalogs (all handled):

- `ducklake_schema_versions` has `table_id` in practice.
- `ducklake_column` has extra columns (`default_value_type`, `default_value_dialect`) in practice.
- `ducklake_snapshot_changes.changes_made` values include forms like `inlined_insert:...`, `inline_flush:...`, `merge_adjacent:...`.
- Temporal partition values follow DuckDB calendar semantics in metadata. Write path supports both calendar and epoch via config.

See [REPORT_CROSS_ENGINE_WRITE.md](REPORT_CROSS_ENGINE_WRITE.md) for spec issues filed with the DuckDB team.

## Test Plan

### Test suites

- `TestDucklakeWriteIntegration` (25 tests): DDL/DML correctness for unpartitioned and partitioned tables
- `TestDucklakeDeleteIntegration` (25 tests): DELETE (15), UPDATE (5), MERGE (5) — row-level mutations
- `TestDucklakeCrossEngineCompatibility` (10 tests): Trino writes -> DuckDB reads round-trips
- `TestDucklakeParquetSchemaBuilder` (6 tests): Parquet field_id assignment
- `TestDucklakePartitionComputer` (18 tests): partition value computation, both encodings
- `TestDucklakeWriteFragment` (5 tests): fragment JSON serialization
- `TestDucklakeDDLIntegration` (12 tests): schema/table DDL

### Existing suites (no regressions)

- `TestDucklakeIntegration` (145 tests): DuckDB-created -> Trino-read baseline
- `TestDucklakeCatalog`, `TestDucklakeSplitManager`, `TestDucklakePartitionPruning`, `TestDucklakePageSourceProvider`, `TestDucklakeDeleteFileHandling`

### Test commands

```bash
# Full suite
mvn test -f plugin/trino-ducklake/pom.xml -DforkCount=1

# Write tests only
mvn test -f plugin/trino-ducklake/pom.xml -Dtest=TestDucklakeWriteIntegration

# Cross-engine tests only
mvn test -f plugin/trino-ducklake/pom.xml -Dtest=TestDucklakeCrossEngineCompatibility

# Fast loop (skip validation)
cd plugin/trino-ducklake && ../../mvnw -Dair.check.skip-all -Dtest=TestDucklakeWriteIntegration test
```

## Definition of Done (Write Mode v1) — Complete

- [x] DDL (`CREATE/DROP SCHEMA`, `CREATE/DROP TABLE`) implemented and validated.
- [x] `INSERT` and `CTAS` implemented for unpartitioned and partitioned tables.
- [x] Parquet files include `field_id` for DuckDB column mapping.
- [x] Calendar and epoch temporal partition encoding supported.
- [x] 10 cross-engine compatibility tests green (Trino writes -> DuckDB reads).
- [x] No regressions on existing read-path suites.
- [x] 410 tests pass, 0 failures (including 25 row-level mutation tests).

## Risk Register

- [x] Schema drift between markdown spec and real extension output:
  Mitigated: side-by-side catalog comparison identified 10 metadata format differences, all fixed.
- [x] Cross-engine Parquet column mapping:
  Fixed: `DucklakeParquetSchemaBuilder` sets `field_id` on all Parquet fields from DuckLake `column_id`.
- [ ] Concurrency conflicts:
  MVP uses strict optimistic single-commit behavior; add retries/conflict codes in M7.
- [ ] Unsigned integer overflow:
  DuckLake `uint*` types are mapped to larger signed Trino types at read time. No range validation on writes from Trino. Low risk: only affects DuckDB-created tables with unsigned types.
- [ ] Commit-failure file cleanup:
  Files written before a failed commit become orphans. DuckLake's `ducklake_delete_orphaned_files()` maintenance procedure handles cleanup.
