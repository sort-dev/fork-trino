# DuckLake Write Mode Plan (Trino Connector)

Last updated: 2026-04-01

## Objective

Implement write support in `trino-ducklake` in a compatibility-first order, with clear milestones and a full validation matrix across:

- Catalog backends: `sqlite`, `duckdb`, `postgresql`
- Engine interoperability scenarios:
  - DuckDB-created -> Trino-read
  - Trino-created -> Trino-read
  - Trino-created -> DuckDB-read

This plan is intentionally reuse-heavy: reuse existing Trino writer infrastructure wherever possible; keep DuckLake-specific code limited to catalog semantics and snapshot logic.

## Scope Order

### MVP (Write Mode v1)

1. `CREATE SCHEMA`, `DROP SCHEMA`
2. `CREATE TABLE`, `DROP TABLE`
3. `INSERT` into existing tables
4. `CREATE TABLE AS SELECT` (CTAS)
5. Compatibility tests for all 9 scenarios (3 catalogs x 3 interoperability combos)

### Next

6. `DELETE`
7. `UPDATE` (as delete+insert)
8. `MERGE`
9. `ALTER TABLE` family (add/drop/rename column, partition evolution)

### First Write Item: Views (Read + Write Together)

Views are the simplest write operation — just catalog metadata, no parquet files, partitions, or schema evolution. Good entry point for proving out the write path before tackling data writes.

**Read side** (also in TODO-READ-MODE.md R7):
- Implement `listViews`, `getView`, `isView`, `getViews` in `DucklakeMetadata`
- Add `DucklakeView` model class, catalog query methods
- Filter by `dialect`: only expose views with Trino-compatible SQL (dialect = 'trino' or 'sql')
- DuckDB-dialect views are skipped with a log warning (future: transpiler)

**Write side**:
- Implement `createView` in `DucklakeMetadata`:
  - Store SQL with `dialect = 'trino'`
  - INSERT into `ducklake_view` with `begin_snapshot` = new snapshot, `end_snapshot` = NULL
  - Create new snapshot row + snapshot_changes entry (`created_view:<view_name>`)
- Implement `dropView`:
  - SET `end_snapshot` on existing view row
  - New snapshot + changes entry (`dropped_view:<view_name>`)
- Implement `renameView` (optional, lower priority)

**Why views first**:
- No file writing, no partition logic, no stats — pure catalog metadata mutation
- Exercises the snapshot commit path (read current → create next → write changes)
- Tests the write transaction abstraction from M0
- Immediately useful — views in DuckLake are a real user feature
- Read + write can be implemented and tested together as one unit

**Depends on**: M0 (catalog write transaction abstraction) — but views are simple enough that M0 can be scoped to just what views need.

## Trino Write SQL/API Surface (Proposed)

## Core SQL (P0/P1)

Use standard Trino DDL/DML surface (no DuckDB-specific syntax):

- `CREATE SCHEMA`, `DROP SCHEMA`
- `CREATE TABLE`, `DROP TABLE`
- `INSERT`
- `CREATE TABLE AS SELECT`
- `DELETE`, `UPDATE`, `MERGE` (after MVP)

## Commit Context (DuckDB `set_commit_message` equivalent)

DuckDB has:

```sql
CALL ducklake.set_commit_message(author, message, extra_info => ...);
```

Trino proposal:

- Session properties (simple path, works with standard SQL clients):
  - `ducklake.commit_author`
  - `ducklake.commit_message`
  - `ducklake.commit_extra_info`
- Optional convenience procedures:
  - `CALL ducklake.system.set_commit_context(author => 'Pedro', message => 'Inserting myself', extra_info => '{\"foo\":7}')`
  - `CALL ducklake.system.clear_commit_context()`

Write commit rule:

- Every snapshot write pulls commit context from session/procedure state and writes into `ducklake_snapshot_changes` (`author`, `commit_message`, `commit_extra_info`).

## Proposed Session Properties (Write + Maintenance)

Use typed properties rather than DuckDB-style generic `set_option(key, value)`:

- `ducklake.commit_author`
- `ducklake.commit_message`
- `ducklake.commit_extra_info`
- `ducklake.target_file_size_bytes` (writer/compaction target)
- `ducklake.rewrite_delete_threshold` (rewrite trigger)
- `ducklake.expire_older_than` (snapshot expiration default)
- `ducklake.delete_older_than` (file cleanup default)

## Snapshot/Time-Travel Interop

Write path depends on read-side time-travel features to validate snapshot correctness after commits.
Read surface and priorities are tracked in `TODO-READ-MODE.md`.

## DuckDB Extension Parity (Write + Maintenance)

| DuckDB feature/function | Trino equivalent (proposed) | Priority | Decision |
|---|---|---|---|
| `CALL catalog.set_commit_message(...)` | session commit properties + `system.set_commit_context` | P0 | Do |
| Transaction block (`BEGIN ... COMMIT`) | Trino transaction semantics | P0 | Do |
| `MERGE INTO` | Trino `MERGE INTO` via connector write SPI | P1 | Do |
| `CALL catalog.set_option(...)` | typed table/session/catalog properties | P2 | Partial |
| `FROM catalog.options()/settings()` | optional system metadata table later | P3 | Later |
| `CALL ducklake_flush_inlined_data(...)` | `CALL ducklake.system.flush_inlined_data(...)` | P3 | Later |
| `CALL ducklake_merge_adjacent_files(...)` | `ALTER TABLE ... EXECUTE optimize` or system proc | P3 | Later |
| `CALL ducklake_rewrite_data_files(...)` | `ALTER TABLE ... EXECUTE rewrite_data_files` | P3 | Later |
| `CALL ducklake_expire_snapshots(...)` | `CALL ducklake.system.expire_snapshots(...)` | P3 | Later |
| `CALL ducklake_cleanup_old_files(...)` | `CALL ducklake.system.cleanup_old_files(...)` | P3 | Later |
| `CALL ducklake_delete_orphaned_files(...)` | `CALL ducklake.system.remove_orphan_files(...)` | P3 | Later |
| `CALL ducklake_add_data_files(...)` | `CALL ducklake.system.add_data_files(...)` | P4 | Later |
| `CHECKPOINT` | none (catalog DB admin concern, outside connector) | - | Not planned |

## Reality Check: Spec vs Actual Catalog Shape

Before writing code, lock to observed DuckDB-generated catalogs (current reality), not only markdown spec examples.

Known differences observed in generated catalogs:

- `ducklake_schema_versions` has `table_id` in practice.
- `ducklake_column` has extra columns (`default_value_type`, `default_value_dialect`) in practice.
- `ducklake_data_file.partial_max` is often populated (not consistently NULL).
- `ducklake_snapshot_changes.changes_made` values include forms like `inlined_insert:...`, `inline_flush:...`, `merge_adjacent:...`.
- Inlined metadata can point at an inlined table that is absent (`ducklake_inlined_data_tables` row exists but `ducklake_inlined_data_<tableId>_<schemaVersion>` does not). Current read path handles this with `SQLException` fallback to empty results; write mode should avoid creating/keeping stale pointers.
- Temporal partition values currently follow DuckDB calendar semantics in metadata (`year=2023`, `month=6`, `day=15`), not epoch-offset semantics. Upstream response indicates docs are misaligned and epoch-style support is planned; plan write mode to be forward-compatible with both encodings.

Plan implication: write path must be compatible with what DuckDB reads today.

## Reuse-First Strategy

## Reuse directly

- `trino-hive` Parquet writer stack:
  - `io.trino.plugin.hive.parquet.ParquetFileWriter`
  - `io.trino.plugin.hive.parquet.ParquetWriterConfig`
  - `io.trino.parquet.writer.ParquetWriterOptions`
- `trino-filesystem` APIs already in connector.
- `ClassLoaderSafeConnectorPageSinkProvider` pattern from Iceberg/Hive/Delta.
- `BaseConnectorTest` approach for broad write behavior verification.

## Reuse patterns (copy/adapt architecture)

- Iceberg/Delta page sink lifecycle:
  - one writer per partition key
  - rotate by target file size
  - fragment emission on `finish()`
  - rollback cleanup on abort
- Delta statistics extraction from Parquet footer (file-level stats) as an implementation pattern.

## Keep custom (do not force reuse)

- DuckLake snapshot commit semantics (`ducklake_snapshot`, `ducklake_snapshot_changes`)
- DuckLake metadata table mutations
- Row-id assignment (`row_id_start`) and table stats updates
- DuckLake partition transform semantics (including current DuckDB temporal behavior)

## Milestone Plan (Execution Order)

## M0: Catalog Write Contract + Transaction Primitive

> **Note**: A lightweight snapshot-commit helper was added in `JdbcDucklakeCatalog` for view create/drop operations (the first write-side feature, R7 in TODO-READ-MODE.md). The `createView` and `dropView` methods use ad-hoc `autoCommit=false` + manual commit/rollback on a single connection. When M0 is implemented, these methods should be refactored to use the proper write transaction abstraction instead of their current inline connection management. Look for the `TODO: Refactor into M0` comments in those methods.

- [ ] Extend `DucklakeCatalog` with explicit write transaction API (not ad-hoc per statement).
- [ ] Add JDBC-backed write transaction object in `JdbcDucklakeCatalog`:
  - single connection
  - `autoCommit=false`
  - explicit `commit`/`rollback`
  - deterministic cleanup on failure
- [ ] Add capability/introspection at startup for optional columns (`ducklake_schema_versions.table_id`, extra `ducklake_column` fields) to keep SQL portable across catalog variants.
- [ ] Add unit tests for transaction lifecycle and rollback behavior.

Exit criteria:

- We can run a multi-statement metadata mutation atomically in each backend (`sqlite`, `duckdb`, `postgresql`).

## M1: Connector Write Plumbing

- [ ] Add writable handle types:
  - `DucklakeOutputTableHandle`
  - `DucklakeInsertTableHandle`
  - shared `DucklakeWritableTableHandle`
  - `DucklakeWriteFragment` (serialized file metadata + stats)
- [ ] Add `DucklakePageSinkProvider`.
- [ ] Add `DucklakePageSink` with:
  - partition-aware writer indexing
  - file writer rotation by size
  - fragment emission on finish
  - abort rollback
- [ ] Wire connector/module:
  - add `getPageSinkProvider()` in `DucklakeConnector`
  - bind provider + classloader-safe wrapper in `DucklakeModule`
  - bind `ParquetWriterConfig`

Exit criteria:

- Engine can call write SPI paths (`beginInsert`/`finishInsert`, `beginCreateTable`/`finishCreateTable`) end-to-end.

## M2: DDL Metadata Writes

### M2a `CREATE SCHEMA` / `DROP SCHEMA`

- [ ] Implement snapshot commit helper:
  - read current snapshot
  - create next snapshot row
  - insert `snapshot_changes`
  - update `next_catalog_id` / `schema_version` as needed
- [ ] Implement schema row insert/update with `begin_snapshot`/`end_snapshot`.

### M2b `CREATE TABLE` / `DROP TABLE`

- [ ] Create table metadata rows:
  - `ducklake_table` (path `<table_name>/`, relative path)
  - `ducklake_column` entries (flatten nested types with parent links)
  - initialize `ducklake_table_stats` (`record_count=0`, `next_row_id=0`, `file_size_bytes=0`)
  - `ducklake_schema_versions` update on schema change
- [ ] Drop table as end-snapshot updates across relevant metadata tables (`table`, `column`, `partition_info`, `data_file`, `delete_file`, tags).
- [ ] Keep change strings DuckDB-compatible for broad interoperability (`created_table:...`, `dropped_table:...`, etc.).

Exit criteria:

- Tables created by Trino are discoverable and readable by Trino and DuckDB before any data insert.

## M3: INSERT (Core Data Write)

### M3a File writing

- [ ] Write Parquet data files through reusable writer infra.
- [ ] Build output path from catalog `data_path` + schema path + table path.
- [ ] Support unpartitioned and partitioned writes.
- [ ] Generate stable filenames (`ducklake-<uuid>.parquet`).

### M3b Metadata commit

- [ ] Allocate `data_file_id` from snapshot `next_file_id`.
- [ ] Insert `ducklake_data_file` rows:
  - `begin_snapshot = new snapshot`
  - `end_snapshot = NULL`
  - `row_id_start` from table `next_row_id` prefix sums
  - `file_format='parquet'`
  - `path_is_relative=true`
  - `partition_id` when partitioned
- [ ] Insert `ducklake_file_column_stats` from fragment stats.
- [ ] Upsert/merge `ducklake_table_column_stats`.
- [ ] Update `ducklake_table_stats`:
  - `record_count += written_rows`
  - `next_row_id += written_rows`
  - `file_size_bytes += written_bytes`
- [ ] Insert `ducklake_file_partition_value` for partitioned files.
- [ ] Insert snapshot + changes (`inserted_into_table:<table_id>`).

### M3c Correctness + cleanup

- [ ] On commit failure, delete newly written files best-effort.
- [ ] Ensure idempotent abort path.

Exit criteria:

- `INSERT` works for Trino-created tables in all three catalogs and remains readable by DuckDB.

## M4: CTAS

- [ ] Implement `beginCreateTable` / `finishCreateTable` with write fragments.
- [ ] Prefer a single atomic snapshot containing both create-table metadata and inserted files.
- [ ] Ensure drop-on-failure semantics for partially written data files.

Exit criteria:

- `CREATE TABLE ... AS SELECT` passes connector tests and cross-engine validation.

## M5: Partitioning + Type Completeness for Writes

- [ ] Add table property for partition spec (expression strings), e.g. `ARRAY['region']`, `ARRAY['year(event_date)', 'month(event_date)']`.
- [ ] Implement transform parser -> `DucklakePartitionTransform`.
- [ ] Persist partition spec into `ducklake_partition_info` + `ducklake_partition_column`.
- [ ] Compute partition values using current DuckDB-compatible temporal encoding.
- [ ] Complete `DucklakeTypeConverter.toDucklakeType()` for nested types (`array`, `row`, `map`) and write-time type validation.

Exit criteria:

- Partitioned tables created and written by Trino are pruned correctly by Trino and readable by DuckDB.

## M6: Row-Level Mutations

- [ ] `DELETE`: write delete parquet files + metadata rows in `ducklake_delete_file`.
- [ ] `UPDATE`: implement as delete+insert in one snapshot commit.
- [ ] `MERGE`: compose insert/delete fragments with conflict checks.

Exit criteria:

- Row-level operations preserve read correctness and snapshot integrity.

## M7: ALTER + Hardening

- [ ] `ALTER TABLE ADD/DROP/RENAME COLUMN` with snapshot-versioned `ducklake_column`.
- [ ] Schema evolution metadata alignment (`ducklake_schema_versions`, stats resilience).
- [ ] Concurrency/conflict handling (optimistic commit conflict detection using snapshot lineage and changes).
- [ ] Performance pass (writer scaling, file size tuning, stats cost).

Exit criteria:

- Connector supports practical production write workflows with stable semantics.

## M8: Maintenance Operations (Post-v1)

- [ ] Add maintenance verbs that map cleanly to Trino conventions:
  - `ALTER TABLE ... EXECUTE optimize` (DuckDB `merge_adjacent_files` equivalent)
  - `ALTER TABLE ... EXECUTE rewrite_data_files`
- [ ] Add connector procedures in `ducklake.system`:
  - `expire_snapshots`
  - `cleanup_old_files`
  - `remove_orphan_files`
  - `flush_inlined_data`
- [ ] Add result tables from procedures (rows affected/files deleted/bytes reclaimed).

Exit criteria:

- Key maintenance workflows are available without DuckDB-specific function syntax, with behavior validated against DuckDB visibility/readability.

## Test Plan (Must-Have Matrix)

Run the following matrix for each backend:

| Catalog backend | DuckDB-created -> Trino-read | Trino-created -> Trino-read | Trino-created -> DuckDB-read |
|---|---|---|---|
| `sqlite` | Required | Required | Required |
| `duckdb` | Required | Required | Required |
| `postgresql` | Required | Required | Required |

Total mandatory scenarios: 9.

## Test suites to add

- [ ] `TestDucklakeWriteConnectorTest` (extends `BaseConnectorTest`), backend parameterized by `ducklake.test.catalog-backend`.
- [ ] `TestDucklakeWriteIntegration` (focused DDL/DML correctness and snapshot table checks).
- [ ] `TestDucklakeCrossEngineCompatibility`:
  - run Trino writes
  - attach same catalog in DuckDB JDBC
  - assert DuckDB can `SHOW TABLES`, `DESCRIBE`, `SELECT`, and row counts match.

## Existing suites to keep running

- [ ] `TestDucklakeIntegration` (DuckDB-created -> Trino-read baseline)
- [ ] `TestDucklakeCatalog`
- [ ] `TestDucklakeSplitManager`
- [ ] `TestDucklakePartitionPruning`
- [ ] `TestDucklakePageSourceProvider`
- [ ] `TestDucklakeDeleteFileHandling` (SQLite-specific)

## Backend-specific notes

- `postgresql` tests need Docker/Testcontainers.
- DuckDB cross-engine test path:
  - `INSTALL ducklake; LOAD ducklake;`
  - install/load `postgres` extension when backend is PostgreSQL.

## Suggested test command slices

- SQLite:
  - `mvn test -Dducklake.test.catalog-backend=sqlite`
- DuckDB:
  - `mvn test -Dducklake.test.catalog-backend=duckdb`
- PostgreSQL:
  - `mvn test -Dducklake.test.catalog-backend=postgresql`

## Definition of Done (Write Mode v1)

- [ ] DDL (`CREATE/DROP SCHEMA`, `CREATE/DROP TABLE`) implemented and validated in all three catalogs.
- [ ] `INSERT` and `CTAS` implemented and validated in all three catalogs.
- [ ] 9-scenario interoperability matrix green.
- [ ] No regressions on existing read-path suites.
- [ ] `STATUS.md` updated with exact write capability coverage and any explicit non-goals.

## Risk Register

- [ ] Schema drift between markdown spec and real extension output:
  - Mitigate with catalog schema introspection + compatibility SQL.
- [ ] Concurrency conflicts:
  - MVP may start with strict optimistic single-commit behavior; add retries/conflict codes in later milestone.
- [ ] Stats correctness for nested types:
  - Start with primitive leaf stats; avoid incorrect pruning over aggressive stats.
- [ ] Temporal partition encoding mismatch:
  - Keep DuckDB-compatible behavior today, but add compatibility to handle both calendar and epoch encodings so behavior remains correct as upstream adds epoch support.

## Immediate Next 5 Tasks (first implementation sprint)

1. [ ] Add write transaction abstraction to `DucklakeCatalog` and `JdbcDucklakeCatalog`.
2. [ ] Add writable handles + `DucklakePageSinkProvider` + `DucklakePageSink` skeleton.
3. [ ] Implement `CREATE SCHEMA` and `CREATE TABLE` metadata commits.
4. [ ] Implement `beginInsert`/`finishInsert` for unpartitioned tables.
5. [ ] Add first cross-engine test: `sqlite` catalog, Trino-create + Trino-insert + DuckDB-read.
