# DuckLake Read Mode Plan (Trino Connector)

Last updated: 2026-04-01

## Objective

Define and implement a Trino-native read surface for DuckLake that supports:

- Correct current-snapshot reads (already implemented)
- Time travel reads (`FOR VERSION AS OF`, `FOR TIMESTAMP AS OF`)
- Snapshot metadata introspection (DuckDB parity where it makes sense in Trino)
- Cross-engine interoperability with DuckDB across all catalog backends (`sqlite`, `duckdb`, `postgresql`)

## Scope Decisions (for review)

1. Primary time travel interface: query-level `AS OF` (high priority)
2. Optional pinned-snapshot mode (catalog/session default): low priority
3. DuckDB-specific attach-time snapshot options are not mirrored directly in Trino; we expose equivalent behavior through Trino query/session semantics

## Current State (as implemented)

- Current-snapshot reads plus optional snapshot pinning through session/catalog defaults.
- `DucklakeMetadata#getTableHandle(...)` now converts Trino table-version arguments (`FOR VERSION/TIMESTAMP AS OF`) to DuckLake snapshot resolution inputs.
- Read path is strong on pruning and compatibility for present snapshot:
  - file stats pruning
  - partition pruning
  - Parquet row-group pruning
  - dynamic filtering intersection
  - delete-file merge-on-read
  - inlined data read path for no-parquet case
- Row-group pruning remains base-column/statistics driven; connector does not map transformed predicates like `year(ts)` onto base Parquet stats directly.
- DuckLake session properties now expose optional snapshot pinning (`read_snapshot_id`, `read_snapshot_timestamp`).

## Reality Check: Known Edge Cases

- Inlined metadata pointer mismatch:
  - `ducklake_inlined_data_tables` may contain a row while the physical `ducklake_inlined_data_<tableId>_<schemaVersion>` table does not exist.
  - Current behavior: catch `SQLException`, return empty, continue safely.
- Inlined + Parquet mixed state:
  - Implemented: split generation emits both Parquet and inlined splits when both are active in the same snapshot.
- Temporal partition encoding ambiguity:
  - Docs and observed behavior differ.
  - Current read path assumes DuckDB calendar semantics for temporal transforms (`year=2023`, `month=6`, `day=15`, `hour=0..23`).
  - Upstream response on bug report: docs are misaligned; epoch form should be supported too.
  - Plan requirement: read/pruning logic must support both encodings.

## Trino Read SQL Surface (Proposed)

## A. Time Travel Queries (P0)

Use Trino table version syntax as the canonical API:

```sql
SELECT * FROM lake.sales.orders FOR VERSION AS OF 3;
SELECT * FROM lake.sales.orders FOR TIMESTAMP AS OF TIMESTAMP '2025-05-26 00:00:00 UTC';
```

Behavior:

- `FOR VERSION AS OF <bigint>` maps directly to `ducklake_snapshot.snapshot_id`
- `FOR TIMESTAMP AS OF <timestamp>` resolves to latest snapshot with `snapshot_time <= timestamp`
- Missing version/time: deterministic user error

## B. Metadata Introspection (P0/P1)

Trino-native equivalent to DuckDB `snapshots()` and `current_snapshot()`:

```sql
SELECT * FROM lake.sales.orders$files;
SELECT * FROM lake.sales.orders$snapshots;
SELECT * FROM lake.sales.orders$current_snapshot;
SELECT * FROM lake.sales.orders$snapshot_changes;
```

Recommended columns:

- `$files`: `data_file_id`, `path`, `file_format`, `record_count`, `file_size_bytes`, `row_id_start`, `partition_id`, `delete_file_path`
- `$snapshots`: `snapshot_id`, `snapshot_time`, `schema_version`, `next_catalog_id`, `next_file_id`
- `$current_snapshot`: same schema, exactly one row
- `$snapshot_changes`: `snapshot_id`, `changes_made`, `author`, `commit_message`, `commit_extra_info`

## C. Optional Session Pinning (P2, low priority)

Optional session properties for “fixed read point” without editing every query:

- `ducklake.read_snapshot_id` (BIGINT)
- `ducklake.read_snapshot_timestamp` (ISO-8601 instant string; validated as timestamp)

Rules:

- mutually exclusive
- explicit query `FOR ... AS OF` always wins
- default is unset (current snapshot)

## D. Optional Catalog Pinning (P3, low priority)

Catalog startup properties (mostly for debugging/repro):

- `ducklake.default-snapshot-id`
- `ducklake.default-snapshot-timestamp`

Same precedence rule: query `AS OF` > session pin > catalog pin > current snapshot.

## Read Feature Roadmap (Order)

## R0: Snapshot Resolution Layer

- [x] Add a dedicated snapshot resolver component:
  - by version ID
  - by timestamp
  - from session defaults
- [x] Extend catalog API:
  - `Optional<DucklakeSnapshot> getSnapshot(long snapshotId)` already exists
  - add `Optional<DucklakeSnapshot> getSnapshotAtOrBefore(Instant timestamp)`
- [x] Centralize precedence logic (query > session > catalog > current)

## R1: Mixed Inlined + Parquet Union Read (Correctness First)

- [x] Change split planning to emit both Parquet splits and inlined splits when both are present in the same snapshot.
- [x] Preserve correctness with delete filtering and row-id semantics.
- [x] Add regression tests for mixed-state snapshots (inlined rows + data files simultaneously visible).
- [x] Keep fallback behavior for stale inlined pointers (missing `ducklake_inlined_data_*` table) as non-fatal.

## R2: Table Version Support

- [x] Implement `startVersion` handling in `DucklakeMetadata#getTableHandle(...)`
- [x] Convert Trino `ConnectorTableVersion` values to snapshot IDs
- [x] Return precise errors for invalid versions/timestamps

## R3: Metadata Tables (`$` naming, Iceberg-style first)

- [x] Add `$files` (first-class, Iceberg-style high-value metadata)
- [x] Add `$snapshots`
- [x] Add `$current_snapshot`
- [x] Add `$snapshot_changes`
- [x] Keep schemas stable and documented.
- [x] Follow Iceberg-style naming/shape where possible, then add DuckLake-specific metadata tables only when needed.

## R4: Temporal Encoding Compatibility (Calendar + Epoch)

- [x] Add dual-encoding transform mode in partition pruning
- [x] Default mode: `auto` with safe pruning (never false-negative), i.e. only prune when both calendar and epoch interpretations exclude a file
- [x] Add explicit test cases where calendar and epoch interpretations diverge

## R5: Optional Session/Catalog Pinning

- [x] Session properties wiring
- [x] Optional catalog startup pin properties
- [x] Validate precedence and conflict errors

## R6: Change Feed and Extended Metadata Parity (later)

- [ ] Evaluate Trino equivalents for:
  - DuckDB `table_changes`, `table_insertions`, `table_deletions`
  - DuckLake-specific metadata surfaces beyond `$files`/`$snapshots`
- [ ] Implement only when there is clear Trino use-case and maintainable API shape

## DuckDB Extension Parity Matrix (Complete View)

| DuckDB feature/function | Category | Trino equivalent (proposed) | Priority | Decision |
|---|---|---|---|---|
| `FROM catalog.snapshots()` | Read metadata | `table$snapshots` | P0 | Do |
| `FROM catalog.current_snapshot()` | Read metadata | `table$current_snapshot` | P0 | Do |
| `FROM catalog.last_committed_snapshot()` | Connection-local state | None | - | Not planned |
| `SELECT ... AT (VERSION => ...)` | Time travel read | `FOR VERSION AS OF ...` | P0 | Do |
| `SELECT ... AT (TIMESTAMP => ...)` | Time travel read | `FOR TIMESTAMP AS OF ...` | P0 | Do |
| `ATTACH ... (SNAPSHOT_VERSION ...)` | Catalog attach-time pin | Session/catalog pin properties | P2/P3 | Optional |
| `ATTACH ... (SNAPSHOT_TIME ...)` | Catalog attach-time pin | Session/catalog pin properties | P2/P3 | Optional |
| `FROM table_changes(...)` | Change feed | Connector table function/system table | P3 | Later |
| `FROM table_insertions(...)` | Change feed | Connector table function/system table | P3 | Later |
| `FROM table_deletions(...)` | Change feed | Connector table function/system table | P3 | Later |
| `FROM ducklake_list_files(...)` | Metadata utility | `table$files` (primary) + optional table function later | P1/P3 | Do + Later |
| `rowid` virtual column | Lineage | Hidden column/metadata table if needed | P3 | Later |
| `FROM catalog.options()/settings()` | Config metadata | Session/catalog properties + optional system table | P3 | Later |
| `CALL catalog.set_option(...)` | Generic config mutation | Typed Trino properties only | - | Not planned |

## DuckDB Docs Coverage Used For This Plan

Pages reviewed while building parity scope:

- `duckdb/usage`: `connecting`, `configuration`, `snapshots`, `time_travel`, `schema_evolution`, `upserting`
- `duckdb/advanced_features`: `transactions`, `conflict_resolution`, `data_change_feed`, `data_inlining`, `partitioning`, `row_lineage`, `sorted_tables`, `views`
- `duckdb/maintenance`: `recommended_maintenance`, `checkpoint`, `expire_snapshots`, `merge_adjacent_files`, `rewrite_data_files`, `cleanup_of_files`
- `duckdb/metadata`: `list_files`, `adding_files`
- `duckdb/unsupported_features`

## Reuse-First Read Strategy

Reuse directly:

- Existing Trino Parquet read stack (already used)
- Existing split pruning infrastructure (already used)
- Existing `ConnectorTableVersion` SPI plumbing
- Metadata table patterns from Iceberg/Delta connectors (design/reference)

Keep custom:

- DuckLake snapshot resolution against SQL metadata
- Temporal dual-encoding compatibility logic for DuckLake partition transforms
- Inlined-data fallback semantics and mixed-mode policy

## Read Test Matrix

Run for each catalog backend: `sqlite`, `duckdb`, `postgresql`.

| Scenario | DuckDB-created -> Trino-read | Trino-created -> Trino-read | Trino-created -> DuckDB-read |
|---|---|---|---|
| Current snapshot reads | Required | Required | Required |
| Mixed inlined + Parquet snapshot reads | Required | Required | Required |
| `FOR VERSION AS OF` | Required | Required | Required (validate snapshot exists/readable in DuckDB) |
| `FOR TIMESTAMP AS OF` | Required | Required | Required |
| `$files` and snapshot metadata tables | Required | Required | N/A |

Total required interoperability scenarios remain aligned with write plan matrix (9 baseline combos, then feature overlays).

## R7: View Support (Read + Write Pair)

**Note**: Views are the first write-side item. Read and write should be implemented together. See also TODO-WRITE-MODE.md "First Write Item: Views" section.

DuckLake stores views in the `ducklake_view` catalog table with full snapshot-scoped visibility. Views created in DuckDB are currently invisible to Trino because the connector has zero view implementation.

### Current State

- **DuckLake catalog schema**: `ducklake_view` table exists with columns: `view_id`, `view_uuid`, `begin_snapshot`, `end_snapshot`, `schema_id`, `view_name`, `dialect`, `sql`, `column_aliases`
- **Trino connector**: No view methods in `DucklakeCatalog` interface, `JdbcDucklakeCatalog`, or `DucklakeMetadata`. The `ConnectorMetadata` default implementations return empty/throw NOT_SUPPORTED.
- **DuckDB behavior**: Views are created via standard `CREATE VIEW`, stored with `dialect` field (e.g., "duckdb"), and support time-travel (begin/end snapshot scoping).

### Implementation Plan (Read-Only MVP)

**1. Model class**: Create `DucklakeView` record with fields: `viewId`, `viewUuid`, `viewName`, `schemaId`, `sql`, `dialect`, `columnAliases`.

**2. Catalog interface** — add to `DucklakeCatalog`:
```java
List<DucklakeView> listViews(long schemaId, long snapshotId);
Optional<DucklakeView> getView(String schemaName, String viewName, long snapshotId);
```

**3. JDBC implementation** — add to `JdbcDucklakeCatalog`:
```sql
SELECT view_id, view_uuid, view_name, dialect, sql, column_aliases
FROM ducklake_view
WHERE schema_id = ?
  AND ? >= begin_snapshot
  AND (? < end_snapshot OR end_snapshot IS NULL)
```

**4. Metadata methods** — implement in `DucklakeMetadata`:
- `listViews(session, schemaName)` → query catalog, return `List<SchemaTableName>`
- `getView(session, viewName)` → return `Optional<ConnectorViewDefinition>`
- `isView(session, viewName)` → delegate to `getView`
- `getViews(session, schemaName)` → bulk version of `getView`

**5. SQL dialect challenge**: DuckLake stores the SQL body with a `dialect` field (typically "duckdb"). Options:
- **Option A (simplest)**: Return the SQL as-is via `ConnectorViewDefinition.getOriginalSql()`. Trino will attempt to parse it — DuckDB SQL is mostly compatible but may fail on DuckDB-specific syntax.
- **Option B (robust)**: Only expose views where `dialect = 'trino'` or `dialect = 'sql'`. Log a warning for DuckDB-dialect views. When Trino write support is added, store views with `dialect = 'trino'`.
- **Option C (advanced)**: Implement a DuckDB→Trino SQL transpiler for common patterns. Defer this — scope is large and error-prone.
- **Recommended**: Start with Option B. Views created by DuckDB with DuckDB-specific syntax (e.g., `STRUCT` literals, `LIST` functions) will not parse in Trino anyway. Expose only compatible views, and when we add `CREATE VIEW` in write mode, store as Trino dialect.

**6. Column aliases**: The `column_aliases` field may contain column name remapping. Map these to `ConnectorViewDefinition.ViewColumn` entries.

**7. Write operations (deferred to write mode)**:
- `createView` / `dropView` / `renameView` — defer until write support
- `setViewComment` / `setViewColumnComment` — defer

### Future: Cross-Dialect View Transpilation (Research Item)

Instead of filtering out DuckDB-dialect views, transpile them to Trino SQL at read time. This would allow Trino to expose all DuckLake views regardless of which engine created them.

**Architecture**: Rust-based SQL transpiler → compile to WASM → load in JVM via pure-Java WASM runtime → call `transpile(sql, from="duckdb", to="trino")` per view access.

**Transpiler candidates** (need research/evaluation):
- **sqlglot (Python)**: https://github.com/tobymao/sqlglot — most mature (25+ dialects), but Python embedding in JVM is impractical for a Trino plugin
- **sqlglot-rust (crates.io) / sql-glot-rust (Protegrity)**: https://github.com/protegrity/sql-glot-rust — similar Rust port, v0.9.24
- **polyglot-sql**: https://github.com/tobilg/polyglot — Rust, very early stage

opposite direction:
- **papera**: https://lib.rs/crates/papera (trino to duck)

**WASM-on-JVM runtime**:
- **Chicory**: https://github.com/dylibso/chicory — pure Java WASM interpreter, no native dependencies, works in any JVM. Right choice for Trino plugins (classloader isolation, no JNI). Performance is fine for transpiling a single SQL string per view access (plan time, not hot path).
- GraalVM WASM and wasmtime-java/wasmer-java are alternatives but require native code or specific JVM, which Trino plugins can't guarantee.

**Configuration**: Add a catalog property like `ducklake.view-dialect-transpilation=duckdb` (or a list: `duckdb,spark`) that enables transpilation for those dialects. Default: disabled (only Trino-dialect views exposed). When enabled, views with matching dialect are transpiled before being returned to the planner.

**Risks to evaluate**:
- Transpilation correctness — a bug silently changes query semantics
- Rust port maturity vs Python original — DuckDB-specific syntax coverage
- WASM binary as a shipped dependency — versioning and update story
- Error handling — what happens when transpilation fails (fallback to skip? surface error?)

**Status**: Research item. Assign someone to evaluate sqlglot-rust / sql-glot-rust DuckDB→Trino transpilation quality against real DuckLake views.

### Reference: Iceberg's approach

Iceberg connector implements all view methods by delegating to `catalog.listViews()` / `catalog.getView()` / `catalog.createView()`. It also handles unsupported view dialects with a custom error code (`ICEBERG_UNSUPPORTED_VIEW_DIALECT`). Similar pattern applies here.

### Test Plan

- [x] Add DuckDB-created views to `DucklakeCatalogGenerator` (simple_view, aliased_view, duckdb_specific_view)
- [x] Test `listViews` returns views from catalog
- [x] Test `getView` returns `ConnectorViewDefinition` with correct SQL and columns
- [ ] Test view snapshot scoping (view visible at snapshot N but not N-1)
- [x] Test dialect filtering (DuckDB-dialect views handled gracefully)
- [ ] Test views across all catalog backends (SQLite, PostgreSQL, DuckDB)

## Definition of Done (Read Mode)

- [ ] `FOR VERSION AS OF` and `FOR TIMESTAMP AS OF` implemented and tested across all 3 catalog backends.
- [ ] Mixed inlined+Parquet union-read correctness implemented and validated.
- [ ] `$files` and snapshot metadata tables exposed and validated.
- [ ] Temporal partition pruning handles both calendar and epoch encodings safely.
- [ ] Existing read correctness/perf tests remain green.
- [ ] `STATUS.md` updated with exact read feature coverage and explicit non-goals.
