/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.ducklake;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Cross-engine compatibility test: Trino creates tables and inserts data,
 * then DuckDB reads the same catalog and verifies the data.
 * Uses SQLite catalog backend for simplicity (no external services needed).
 */
@TestInstance(PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeCrossEngineCompatibility
        extends AbstractTestQueryFramework
{
    private Path catalogPath;
    private Path dataDir;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // useIsolatedCatalog generates the catalog, so just compute the path it will use
        catalogPath = Path.of("target", "test-catalog-isolated-cross-engine", "catalog.db");
        dataDir = catalogPath.getParent().resolve("data");

        return DucklakeQueryRunner.builder()
                .useIsolatedCatalog("cross-engine")
                .build();
    }

    /**
     * Creates a fresh DuckDB connection to avoid stale catalog cache.
     * Caller is responsible for closing.
     */
    private Connection createDuckdbConnection()
            throws Exception
    {
        // A separate SQLite JDBC connection must be held open while DuckDB attaches
        // the catalog. Without this, DuckDB's SQLite extension may not see data committed
        // by Trino's HikariCP connection pool (SQLite rollback journal visibility quirk).
        Connection syncConn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath.toAbsolutePath());
        syncConn.createStatement().executeQuery("SELECT max(snapshot_id) FROM ducklake_snapshot").close();

        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("INSTALL ducklake");
            stmt.execute("LOAD ducklake");
            stmt.execute("ATTACH 'sqlite:" + catalogPath.toAbsolutePath() + "' AS ducklake_db " +
                    "(TYPE DUCKLAKE, DATA_PATH '" + dataDir.toAbsolutePath() + "')");
        }

        syncConn.close();
        return conn;
    }

    // ==================== Basic round-trip ====================

    @Test
    public void testTrinoInsertDuckdbRead()
            throws Exception
    {
        computeActual("CREATE TABLE test_schema.xengine_basic (id INTEGER, name VARCHAR)");
        try {
            computeActual("INSERT INTO test_schema.xengine_basic VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')");

            // Verify Trino can read it
            MaterializedResult trinoResult = computeActual("SELECT count(*) FROM test_schema.xengine_basic");
            assertThat(trinoResult.getMaterializedRows().get(0).getField(0)).isEqualTo(3L);

            // First verify DuckDB can read the raw Parquet file directly
            java.io.File[] parquetFiles = new java.io.File(dataDir.toAbsolutePath() + "/test_schema/xengine_basic").listFiles(
                    (dir, name) -> name.endsWith(".parquet"));
            assertThat(parquetFiles).isNotNull().isNotEmpty();
            try (Connection rawConn = DriverManager.getConnection("jdbc:duckdb:");
                    Statement rawStmt = rawConn.createStatement();
                    ResultSet rawRs = rawStmt.executeQuery("SELECT count(*) FROM read_parquet('" + parquetFiles[0].getAbsolutePath() + "')")) {
                assertThat(rawRs.next()).isTrue();
                long rawCount = rawRs.getLong(1);
                assertThat(rawCount).as("DuckDB direct Parquet read").isEqualTo(3);
            }

            syncCatalogForCrossEngineRead();

            // Verify DuckDB can read it via DuckLake catalog (fresh connection)
            try (Connection conn = createDuckdbConnection()) {
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT * FROM ducklake_db.test_schema.xengine_basic ORDER BY id")) {
                    assertThat(rs.next()).as("DuckDB should find row 1").isTrue();
                    assertThat(rs.getInt("id")).isEqualTo(1);
                    assertThat(rs.getString("name")).isEqualTo("alice");
                    assertThat(rs.next()).as("DuckDB should find row 2").isTrue();
                    assertThat(rs.getInt("id")).isEqualTo(2);
                    assertThat(rs.getString("name")).isEqualTo("bob");
                    assertThat(rs.next()).as("DuckDB should find row 3").isTrue();
                    assertThat(rs.getInt("id")).isEqualTo(3);
                    assertThat(rs.getString("name")).isEqualTo("charlie");
                    assertThat(rs.next()).isFalse();
                }
            }
        }
        finally {
            tryDropTable("test_schema.xengine_basic");
        }
    }

    // ==================== CTAS round-trip ====================

    @Test
    public void testTrinoCtasDuckdbRead()
            throws Exception
    {
        computeActual("CREATE TABLE test_schema.xengine_ctas AS " +
                "SELECT CAST(id AS INTEGER) AS id, CAST(name AS VARCHAR) AS name " +
                "FROM (VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')) AS t(id, name)");
        try {
            try (Connection conn = createDuckdbConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT id, name FROM ducklake_db.test_schema.xengine_ctas ORDER BY id")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("id")).isEqualTo(1);
                assertThat(rs.getString("name")).isEqualTo("alpha");
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("id")).isEqualTo(2);
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("id")).isEqualTo(3);
                assertThat(rs.getString("name")).isEqualTo("gamma");
                assertThat(rs.next()).isFalse();
            }
        }
        finally {
            tryDropTable("test_schema.xengine_ctas");
        }
    }

    // ==================== Multiple inserts ====================

    @Test
    public void testTrinoMultipleInsertsDuckdbRead()
            throws Exception
    {
        computeActual("CREATE TABLE test_schema.xengine_multi (id INTEGER, value DOUBLE)");
        try {
            computeActual("INSERT INTO test_schema.xengine_multi VALUES (1, 10.0), (2, 20.0)");
            computeActual("INSERT INTO test_schema.xengine_multi VALUES (3, 30.0), (4, 40.0)");

            try (Connection conn = createDuckdbConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT count(*), sum(value) FROM ducklake_db.test_schema.xengine_multi")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1)).isEqualTo(4);
                assertThat(rs.getDouble(2)).isEqualTo(100.0);
            }
        }
        finally {
            tryDropTable("test_schema.xengine_multi");
        }
    }

    // ==================== Type coverage ====================

    @Test
    public void testTrinoTypesDuckdbRead()
            throws Exception
    {
        computeActual("CREATE TABLE test_schema.xengine_types (" +
                "col_int INTEGER, " +
                "col_bigint BIGINT, " +
                "col_double DOUBLE, " +
                "col_varchar VARCHAR, " +
                "col_boolean BOOLEAN, " +
                "col_date DATE" +
                ")");
        try {
            computeActual("INSERT INTO test_schema.xengine_types VALUES " +
                    "(42, 1000000000, 3.14, 'hello world', true, DATE '2024-06-15')");

            try (Connection conn = createDuckdbConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT * FROM ducklake_db.test_schema.xengine_types")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("col_int")).isEqualTo(42);
                assertThat(rs.getLong("col_bigint")).isEqualTo(1000000000L);
                assertThat(rs.getDouble("col_double")).isEqualTo(3.14);
                assertThat(rs.getString("col_varchar")).isEqualTo("hello world");
                assertThat(rs.getBoolean("col_boolean")).isTrue();
                assertThat(rs.getDate("col_date").toString()).isEqualTo("2024-06-15");
                assertThat(rs.next()).isFalse();
            }
        }
        finally {
            tryDropTable("test_schema.xengine_types");
        }
    }

    // ==================== NULLs ====================

    @Test
    public void testTrinoNullsDuckdbRead()
            throws Exception
    {
        computeActual("CREATE TABLE test_schema.xengine_nulls (id INTEGER, name VARCHAR, value DOUBLE)");
        try {
            computeActual("INSERT INTO test_schema.xengine_nulls VALUES (1, 'present', 10.0), (2, NULL, NULL)");

            try (Connection conn = createDuckdbConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT * FROM ducklake_db.test_schema.xengine_nulls ORDER BY id")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("id")).isEqualTo(1);
                assertThat(rs.getString("name")).isEqualTo("present");
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("id")).isEqualTo(2);
                assertThat(rs.getString("name")).isNull();
                assertThat(rs.getObject("value")).isNull();
                assertThat(rs.next()).isFalse();
            }
        }
        finally {
            tryDropTable("test_schema.xengine_nulls");
        }
    }

    // ==================== DuckDB SHOW TABLES sees Trino-created tables ====================

    @Test
    public void testDuckdbShowTablesIncludesTrinoTables()
            throws Exception
    {
        computeActual("CREATE TABLE test_schema.xengine_visible (id INTEGER)");
        try {
            computeActual("INSERT INTO test_schema.xengine_visible VALUES (1)");

            try (Connection conn = createDuckdbConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SHOW TABLES FROM ducklake_db.test_schema")) {
                List<String> tables = new ArrayList<>();
                while (rs.next()) {
                    tables.add(rs.getString(1));
                }
                assertThat(tables).contains("xengine_visible");
            }
        }
        finally {
            tryDropTable("test_schema.xengine_visible");
        }
    }

    // ==================== DuckDB DESCRIBE sees correct schema ====================

    @Test
    public void testDuckdbDescribeTrinoTable()
            throws Exception
    {
        computeActual("CREATE TABLE test_schema.xengine_describe (id INTEGER, name VARCHAR, amount DOUBLE)");
        try {
            try (Connection conn = createDuckdbConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT column_name FROM information_schema.columns " +
                            "WHERE table_schema = 'test_schema' AND table_name = 'xengine_describe' " +
                            "AND table_catalog = 'ducklake_db' ORDER BY ordinal_position")) {
                List<String> columnNames = new ArrayList<>();
                while (rs.next()) {
                    columnNames.add(rs.getString(1));
                }
                assertThat(columnNames).containsExactly("id", "name", "amount");
            }
        }
        finally {
            tryDropTable("test_schema.xengine_describe");
        }
    }

    // ==================== Diagnostic ====================

    @Test
    public void testDuckdbReadsDuckdbCreatedData()
            throws Exception
    {
        // Sanity check: can DuckDB read a table that DuckDB itself created?
        try (Connection conn = createDuckdbConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT count(*) FROM ducklake_db.test_schema.simple_table")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong(1)).as("DuckDB should read its own simple_table").isEqualTo(5);
        }
    }

    @Test
    public void testDiagnosticTrinoWriteThenDuckdbRead()
            throws Exception
    {
        computeActual("CREATE TABLE test_schema.diag_table (id INTEGER, name VARCHAR)");
        computeActual("INSERT INTO test_schema.diag_table VALUES (1, 'hello'), (2, 'world')");

        // Verify Trino reads it
        assertThat(computeActual("SELECT count(*) FROM test_schema.diag_table")
                .getMaterializedRows().get(0).getField(0)).isEqualTo(2L);

        // Dump catalog state for debugging via SQLite JDBC (same driver as Trino uses)
        try (Connection sqliteConn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath.toAbsolutePath());
                Statement sqliteStmt = sqliteConn.createStatement()) {
            ResultSet snapRs = sqliteStmt.executeQuery("SELECT max(snapshot_id) FROM ducklake_snapshot");
            long currentSnapshot = snapRs.next() ? snapRs.getLong(1) : -1;

            ResultSet tableRs = sqliteStmt.executeQuery(
                    "SELECT table_id, begin_snapshot, end_snapshot FROM ducklake_table WHERE table_name = 'diag_table'");
            long tableId = -1;
            long tableBegin = -1;
            String tableEnd = "?";
            if (tableRs.next()) {
                tableId = tableRs.getLong(1);
                tableBegin = tableRs.getLong(2);
                tableEnd = tableRs.getString(3);
            }

            ResultSet fileRs = sqliteStmt.executeQuery(
                    "SELECT data_file_id, begin_snapshot, end_snapshot, path, record_count FROM ducklake_data_file WHERE table_id = " + tableId);
            long fileBegin = -1;
            String fileEnd = "?";
            String filePath = "?";
            long fileRecords = -1;
            if (fileRs.next()) {
                fileBegin = fileRs.getLong(2);
                fileEnd = fileRs.getString(3);
                filePath = fileRs.getString(4);
                fileRecords = fileRs.getLong(5);
            }

            // Now try DuckDB
            try (Connection duckConn = createDuckdbConnection();
                    Statement duckStmt = duckConn.createStatement();
                    ResultSet duckRs = duckStmt.executeQuery("SELECT count(*) FROM ducklake_db.test_schema.diag_table")) {
                long duckCount = duckRs.next() ? duckRs.getLong(1) : -1;

                assertThat(duckCount)
                        .as("DuckDB count (snapshot=%d, table=%d begin=%d end=%s, file begin=%d end=%s path=%s records=%d)",
                                currentSnapshot, tableId, tableBegin, tableEnd, fileBegin, fileEnd, filePath, fileRecords)
                        .isEqualTo(2);
            }
        }
        finally {
            tryDropTable("test_schema.diag_table");
        }
    }

    // ==================== Helpers ====================

    /**
     * Forces SQLite to flush any pending writes from Trino's connection pool.
     * Without this, DuckDB's SQLite extension may not see the latest committed data
     * because SQLite in rollback journal mode can leave pending changes invisible
     * to other processes/connections until the journal is fully checkpointed.
     */
    /**
     * Ensures DuckDB can see the latest catalog state.
     * Opens a separate SQLite JDBC connection and reads, which appears to
     * synchronize the SQLite file state for cross-process readers.
     */
    private void syncCatalogForCrossEngineRead()
    {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath.toAbsolutePath());
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT max(snapshot_id) FROM ducklake_snapshot")) {
            rs.next();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to sync SQLite catalog", e);
        }
    }

    private void tryDropTable(String tableName)
    {
        try {
            computeActual("DROP TABLE " + tableName);
        }
        catch (Exception ignored) {
        }
    }
}
