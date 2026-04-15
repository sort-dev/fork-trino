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

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.List;

import static io.trino.plugin.ducklake.DucklakeSessionProperties.READ_SNAPSHOT_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for DuckLake DDL: CREATE/DROP SCHEMA, CREATE/DROP TABLE.
 * Uses an isolated catalog to avoid cross-test interference.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeDDLIntegration
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DucklakeQueryRunner.builder()
                .useIsolatedCatalog("ddl-integration")
                .build();
    }

    // ==================== Schema DDL ====================

    @Test
    public void testCreateAndDropSchema()
    {
        computeActual("CREATE SCHEMA new_schema");

        List<String> schemas = computeActual("SHOW SCHEMAS").getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .toList();
        assertThat(schemas).contains("new_schema");

        computeActual("DROP SCHEMA new_schema");

        List<String> schemasAfterDrop = computeActual("SHOW SCHEMAS").getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .toList();
        assertThat(schemasAfterDrop).doesNotContain("new_schema");
    }

    @Test
    public void testDropSchemaWithTablesFails()
    {
        computeActual("CREATE SCHEMA schema_with_table");
        try {
            computeActual("CREATE TABLE schema_with_table.t1 (id INTEGER, name VARCHAR)");
            try {
                assertThatThrownBy(() -> computeActual("DROP SCHEMA schema_with_table"))
                        .hasMessageContaining("non-empty");
            }
            finally {
                computeActual("DROP TABLE schema_with_table.t1");
            }
        }
        finally {
            tryDropSchema("schema_with_table");
        }
    }

    // ==================== Table DDL (simple) ====================

    @Test
    public void testCreateAndDropSimpleTable()
    {
        computeActual("CREATE TABLE test_schema.simple_write_test (id INTEGER, name VARCHAR, price DOUBLE, active BOOLEAN)");
        try {
            // Verify table appears in SHOW TABLES
            List<String> tables = computeActual("SHOW TABLES FROM test_schema").getMaterializedRows().stream()
                    .map(row -> row.getField(0).toString())
                    .toList();
            assertThat(tables).contains("simple_write_test");

            // Verify column metadata
            MaterializedResult columns = computeActual("DESCRIBE test_schema.simple_write_test");
            List<String> columnNames = columns.getMaterializedRows().stream()
                    .map(row -> row.getField(0).toString())
                    .toList();
            assertThat(columnNames).containsExactly("id", "name", "price", "active");

            // Verify it's an empty table
            MaterializedResult data = computeActual("SELECT count(*) FROM test_schema.simple_write_test");
            assertThat(data.getMaterializedRows().get(0).getField(0)).isEqualTo(0L);
        }
        finally {
            tryDropTable("test_schema.simple_write_test");
        }
    }

    @Test
    public void testDropTableMakesItInvisible()
    {
        computeActual("CREATE TABLE test_schema.drop_test (id INTEGER)");

        // Get snapshot before drop
        MaterializedResult snapshot = computeActual("SELECT max(snapshot_id) FROM \"simple_table$snapshots\"");
        long snapshotBeforeDrop = (long) snapshot.getMaterializedRows().get(0).getField(0);

        computeActual("DROP TABLE test_schema.drop_test");

        // Table gone at current snapshot
        List<String> tables = computeActual("SHOW TABLES FROM test_schema").getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .toList();
        assertThat(tables).doesNotContain("drop_test");

        // Table still visible at old snapshot via time travel
        Session oldSnapshot = Session.builder(getSession())
                .setCatalogSessionProperty("ducklake", READ_SNAPSHOT_ID, String.valueOf(snapshotBeforeDrop))
                .build();
        List<String> oldTables = computeActual(oldSnapshot, "SHOW TABLES FROM test_schema").getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .toList();
        assertThat(oldTables).contains("drop_test");
    }

    // ==================== Table DDL (nested types) ====================

    @Test
    public void testCreateTableWithArrayType()
    {
        computeActual("CREATE TABLE test_schema.array_write_test (id INTEGER, tags ARRAY(VARCHAR))");
        try {
            MaterializedResult columns = computeActual("DESCRIBE test_schema.array_write_test");
            List<String> columnNames = columns.getMaterializedRows().stream()
                    .map(row -> row.getField(0).toString())
                    .toList();
            assertThat(columnNames).containsExactly("id", "tags");

            // Check the type is correct
            String tagsType = columns.getMaterializedRows().get(1).getField(1).toString();
            assertThat(tagsType).isEqualTo("array(varchar)");
        }
        finally {
            tryDropTable("test_schema.array_write_test");
        }
    }

    @Test
    public void testCreateTableWithStructType()
    {
        computeActual("CREATE TABLE test_schema.struct_write_test (id INTEGER, metadata ROW(key VARCHAR, value VARCHAR))");
        try {
            MaterializedResult columns = computeActual("DESCRIBE test_schema.struct_write_test");
            List<String> columnNames = columns.getMaterializedRows().stream()
                    .map(row -> row.getField(0).toString())
                    .toList();
            assertThat(columnNames).containsExactly("id", "metadata");

            String metadataType = columns.getMaterializedRows().get(1).getField(1).toString();
            assertThat(metadataType).isEqualTo("row(\"key\" varchar, \"value\" varchar)");
        }
        finally {
            tryDropTable("test_schema.struct_write_test");
        }
    }

    @Test
    public void testCreateTableWithMapType()
    {
        computeActual("CREATE TABLE test_schema.map_write_test (id INTEGER, attributes MAP(VARCHAR, VARCHAR))");
        try {
            MaterializedResult columns = computeActual("DESCRIBE test_schema.map_write_test");
            List<String> columnNames = columns.getMaterializedRows().stream()
                    .map(row -> row.getField(0).toString())
                    .toList();
            assertThat(columnNames).containsExactly("id", "attributes");

            String mapType = columns.getMaterializedRows().get(1).getField(1).toString();
            assertThat(mapType).isEqualTo("map(varchar, varchar)");
        }
        finally {
            tryDropTable("test_schema.map_write_test");
        }
    }

    // ==================== Table DDL (partitioned) ====================

    @Test
    public void testCreateTableWithIdentityPartition()
    {
        computeActual("CREATE TABLE test_schema.partitioned_write_test (id INTEGER, region VARCHAR, amount DOUBLE) " +
                "WITH (partitioned_by = ARRAY['region'])");
        try {
            // Verify table exists
            List<String> tables = computeActual("SHOW TABLES FROM test_schema").getMaterializedRows().stream()
                    .map(row -> row.getField(0).toString())
                    .toList();
            assertThat(tables).contains("partitioned_write_test");

            // Verify columns
            MaterializedResult columns = computeActual("DESCRIBE test_schema.partitioned_write_test");
            List<String> columnNames = columns.getMaterializedRows().stream()
                    .map(row -> row.getField(0).toString())
                    .toList();
            assertThat(columnNames).containsExactly("id", "region", "amount");
        }
        finally {
            tryDropTable("test_schema.partitioned_write_test");
        }
    }

    @Test
    public void testCreateTableWithTemporalPartitions()
    {
        computeActual("CREATE TABLE test_schema.temporal_part_test (id INTEGER, event_date DATE, amount DOUBLE) " +
                "WITH (partitioned_by = ARRAY['year(event_date)', 'month(event_date)'])");
        try {
            List<String> tables = computeActual("SHOW TABLES FROM test_schema").getMaterializedRows().stream()
                    .map(row -> row.getField(0).toString())
                    .toList();
            assertThat(tables).contains("temporal_part_test");

            MaterializedResult columns = computeActual("DESCRIBE test_schema.temporal_part_test");
            List<String> columnNames = columns.getMaterializedRows().stream()
                    .map(row -> row.getField(0).toString())
                    .toList();
            assertThat(columnNames).containsExactly("id", "event_date", "amount");
        }
        finally {
            tryDropTable("test_schema.temporal_part_test");
        }
    }

    // ==================== Snapshot tracking ====================

    @Test
    public void testSnapshotChangesTracked()
    {
        // Get baseline snapshot count
        MaterializedResult before = computeActual("SELECT max(snapshot_id) FROM \"simple_table$snapshots\"");
        long baselineSnapshot = (long) before.getMaterializedRows().get(0).getField(0);

        computeActual("CREATE TABLE test_schema.snapshot_test (id INTEGER)");
        try {
            // Should have created a new snapshot
            MaterializedResult after = computeActual("SELECT max(snapshot_id) FROM \"simple_table$snapshots\"");
            long newSnapshot = (long) after.getMaterializedRows().get(0).getField(0);
            assertThat(newSnapshot).isGreaterThan(baselineSnapshot);

            // Check changes contain the table creation
            MaterializedResult changes = computeActual(
                    "SELECT changes_made FROM \"simple_table$snapshot_changes\" WHERE snapshot_id = " + newSnapshot);
            assertThat(changes.getRowCount()).isGreaterThan(0);
            String changeMade = changes.getMaterializedRows().get(0).getField(0).toString();
            assertThat(changeMade).contains("created_table:snapshot_test");
        }
        finally {
            tryDropTable("test_schema.snapshot_test");
        }
    }

    // ==================== Wide types ====================

    @Test
    public void testCreateTableWithWideTypes()
    {
        computeActual("CREATE TABLE test_schema.wide_type_test (" +
                "col_tinyint TINYINT, " +
                "col_smallint SMALLINT, " +
                "col_int INTEGER, " +
                "col_bigint BIGINT, " +
                "col_real REAL, " +
                "col_double DOUBLE, " +
                "col_decimal DECIMAL(10,2), " +
                "col_varchar VARCHAR, " +
                "col_boolean BOOLEAN, " +
                "col_date DATE, " +
                "col_timestamp TIMESTAMP(6), " +
                "col_timestamptz TIMESTAMP(6) WITH TIME ZONE" +
                ")");
        try {
            MaterializedResult columns = computeActual("DESCRIBE test_schema.wide_type_test");
            assertThat(columns.getRowCount()).isEqualTo(12);
        }
        finally {
            tryDropTable("test_schema.wide_type_test");
        }
    }

    // ==================== ALTER TABLE ====================

    @Test
    public void testAddColumn()
    {
        try {
            computeActual("CREATE TABLE test_schema.alter_add (id INTEGER, name VARCHAR)");
            computeActual("INSERT INTO test_schema.alter_add VALUES (1, 'Alice'), (2, 'Bob')");

            computeActual("ALTER TABLE test_schema.alter_add ADD COLUMN age INTEGER");

            // DESCRIBE should show 3 columns
            MaterializedResult describe = computeActual("DESCRIBE test_schema.alter_add");
            assertThat(describe.getRowCount()).isEqualTo(3);

            // Existing rows should have NULL for the new column
            MaterializedResult result = computeActual("SELECT id, name, age FROM test_schema.alter_add ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(0).getField(2)).isNull();
            assertThat(result.getMaterializedRows().get(1).getField(2)).isNull();

            // Insert a row with the new column
            computeActual("INSERT INTO test_schema.alter_add VALUES (3, 'Charlie', 30)");

            result = computeActual("SELECT id, name, age FROM test_schema.alter_add ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(3);
            assertThat(result.getMaterializedRows().get(0).getField(2)).isNull();
            assertThat(result.getMaterializedRows().get(2).getField(2)).isEqualTo(30);
        }
        finally {
            tryDropTable("test_schema.alter_add");
        }
    }

    @Test
    public void testAddMultipleColumns()
    {
        try {
            computeActual("CREATE TABLE test_schema.alter_add_multi (id INTEGER)");

            computeActual("ALTER TABLE test_schema.alter_add_multi ADD COLUMN name VARCHAR");
            computeActual("ALTER TABLE test_schema.alter_add_multi ADD COLUMN score DOUBLE");

            MaterializedResult describe = computeActual("DESCRIBE test_schema.alter_add_multi");
            assertThat(describe.getRowCount()).isEqualTo(3);

            // Verify all columns work with INSERT
            computeActual("INSERT INTO test_schema.alter_add_multi VALUES (1, 'Alice', 95.5)");
            MaterializedResult result = computeActual("SELECT * FROM test_schema.alter_add_multi");
            assertThat(result.getRowCount()).isEqualTo(1);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("Alice");
            assertThat(result.getMaterializedRows().get(0).getField(2)).isEqualTo(95.5);
        }
        finally {
            tryDropTable("test_schema.alter_add_multi");
        }
    }

    @Test
    public void testDropColumn()
    {
        try {
            computeActual("CREATE TABLE test_schema.alter_drop (id INTEGER, name VARCHAR, amount DOUBLE)");
            computeActual("INSERT INTO test_schema.alter_drop VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)");

            computeActual("ALTER TABLE test_schema.alter_drop DROP COLUMN amount");

            // DESCRIBE should show 2 columns
            MaterializedResult describe = computeActual("DESCRIBE test_schema.alter_drop");
            assertThat(describe.getRowCount()).isEqualTo(2);

            // Data should be readable without the dropped column
            MaterializedResult result = computeActual("SELECT id, name FROM test_schema.alter_drop ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("Alice");
            assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo("Bob");

            // Insert with remaining columns
            computeActual("INSERT INTO test_schema.alter_drop VALUES (3, 'Charlie')");
            assertThat(computeScalar("SELECT count(*) FROM test_schema.alter_drop")).isEqualTo(3L);
        }
        finally {
            tryDropTable("test_schema.alter_drop");
        }
    }

    @Test
    public void testRenameColumn()
    {
        try {
            computeActual("CREATE TABLE test_schema.alter_rename (id INTEGER, full_name VARCHAR)");
            computeActual("INSERT INTO test_schema.alter_rename VALUES (1, 'Alice'), (2, 'Bob')");

            computeActual("ALTER TABLE test_schema.alter_rename RENAME COLUMN full_name TO name");

            // DESCRIBE should show new column name
            MaterializedResult describe = computeActual("DESCRIBE test_schema.alter_rename");
            List<String> columnNames = describe.getMaterializedRows().stream()
                    .map(row -> (String) row.getField(0))
                    .toList();
            assertThat(columnNames).contains("name");
            assertThat(columnNames).doesNotContain("full_name");

            // Data should be accessible via the new name
            MaterializedResult result = computeActual("SELECT id, name FROM test_schema.alter_rename ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("Alice");

            // Insert with new column name
            computeActual("INSERT INTO test_schema.alter_rename VALUES (3, 'Charlie')");
            assertThat(computeScalar("SELECT count(*) FROM test_schema.alter_rename")).isEqualTo(3L);
        }
        finally {
            tryDropTable("test_schema.alter_rename");
        }
    }

    @Test
    public void testAddColumnThenDelete()
    {
        try {
            computeActual("CREATE TABLE test_schema.alter_del (id INTEGER, name VARCHAR)");
            computeActual("INSERT INTO test_schema.alter_del VALUES (1, 'Alice'), (2, 'Bob')");

            // Add column, then delete from the table
            computeActual("ALTER TABLE test_schema.alter_del ADD COLUMN status VARCHAR");
            computeActual("INSERT INTO test_schema.alter_del VALUES (3, 'Charlie', 'active')");

            computeActual("DELETE FROM test_schema.alter_del WHERE id = 2");

            MaterializedResult result = computeActual("SELECT id, name, status FROM test_schema.alter_del ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
            assertThat(result.getMaterializedRows().get(0).getField(2)).isNull(); // old row, no status
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo(3);
            assertThat(result.getMaterializedRows().get(1).getField(2)).isEqualTo("active");
        }
        finally {
            tryDropTable("test_schema.alter_del");
        }
    }

    @Test
    public void testAddColumnThenUpdate()
    {
        try {
            computeActual("CREATE TABLE test_schema.alter_upd (id INTEGER, name VARCHAR)");
            computeActual("INSERT INTO test_schema.alter_upd VALUES (1, 'Alice'), (2, 'Bob')");

            computeActual("ALTER TABLE test_schema.alter_upd ADD COLUMN score INTEGER");

            // Update an old row to set the new column
            computeActual("UPDATE test_schema.alter_upd SET score = 100 WHERE id = 1");

            MaterializedResult result = computeActual("SELECT id, name, score FROM test_schema.alter_upd ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(0).getField(2)).isEqualTo(100);
            assertThat(result.getMaterializedRows().get(1).getField(2)).isNull(); // not updated
        }
        finally {
            tryDropTable("test_schema.alter_upd");
        }
    }

    @Test
    public void testDropColumnThenInsert()
    {
        try {
            computeActual("CREATE TABLE test_schema.alter_drop_ins (id INTEGER, name VARCHAR, temp VARCHAR)");
            computeActual("INSERT INTO test_schema.alter_drop_ins VALUES (1, 'Alice', 'x')");

            computeActual("ALTER TABLE test_schema.alter_drop_ins DROP COLUMN temp");
            computeActual("INSERT INTO test_schema.alter_drop_ins VALUES (2, 'Bob')");

            MaterializedResult result = computeActual("SELECT * FROM test_schema.alter_drop_ins ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);
            // Both rows should have exactly 2 columns
            assertThat(result.getMaterializedRows().get(0).getFieldCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(1).getFieldCount()).isEqualTo(2);
        }
        finally {
            tryDropTable("test_schema.alter_drop_ins");
        }
    }

    @Test
    public void testRenameColumnPreservesData()
    {
        try {
            computeActual("CREATE TABLE test_schema.rename_data (id INTEGER, value DOUBLE)");
            computeActual("INSERT INTO test_schema.rename_data VALUES (1, 42.5), (2, 99.9)");

            computeActual("ALTER TABLE test_schema.rename_data RENAME COLUMN value TO score");

            // Data should be intact under new name
            MaterializedResult result = computeActual("SELECT sum(score) FROM test_schema.rename_data");
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(142.4);
        }
        finally {
            tryDropTable("test_schema.rename_data");
        }
    }

    // ==================== Helpers ====================

    private void tryDropTable(String tableName)
    {
        try {
            computeActual("DROP TABLE " + tableName);
        }
        catch (Exception ignored) {
        }
    }

    private void tryDropSchema(String schemaName)
    {
        try {
            computeActual("DROP SCHEMA " + schemaName);
        }
        catch (Exception ignored) {
        }
    }
}
