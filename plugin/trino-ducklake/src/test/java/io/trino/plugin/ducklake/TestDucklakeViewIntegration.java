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
 * Integration tests for DuckLake view support (Trino-dialect views).
 * Uses an isolated catalog to avoid cross-test interference from write operations.
 *
 * DuckDB-created views are not exposed until a SQL transpiler is integrated.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeViewIntegration
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DucklakeQueryRunner.builder()
                .useIsolatedCatalog("view-integration")
                .build();
    }

    // ==================== DuckDB-created views are hidden ====================

    @Test
    public void testDuckdbViewsNotVisible()
    {
        // DuckDB-created views (simple_view, aliased_view, duckdb_specific_view)
        // should not be visible — they use dialect='duckdb' and transpiler is not configured
        MaterializedResult result = computeActual("SELECT table_name FROM information_schema.views WHERE table_schema = 'test_schema'");
        List<String> viewNames = result.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .toList();
        assertThat(viewNames).doesNotContain("simple_view", "aliased_view", "duckdb_specific_view");
    }

    // ==================== Trino-created views (CREATE VIEW / DROP VIEW) ====================

    @Test
    public void testCreateAndQueryView()
    {
        try {
            computeActual("CREATE VIEW test_schema.trino_test_view AS SELECT id, name FROM simple_table WHERE id <= 2");

            MaterializedResult result = computeActual("SELECT id, name FROM test_schema.trino_test_view ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("Product A");
            assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo("Product B");
        }
        finally {
            tryDropView("test_schema.trino_test_view");
        }
    }

    @Test
    public void testCreateViewShowsInListViews()
    {
        try {
            computeActual("CREATE VIEW test_schema.listed_view AS SELECT id FROM simple_table");

            MaterializedResult result = computeActual("SELECT table_name FROM information_schema.views WHERE table_schema = 'test_schema'");
            List<String> viewNames = result.getMaterializedRows().stream()
                    .map(row -> row.getField(0).toString())
                    .toList();
            assertThat(viewNames).contains("listed_view");
        }
        finally {
            tryDropView("test_schema.listed_view");
        }
    }

    @Test
    public void testDropView()
    {
        computeActual("CREATE VIEW test_schema.drop_me_view AS SELECT id FROM simple_table");

        MaterializedResult beforeDrop = computeActual("SELECT table_name FROM information_schema.views WHERE table_schema = 'test_schema' AND table_name = 'drop_me_view'");
        assertThat(beforeDrop.getRowCount()).isEqualTo(1);

        computeActual("DROP VIEW test_schema.drop_me_view");

        MaterializedResult afterDrop = computeActual("SELECT table_name FROM information_schema.views WHERE table_schema = 'test_schema' AND table_name = 'drop_me_view'");
        assertThat(afterDrop.getRowCount()).isEqualTo(0);
    }

    @Test
    public void testCreateOrReplaceView()
    {
        try {
            computeActual("CREATE VIEW test_schema.replaceable_view AS SELECT id, name FROM simple_table WHERE id = 1");

            MaterializedResult before = computeActual("SELECT count(*) FROM test_schema.replaceable_view");
            assertThat(before.getMaterializedRows().get(0).getField(0)).isEqualTo(1L);

            computeActual("CREATE OR REPLACE VIEW test_schema.replaceable_view AS SELECT id, name FROM simple_table WHERE id <= 3");

            MaterializedResult after = computeActual("SELECT count(*) FROM test_schema.replaceable_view");
            assertThat(after.getMaterializedRows().get(0).getField(0)).isEqualTo(3L);
        }
        finally {
            tryDropView("test_schema.replaceable_view");
        }
    }

    @Test
    public void testDropNonexistentViewFails()
    {
        assertThatThrownBy(() -> computeActual("DROP VIEW test_schema.nonexistent_view"))
                .hasMessageContaining("nonexistent_view");
    }

    @Test
    public void testViewPreservesColumnTypes()
    {
        try {
            computeActual("CREATE VIEW test_schema.typed_view AS SELECT id, name, price, active, created_date FROM simple_table");

            MaterializedResult result = computeActual("DESCRIBE test_schema.typed_view");
            List<List<Object>> rows = result.getMaterializedRows().stream()
                    .map(row -> List.of(row.getField(0), row.getField(1)))
                    .toList();

            assertThat(rows).containsExactly(
                    List.of("id", "integer"),
                    List.of("name", "varchar"),
                    List.of("price", "double"),
                    List.of("active", "boolean"),
                    List.of("created_date", "date"));
        }
        finally {
            tryDropView("test_schema.typed_view");
        }
    }

    private void tryDropView(String viewName)
    {
        try {
            computeActual("DROP VIEW IF EXISTS " + viewName);
        }
        catch (Exception _) {
            // Ignore cleanup failures
        }
    }
}
