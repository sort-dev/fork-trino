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

import org.junit.jupiter.api.Test;

import static io.trino.plugin.ducklake.DucklakeCatalogProvider.CatalogType.DUCKDB;
import static io.trino.plugin.ducklake.DucklakeCatalogProvider.CatalogType.GENERIC_JDBC;
import static io.trino.plugin.ducklake.DucklakeCatalogProvider.CatalogType.POSTGRESQL;
import static io.trino.plugin.ducklake.DucklakeCatalogProvider.CatalogType.SQLITE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDucklakeCatalogProvider
{
    @Test
    public void testRoutesSqlite()
    {
        assertThat(DucklakeCatalogProvider.catalogTypeForUrl("jdbc:sqlite:target/test-catalog/catalog.db"))
                .isEqualTo(SQLITE);
    }

    @Test
    public void testRoutesPostgreSql()
    {
        assertThat(DucklakeCatalogProvider.catalogTypeForUrl("jdbc:postgresql://localhost:5432/ducklake"))
                .isEqualTo(POSTGRESQL);
    }

    @Test
    public void testRoutesDuckDb()
    {
        assertThat(DucklakeCatalogProvider.catalogTypeForUrl("jdbc:duckdb:target/test-catalog/catalog.duckdb"))
                .isEqualTo(DUCKDB);
    }

    @Test
    public void testRoutesGeneric()
    {
        assertThat(DucklakeCatalogProvider.catalogTypeForUrl("jdbc:sqlserver://localhost:1433;databaseName=ducklake"))
                .isEqualTo(GENERIC_JDBC);
    }
}
