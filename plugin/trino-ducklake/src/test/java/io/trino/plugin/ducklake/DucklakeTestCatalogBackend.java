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

import static java.util.Locale.ENGLISH;

public enum DucklakeTestCatalogBackend
{
    SQLITE,
    POSTGRESQL,
    DUCKDB;

    private static final String BACKEND_PROPERTY = "ducklake.test.catalog-backend";

    public static DucklakeTestCatalogBackend current()
    {
        String value = System.getProperty(BACKEND_PROPERTY, POSTGRESQL.name()).trim();
        return switch (value.toLowerCase(ENGLISH)) {
            case "sqlite" -> SQLITE;
            case "postgres", "postgresql" -> POSTGRESQL;
            case "duckdb" -> DUCKDB;
            default -> throw new IllegalArgumentException("Unsupported catalog backend: " + value + ". Allowed values: sqlite, postgresql, duckdb");
        };
    }
}
