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

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.plugin.ducklake.catalog.DucklakeCatalog;
import io.trino.plugin.ducklake.catalog.JdbcDucklakeCatalog;
import io.trino.plugin.ducklake.catalog.PostgreSqlDucklakeCatalog;
import io.trino.plugin.ducklake.catalog.SqliteDucklakeCatalog;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class DucklakeCatalogProvider
        implements Provider<DucklakeCatalog>
{
    private final DucklakeConfig config;

    @Inject
    public DucklakeCatalogProvider(DucklakeConfig config)
    {
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public DucklakeCatalog get()
    {
        String databaseUrl = requireNonNull(config.getCatalogDatabaseUrl(), "ducklake.catalog.database-url is null");
        String normalizedUrl = databaseUrl.toLowerCase(ENGLISH);

        if (normalizedUrl.startsWith("jdbc:sqlite:")) {
            return new SqliteDucklakeCatalog(config);
        }
        if (normalizedUrl.startsWith("jdbc:postgresql:")) {
            return new PostgreSqlDucklakeCatalog(config);
        }

        return new JdbcDucklakeCatalog(config);
    }
}
