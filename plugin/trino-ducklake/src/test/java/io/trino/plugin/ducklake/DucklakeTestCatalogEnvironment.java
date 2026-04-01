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

import com.google.common.collect.ImmutableMap;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public final class DucklakeTestCatalogEnvironment
{
    private static final Object LOCK = new Object();
    private static final Path SQLITE_CATALOG_PATH = Path.of("target/test-catalog/catalog.db");
    private static final Path SQLITE_DATA_PATH = SQLITE_CATALOG_PATH.getParent().resolve("data");

    private static volatile TestingDucklakePostgreSqlCatalogServer postgreSqlServer;
    private static volatile RuntimeException postgreSqlBackendUnavailable;

    private DucklakeTestCatalogEnvironment() {}

    public static Path ensureSqliteCatalog()
            throws Exception
    {
        if (!Files.exists(SQLITE_CATALOG_PATH)) {
            synchronized (LOCK) {
                if (!Files.exists(SQLITE_CATALOG_PATH)) {
                    DucklakeCatalogGenerator.generateTestCatalog();
                }
            }
        }
        return SQLITE_CATALOG_PATH;
    }

    public static DucklakeConfig createDucklakeConfig()
            throws Exception
    {
        DucklakeTestCatalogBackend backend = DucklakeTestCatalogBackend.current();
        Path sqliteCatalogPath = ensureSqliteCatalog();

        DucklakeConfig config = new DucklakeConfig()
                .setDataPath(SQLITE_DATA_PATH.toAbsolutePath().toString())
                .setMaxCatalogConnections(5);

        switch (backend) {
            case SQLITE -> config.setCatalogDatabaseUrl("jdbc:sqlite:" + sqliteCatalogPath.toAbsolutePath());
            case POSTGRESQL -> {
                TestingDucklakePostgreSqlCatalogServer server = getPostgreSqlServer(sqliteCatalogPath);
                config.setCatalogDatabaseUrl(server.getJdbcUrl());
                config.setCatalogDatabaseUser(server.getUser());
                config.setCatalogDatabasePassword(server.getPassword());
            }
        }

        return config;
    }

    public static Map<String, String> getConnectorProperties()
            throws Exception
    {
        DucklakeConfig config = createDucklakeConfig();
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("ducklake.catalog.database-url", requireNonNull(config.getCatalogDatabaseUrl(), "catalogDatabaseUrl is null"))
                .put("ducklake.data-path", requireNonNull(config.getDataPath(), "dataPath is null"));

        if (config.getCatalogDatabaseUser() != null) {
            properties.put("ducklake.catalog.database-user", config.getCatalogDatabaseUser());
        }
        if (config.getCatalogDatabasePassword() != null) {
            properties.put("ducklake.catalog.database-password", config.getCatalogDatabasePassword());
        }

        return properties.buildOrThrow();
    }

    public static DucklakeTestCatalogBackend currentBackend()
    {
        return DucklakeTestCatalogBackend.current();
    }

    private static TestingDucklakePostgreSqlCatalogServer getPostgreSqlServer(Path sqliteCatalogPath)
            throws Exception
    {
        RuntimeException backendUnavailable = postgreSqlBackendUnavailable;
        if (backendUnavailable != null) {
            skipPostgreSqlTests(backendUnavailable);
        }

        TestingDucklakePostgreSqlCatalogServer server = postgreSqlServer;
        if (server == null) {
            synchronized (LOCK) {
                server = postgreSqlServer;
                if (server == null) {
                    try {
                        server = new TestingDucklakePostgreSqlCatalogServer(sqliteCatalogPath);
                        postgreSqlServer = server;
                        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
                    }
                    catch (RuntimeException e) {
                        if (isDockerUnavailable(e)) {
                            postgreSqlBackendUnavailable = e;
                            skipPostgreSqlTests(e);
                        }
                        throw e;
                    }
                }
            }
        }
        return server;
    }

    private static boolean isDockerUnavailable(Throwable throwable)
    {
        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && (message.contains("Could not find a valid Docker environment")
                    || message.contains("Previous attempts to find a Docker environment failed"))) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static void skipPostgreSqlTests(Throwable cause)
    {
        org.junit.jupiter.api.Assumptions.assumeTrue(
                false,
                () -> "PostgreSQL Ducklake tests require a working Docker environment: " + cause.getMessage());
    }
}
