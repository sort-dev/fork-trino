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

import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

public class TestingDucklakePostgreSqlCatalogServer
        implements AutoCloseable
{
    private static final String IMAGE = "postgres:18";
    private static final String DATABASE = "ducklake";
    private static final String USER = "test";
    private static final String PASSWORD = "test";

    private final PostgreSQLContainer container;

    public TestingDucklakePostgreSqlCatalogServer()
    {
        container = new PostgreSQLContainer(DockerImageName.parse(IMAGE))
                .withDatabaseName(DATABASE)
                .withUsername(USER)
                .withPassword(PASSWORD)
                .withStartupAttempts(3);
        container.start();
    }

    public String getJdbcUrl()
    {
        return container.getJdbcUrl();
    }

    public String getUser()
    {
        return USER;
    }

    public String getPassword()
    {
        return PASSWORD;
    }

    public String getDuckDbAttachUri()
    {
        return "ducklake:postgres:dbname=" + DATABASE +
                " host=" + container.getHost() +
                " port=" + container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT) +
                " user=" + USER +
                " password=" + PASSWORD;
    }

    @Override
    public void close()
    {
        container.close();
    }
}
