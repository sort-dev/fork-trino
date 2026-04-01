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

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.lang.String.format;

public class TestingDucklakePostgreSqlCatalogServer
        implements AutoCloseable
{
    private static final String DATABASE = "ducklake";
    private static final String USER = "test";
    private static final String PASSWORD = "test";

    private final PostgreSQLContainer container;

    public TestingDucklakePostgreSqlCatalogServer(Path sqliteCatalogPath)
            throws Exception
    {
        container = new PostgreSQLContainer(DockerImageName.parse("postgres:18"))
                .withDatabaseName(DATABASE)
                .withUsername(USER)
                .withPassword(PASSWORD)
                .withStartupAttempts(3);
        container.start();

        loadCatalogFromSqlite(sqliteCatalogPath);
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

    @Override
    public void close()
    {
        container.close();
    }

    private void loadCatalogFromSqlite(Path sqliteCatalogPath)
            throws Exception
    {
        String sqliteUrl = "jdbc:sqlite:" + sqliteCatalogPath.toAbsolutePath();

        try (Connection sqliteConnection = DriverManager.getConnection(sqliteUrl);
                Connection postgresConnection = DriverManager.getConnection(getJdbcUrl(), connectionProperties())) {
            postgresConnection.setAutoCommit(false);

            List<String> tableNames = listTableNames(sqliteConnection);
            for (String tableName : tableNames) {
                createTable(postgresConnection, sqliteConnection, tableName);
                copyTableData(sqliteConnection, postgresConnection, tableName);
            }

            postgresConnection.commit();
        }
    }

    private static List<String> listTableNames(Connection sqliteConnection)
            throws SQLException
    {
        List<String> tableNames = new ArrayList<>();
        try (PreparedStatement statement = sqliteConnection.prepareStatement(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'ducklake_%' ORDER BY name");
                ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                tableNames.add(resultSet.getString(1));
            }
        }
        return tableNames;
    }

    private static void createTable(Connection postgresConnection, Connection sqliteConnection, String tableName)
            throws SQLException
    {
        String dropSql = format("DROP TABLE IF EXISTS %s", quoteIdentifier(tableName));
        try (Statement statement = postgresConnection.createStatement()) {
            statement.execute(dropSql);
        }

        String pragmaSql = format("PRAGMA table_info(%s)", quoteSqlLiteral(tableName));
        List<String> columnDefinitions = new ArrayList<>();
        try (Statement statement = sqliteConnection.createStatement();
                ResultSet resultSet = statement.executeQuery(pragmaSql)) {
            while (resultSet.next()) {
                String columnName = resultSet.getString("name");
                String sqliteType = resultSet.getString("type");
                boolean notNull = resultSet.getInt("notnull") == 1;

                String columnDefinition = quoteIdentifier(columnName) + " " + toPostgreSqlType(sqliteType);
                if (notNull) {
                    columnDefinition += " NOT NULL";
                }
                columnDefinitions.add(columnDefinition);
            }
        }

        if (columnDefinitions.isEmpty()) {
            throw new IllegalStateException("No columns found for table: " + tableName);
        }

        String createSql = format(
                "CREATE TABLE %s (%s)",
                quoteIdentifier(tableName),
                String.join(", ", columnDefinitions));
        try (Statement statement = postgresConnection.createStatement()) {
            statement.execute(createSql);
        }
    }

    private static void copyTableData(Connection sqliteConnection, Connection postgresConnection, String tableName)
            throws SQLException
    {
        String selectSql = format("SELECT * FROM %s", quoteIdentifier(tableName));
        try (Statement selectStatement = sqliteConnection.createStatement();
                ResultSet resultSet = selectStatement.executeQuery(selectSql)) {
            int columnCount = resultSet.getMetaData().getColumnCount();
            String insertSql = format(
                    "INSERT INTO %s VALUES (%s)",
                    quoteIdentifier(tableName),
                    String.join(", ", java.util.Collections.nCopies(columnCount, "?")));

            try (PreparedStatement insertStatement = postgresConnection.prepareStatement(insertSql)) {
                int rowsInBatch = 0;
                while (resultSet.next()) {
                    for (int column = 1; column <= columnCount; column++) {
                        Object value = resultSet.getObject(column);
                        if (value instanceof byte[] bytes) {
                            insertStatement.setBytes(column, bytes);
                        }
                        else {
                            insertStatement.setObject(column, value);
                        }
                    }
                    insertStatement.addBatch();
                    rowsInBatch++;

                    if (rowsInBatch >= 1000) {
                        insertStatement.executeBatch();
                        rowsInBatch = 0;
                    }
                }

                if (rowsInBatch > 0) {
                    insertStatement.executeBatch();
                }
            }
        }
    }

    private static String toPostgreSqlType(String sqliteType)
    {
        if (sqliteType == null || sqliteType.isBlank()) {
            return "TEXT";
        }

        String normalized = sqliteType.trim().toUpperCase();
        return switch (normalized) {
            case "BIGINT", "INTEGER", "INT" -> "BIGINT";
            case "VARCHAR", "TEXT" -> "TEXT";
            case "BLOB" -> "BYTEA";
            default -> throw new IllegalArgumentException("Unsupported SQLite type in Ducklake catalog: " + sqliteType);
        };
    }

    private static String quoteIdentifier(String identifier)
    {
        return '"' + identifier.replace("\"", "\"\"") + '"';
    }

    private static String quoteSqlLiteral(String value)
    {
        return "'" + value.replace("'", "''") + "'";
    }

    private static Properties connectionProperties()
    {
        Properties properties = new Properties();
        properties.setProperty("user", USER);
        properties.setProperty("password", PASSWORD);
        return properties;
    }
}
