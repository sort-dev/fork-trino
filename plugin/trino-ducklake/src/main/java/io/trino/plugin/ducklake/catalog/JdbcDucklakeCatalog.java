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
package io.trino.plugin.ducklake.catalog;

import com.google.inject.Inject;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.airlift.log.Logger;
import io.trino.plugin.ducklake.DucklakeConfig;
import io.trino.spi.connector.SchemaTableName;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * JDBC implementation of DucklakeCatalog.
 * Queries the Ducklake metadata tables via JDBC.
 */
public class JdbcDucklakeCatalog
        implements DucklakeCatalog
{
    private static final Logger log = Logger.get(JdbcDucklakeCatalog.class);

    private final DataSource dataSource;
    private final HikariDataSource hikariDataSource;

    @Inject
    public JdbcDucklakeCatalog(DucklakeConfig config)
    {
        requireNonNull(config, "config is null");

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.getCatalogDatabaseUrl());
        if (config.getCatalogDatabaseUser() != null) {
            hikariConfig.setUsername(config.getCatalogDatabaseUser());
        }
        if (config.getCatalogDatabasePassword() != null) {
            hikariConfig.setPassword(config.getCatalogDatabasePassword());
        }
        hikariConfig.setMaximumPoolSize(config.getMaxCatalogConnections());
        hikariConfig.setMinimumIdle(1);
        hikariConfig.setConnectionTimeout(30000);

        this.hikariDataSource = new HikariDataSource(hikariConfig);
        this.dataSource = hikariDataSource;

        log.info("Initialized Ducklake JDBC catalog: %s", config.getCatalogDatabaseUrl());
    }

    @Override
    public long getCurrentSnapshotId()
    {
        String sql = "SELECT snapshot_id FROM ducklake_snapshot WHERE snapshot_id = (SELECT max(snapshot_id) FROM ducklake_snapshot)";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return rs.getLong("snapshot_id");
            }
            throw new IllegalStateException("No snapshots found in ducklake_snapshot table");
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get current snapshot", e);
        }
    }

    @Override
    public Optional<DucklakeSnapshot> getSnapshot(long snapshotId)
    {
        String sql = "SELECT snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id " +
                     "FROM ducklake_snapshot WHERE snapshot_id = ?";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(readSnapshot(rs));
                }
                return Optional.empty();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get snapshot: " + snapshotId, e);
        }
    }

    @Override
    public Optional<DucklakeSnapshot> getSnapshotAtOrBefore(Instant timestamp)
    {
        String sql = "SELECT snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id " +
                     "FROM ducklake_snapshot " +
                     "ORDER BY snapshot_id DESC";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    DucklakeSnapshot snapshot = readSnapshot(rs);
                    if (!snapshot.snapshotTime().isAfter(timestamp)) {
                        return Optional.of(snapshot);
                    }
                }
                return Optional.empty();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get snapshot at or before timestamp: " + timestamp, e);
        }
    }

    @Override
    public List<DucklakeSnapshot> listSnapshots()
    {
        String sql = "SELECT snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id " +
                     "FROM ducklake_snapshot " +
                     "ORDER BY snapshot_id DESC";

        List<DucklakeSnapshot> snapshots = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                snapshots.add(readSnapshot(rs));
            }
            return snapshots;
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to list snapshots", e);
        }
    }

    @Override
    public List<DucklakeSnapshotChange> listSnapshotChanges()
    {
        String sql = "SELECT snapshot_id, changes_made, author, commit_message, commit_extra_info " +
                     "FROM ducklake_snapshot_changes " +
                     "ORDER BY snapshot_id DESC";

        List<DucklakeSnapshotChange> changes = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                changes.add(new DucklakeSnapshotChange(
                        rs.getLong("snapshot_id"),
                        getStringOptional(rs, "changes_made"),
                        getStringOptional(rs, "author"),
                        getStringOptional(rs, "commit_message"),
                        getStringOptional(rs, "commit_extra_info")));
            }
            return changes;
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to list snapshot changes", e);
        }
    }

    @Override
    public List<DucklakeSchema> listSchemas(long snapshotId)
    {
        String sql = "SELECT schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative " +
                     "FROM ducklake_schema " +
                     "WHERE ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)";

        List<DucklakeSchema> schemas = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, snapshotId);
            stmt.setLong(2, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    schemas.add(new DucklakeSchema(
                            rs.getLong("schema_id"),
                            UUID.fromString(rs.getString("schema_uuid")),
                            rs.getLong("begin_snapshot"),
                            getLongOptional(rs, "end_snapshot"),
                            rs.getString("schema_name"),
                            getStringOptional(rs, "path"),
                            getBooleanOptional(rs, "path_is_relative")));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to list schemas at snapshot: " + snapshotId, e);
        }

        return schemas;
    }

    @Override
    public Optional<DucklakeSchema> getSchema(String schemaName, long snapshotId)
    {
        String sql = "SELECT schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative " +
                     "FROM ducklake_schema " +
                     "WHERE schema_name = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, schemaName);
            stmt.setLong(2, snapshotId);
            stmt.setLong(3, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new DucklakeSchema(
                            rs.getLong("schema_id"),
                            UUID.fromString(rs.getString("schema_uuid")),
                            rs.getLong("begin_snapshot"),
                            getLongOptional(rs, "end_snapshot"),
                            rs.getString("schema_name"),
                            getStringOptional(rs, "path"),
                            getBooleanOptional(rs, "path_is_relative")));
                }
                return Optional.empty();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get schema: " + schemaName + " at snapshot: " + snapshotId, e);
        }
    }

    @Override
    public List<DucklakeTable> listTables(long schemaId, long snapshotId)
    {
        String sql = "SELECT table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative " +
                     "FROM ducklake_table " +
                     "WHERE schema_id = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)";

        List<DucklakeTable> tables = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, schemaId);
            stmt.setLong(2, snapshotId);
            stmt.setLong(3, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    tables.add(new DucklakeTable(
                            rs.getLong("table_id"),
                            UUID.fromString(rs.getString("table_uuid")),
                            rs.getLong("begin_snapshot"),
                            getLongOptional(rs, "end_snapshot"),
                            rs.getLong("schema_id"),
                            rs.getString("table_name"),
                            getStringOptional(rs, "path"),
                            getBooleanOptional(rs, "path_is_relative")));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to list tables for schema: " + schemaId + " at snapshot: " + snapshotId, e);
        }

        return tables;
    }

    @Override
    public Optional<DucklakeTable> getTable(SchemaTableName tableName, long snapshotId)
    {
        // First get the schema
        Optional<DucklakeSchema> schema = getSchema(tableName.getSchemaName(), snapshotId);
        if (schema.isEmpty()) {
            return Optional.empty();
        }

        String sql = "SELECT table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative " +
                     "FROM ducklake_table " +
                     "WHERE schema_id = ? AND table_name = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, schema.get().schemaId());
            stmt.setString(2, tableName.getTableName());
            stmt.setLong(3, snapshotId);
            stmt.setLong(4, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new DucklakeTable(
                            rs.getLong("table_id"),
                            UUID.fromString(rs.getString("table_uuid")),
                            rs.getLong("begin_snapshot"),
                            getLongOptional(rs, "end_snapshot"),
                            rs.getLong("schema_id"),
                            rs.getString("table_name"),
                            getStringOptional(rs, "path"),
                            getBooleanOptional(rs, "path_is_relative")));
                }
                return Optional.empty();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get table: " + tableName + " at snapshot: " + snapshotId, e);
        }
    }

    @Override
    public Optional<DucklakeTable> getTableById(long tableId, long snapshotId)
    {
        String sql = "SELECT table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative " +
                     "FROM ducklake_table " +
                     "WHERE table_id = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, snapshotId);
            stmt.setLong(3, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new DucklakeTable(
                            rs.getLong("table_id"),
                            UUID.fromString(rs.getString("table_uuid")),
                            rs.getLong("begin_snapshot"),
                            getLongOptional(rs, "end_snapshot"),
                            rs.getLong("schema_id"),
                            rs.getString("table_name"),
                            getStringOptional(rs, "path"),
                            getBooleanOptional(rs, "path_is_relative")));
                }
                return Optional.empty();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get table by ID: " + tableId + " at snapshot: " + snapshotId, e);
        }
    }

    @Override
    public List<DucklakeColumn> getTableColumns(long tableId, long snapshotId)
    {
        String sql = "SELECT column_id, begin_snapshot, end_snapshot, table_id, column_order, column_name, column_type, nulls_allowed, parent_column " +
                     "FROM ducklake_column " +
                     "WHERE table_id = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL) " +
                     "ORDER BY column_order, column_id";

        List<DucklakeColumn> allColumns = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, snapshotId);
            stmt.setLong(3, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    allColumns.add(new DucklakeColumn(
                            rs.getLong("column_id"),
                            rs.getLong("begin_snapshot"),
                            getLongOptional(rs, "end_snapshot"),
                            rs.getLong("table_id"),
                            rs.getLong("column_order"),
                            rs.getString("column_name"),
                            rs.getString("column_type"),
                            rs.getBoolean("nulls_allowed"),
                            getLongOptional(rs, "parent_column")));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get columns for table: " + tableId + " at snapshot: " + snapshotId, e);
        }

        Map<Long, List<DucklakeColumn>> childrenByParent = new HashMap<>();
        for (DucklakeColumn column : allColumns) {
            column.parentColumn().ifPresent(parent ->
                    childrenByParent.computeIfAbsent(parent, ignored -> new ArrayList<>()).add(column));
        }

        List<DucklakeColumn> topLevelColumns = new ArrayList<>();
        for (DucklakeColumn column : allColumns) {
            if (column.parentColumn().isEmpty()) {
                topLevelColumns.add(new DucklakeColumn(
                        column.columnId(),
                        column.beginSnapshot(),
                        column.endSnapshot(),
                        column.tableId(),
                        column.columnOrder(),
                        column.columnName(),
                        resolveColumnType(column, childrenByParent),
                        column.nullsAllowed(),
                        Optional.empty()));
            }
        }

        return topLevelColumns;
    }

    @Override
    public List<DucklakeDataFile> getDataFiles(long tableId, long snapshotId)
    {
        String sql = "SELECT data.data_file_id, data.table_id, data.begin_snapshot, data.end_snapshot, data.file_order, " +
                     "       data.path, data.path_is_relative, data.file_format, data.record_count, data.file_size_bytes, " +
                     "       data.footer_size, data.row_id_start, data.partition_id, " +
                     "       del.path AS delete_file_path, del.path_is_relative AS delete_path_is_relative " +
                     "FROM ducklake_data_file AS data " +
                     "LEFT JOIN ducklake_delete_file AS del ON data.data_file_id = del.data_file_id " +
                     "  AND ? >= del.begin_snapshot AND (? < del.end_snapshot OR del.end_snapshot IS NULL) " +
                     "WHERE data.table_id = ? AND ? >= data.begin_snapshot AND (? < data.end_snapshot OR data.end_snapshot IS NULL) " +
                     "ORDER BY data.file_order";

        List<DucklakeDataFile> files = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, snapshotId);
            stmt.setLong(2, snapshotId);
            stmt.setLong(3, tableId);
            stmt.setLong(4, snapshotId);
            stmt.setLong(5, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    files.add(new DucklakeDataFile(
                            rs.getLong("data_file_id"),
                            rs.getLong("table_id"),
                            rs.getLong("begin_snapshot"),
                            getLongOptional(rs, "end_snapshot"),
                            rs.getLong("file_order"),
                            rs.getString("path"),
                            rs.getBoolean("path_is_relative"),
                            rs.getString("file_format"),
                            rs.getLong("record_count"),
                            rs.getLong("file_size_bytes"),
                            rs.getLong("footer_size"),
                            rs.getLong("row_id_start"),
                            getLongOptional(rs, "partition_id"),
                            getStringOptional(rs, "delete_file_path"),
                            getBooleanOptional(rs, "delete_path_is_relative")));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get data files for table: " + tableId + " at snapshot: " + snapshotId, e);
        }

        return files;
    }

    @Override
    public List<Long> getDataFileIdsForPredicate(long tableId, long columnId, long snapshotId, Object minValue, Object maxValue)
    {
        Optional<String> columnType = getColumnType(tableId, columnId, snapshotId);
        if (columnType.isEmpty()) {
            return List.of();
        }

        String sql = "SELECT stats.data_file_id, stats.min_value, stats.max_value " +
                     "FROM ducklake_file_column_stats AS stats " +
                     "JOIN ducklake_data_file AS data ON stats.data_file_id = data.data_file_id " +
                     "WHERE stats.table_id = ? AND stats.column_id = ? " +
                     "  AND data.table_id = ? " +
                     "  AND ? >= data.begin_snapshot AND (? < data.end_snapshot OR data.end_snapshot IS NULL)";

        List<Long> fileIds = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, columnId);
            stmt.setLong(3, tableId);
            stmt.setLong(4, snapshotId);
            stmt.setLong(5, snapshotId);

            Comparable<?> lowerBound = normalizePredicateValue(columnType.get(), minValue);
            Comparable<?> upperBound = normalizePredicateValue(columnType.get(), maxValue);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Comparable<?> minStat = parseStatValue(columnType.get(), rs.getString("min_value"));
                    Comparable<?> maxStat = parseStatValue(columnType.get(), rs.getString("max_value"));

                    if (isWithinBounds(lowerBound, upperBound, minStat, maxStat)) {
                        fileIds.add(rs.getLong("data_file_id"));
                    }
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get file IDs for predicate on table: " + tableId + ", column: " + columnId, e);
        }

        return fileIds;
    }

    @Override
    public Optional<DucklakeTableStats> getTableStats(long tableId)
    {
        String sql = "SELECT table_id, record_count, file_size_bytes FROM ducklake_table_stats WHERE table_id = ?";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new DucklakeTableStats(
                            rs.getLong("table_id"),
                            rs.getLong("record_count"),
                            rs.getLong("file_size_bytes")));
                }
                return Optional.empty();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get table stats for table: " + tableId, e);
        }
    }

    @Override
    public List<DucklakeColumnStats> getColumnStats(long tableId, long snapshotId, Map<Long, String> columnTypes)
    {
        // Fetch per-file stats and aggregate in Java with typed min/max comparison
        String sql = "SELECT stats.column_id, stats.value_count, stats.null_count, " +
                     "       stats.column_size_bytes, stats.min_value, stats.max_value " +
                     "FROM ducklake_file_column_stats AS stats " +
                     "JOIN ducklake_data_file AS data ON stats.data_file_id = data.data_file_id " +
                     "WHERE stats.table_id = ? AND data.table_id = ? " +
                     "  AND ? >= data.begin_snapshot AND (? < data.end_snapshot OR data.end_snapshot IS NULL)";

        // Accumulate per-column aggregates
        Map<Long, long[]> countAccumulators = new HashMap<>(); // [valueCount, nullCount, sizeBytes]
        Map<Long, String> minAccumulators = new HashMap<>();
        Map<Long, String> maxAccumulators = new HashMap<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, tableId);
            stmt.setLong(3, snapshotId);
            stmt.setLong(4, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    long columnId = rs.getLong("column_id");
                    long valueCount = rs.getLong("value_count");
                    long nullCount = rs.getLong("null_count");
                    long sizeBytes = rs.getLong("column_size_bytes");
                    String minValue = rs.getString("min_value");
                    String maxValue = rs.getString("max_value");

                    countAccumulators.computeIfAbsent(columnId, _ -> new long[3]);
                    long[] counts = countAccumulators.get(columnId);
                    counts[0] += valueCount;
                    counts[1] += nullCount;
                    counts[2] += sizeBytes;

                    String columnType = columnTypes.getOrDefault(columnId, "");
                    if (minValue != null) {
                        minAccumulators.merge(columnId, minValue, (a, b) -> typedMin(a, b, columnType));
                    }
                    if (maxValue != null) {
                        maxAccumulators.merge(columnId, maxValue, (a, b) -> typedMax(a, b, columnType));
                    }
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get column stats for table: " + tableId + " at snapshot: " + snapshotId, e);
        }

        List<DucklakeColumnStats> result = new ArrayList<>();
        for (Map.Entry<Long, long[]> entry : countAccumulators.entrySet()) {
            long columnId = entry.getKey();
            long[] counts = entry.getValue();
            result.add(new DucklakeColumnStats(
                    columnId,
                    counts[0],
                    counts[1],
                    counts[2],
                    Optional.ofNullable(minAccumulators.get(columnId)),
                    Optional.ofNullable(maxAccumulators.get(columnId))));
        }

        return result;
    }

    private static String typedMin(String a, String b, String columnType)
    {
        return typedCompare(a, b, columnType) <= 0 ? a : b;
    }

    private static String typedMax(String a, String b, String columnType)
    {
        return typedCompare(a, b, columnType) >= 0 ? a : b;
    }

    private static int typedCompare(String a, String b, String columnType)
    {
        try {
            return switch (columnType.toLowerCase(java.util.Locale.ENGLISH)) {
                case "bigint", "integer", "int", "smallint", "tinyint", "hugeint" ->
                        Long.compare(Long.parseLong(a), Long.parseLong(b));
                case "double", "real", "float", "decimal" ->
                        Double.compare(Double.parseDouble(a), Double.parseDouble(b));
                case "date" ->
                        java.time.LocalDate.parse(a).compareTo(java.time.LocalDate.parse(b));
                default -> a.compareTo(b);
            };
        }
        catch (RuntimeException _) {
            // If parsing fails, fall back to string comparison (conservative)
            return a.compareTo(b);
        }
    }

    @Override
    public List<DucklakePartitionSpec> getPartitionSpecs(long tableId, long snapshotId)
    {
        String sql = "SELECT pi.partition_id, pi.table_id, " +
                     "       pc.partition_key_index, pc.column_id, pc.transform " +
                     "FROM ducklake_partition_info pi " +
                     "JOIN ducklake_partition_column pc ON pi.partition_id = pc.partition_id AND pi.table_id = pc.table_id " +
                     "WHERE pi.table_id = ? " +
                     "  AND ? >= pi.begin_snapshot AND (? < pi.end_snapshot OR pi.end_snapshot IS NULL) " +
                     "ORDER BY pi.partition_id, pc.partition_key_index";

        Map<Long, List<DucklakePartitionField>> fieldsByPartition = new LinkedHashMap<>();
        Map<Long, Long> tableIdByPartition = new HashMap<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, snapshotId);
            stmt.setLong(3, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    long partitionId = rs.getLong("partition_id");
                    tableIdByPartition.put(partitionId, rs.getLong("table_id"));
                    fieldsByPartition.computeIfAbsent(partitionId, _ -> new ArrayList<>())
                            .add(new DucklakePartitionField(
                                    rs.getInt("partition_key_index"),
                                    rs.getLong("column_id"),
                                    DucklakePartitionTransform.fromString(rs.getString("transform"))));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get partition specs for table: " + tableId + " at snapshot: " + snapshotId, e);
        }

        List<DucklakePartitionSpec> specs = new ArrayList<>();
        for (Map.Entry<Long, List<DucklakePartitionField>> entry : fieldsByPartition.entrySet()) {
            specs.add(new DucklakePartitionSpec(entry.getKey(), tableIdByPartition.get(entry.getKey()), entry.getValue()));
        }
        return specs;
    }

    @Override
    public Map<Long, List<DucklakeFilePartitionValue>> getFilePartitionValues(long tableId, long snapshotId)
    {
        String sql = "SELECT fpv.data_file_id, fpv.partition_key_index, fpv.partition_value " +
                     "FROM ducklake_file_partition_value fpv " +
                     "JOIN ducklake_data_file df ON fpv.data_file_id = df.data_file_id AND fpv.table_id = df.table_id " +
                     "WHERE fpv.table_id = ? " +
                     "  AND ? >= df.begin_snapshot AND (? < df.end_snapshot OR df.end_snapshot IS NULL)";

        Map<Long, List<DucklakeFilePartitionValue>> result = new HashMap<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, snapshotId);
            stmt.setLong(3, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    long dataFileId = rs.getLong("data_file_id");
                    result.computeIfAbsent(dataFileId, _ -> new ArrayList<>())
                            .add(new DucklakeFilePartitionValue(
                                    dataFileId,
                                    rs.getInt("partition_key_index"),
                                    rs.getString("partition_value")));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get file partition values for table: " + tableId + " at snapshot: " + snapshotId, e);
        }

        return result;
    }

    @Override
    public Optional<DucklakeInlinedDataInfo> getInlinedDataInfo(long tableId, long snapshotId)
    {
        // First check if this table has inlined data
        String sql = "SELECT table_id, table_name, schema_version FROM ducklake_inlined_data_tables WHERE table_id = ?";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    long resolvedTableId = rs.getLong("table_id");
                    long schemaVersion = rs.getLong("schema_version");
                    String inlinedTableName = String.format("ducklake_inlined_data_%d_%d", resolvedTableId, schemaVersion);
                    try (PreparedStatement verifyStmt = conn.prepareStatement("SELECT 1 FROM " + inlinedTableName + " WHERE 1 = 0")) {
                        verifyStmt.executeQuery();
                    }
                    catch (SQLException e) {
                        // Catalog metadata can point to a dropped/non-materialized inlined table.
                        // Treat this as "no inlined data" so scan planning does not emit a dead split.
                        log.debug("Inlined data table %s not available for table %d: %s", inlinedTableName, tableId, e.getMessage());
                        return Optional.empty();
                    }

                    return Optional.of(new DucklakeInlinedDataInfo(
                            resolvedTableId,
                            rs.getString("table_name"),
                            schemaVersion));
                }
                return Optional.empty();
            }
        }
        catch (SQLException e) {
            // ducklake_inlined_data_tables may not exist in catalogs that never used inlining
            log.debug("Could not query inlined data tables (table may not exist): %s", e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public List<List<Object>> readInlinedData(long tableId, long schemaVersion, long snapshotId, List<DucklakeColumn> columns)
    {
        String inlinedTableName = String.format("ducklake_inlined_data_%d_%d", tableId, schemaVersion);

        String columnNames = columns.stream()
                .map(DucklakeColumn::columnName)
                .collect(Collectors.joining(", "));

        String sql = String.format(
                "SELECT %s FROM %s WHERE ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL) ORDER BY row_id",
                columnNames, inlinedTableName);

        List<List<Object>> rows = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, snapshotId);
            stmt.setLong(2, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                int columnCount = columns.size();
                while (rs.next()) {
                    List<Object> row = new ArrayList<>(columnCount);
                    for (int i = 1; i <= columnCount; i++) {
                        row.add(rs.getObject(i));
                    }
                    rows.add(row);
                }
            }
        }
        catch (SQLException e) {
            // The inlined data table may not exist if the table was created but never had data inserted,
            // or if the inlined data was flushed to Parquet files. Return empty in these cases.
            log.debug("Could not read inlined data from %s (table may not exist): %s", inlinedTableName, e.getMessage());
            return List.of();
        }

        return rows;
    }

    @Override
    public Optional<String> getDataPath()
    {
        String sql = "SELECT value FROM ducklake_metadata WHERE key = 'data_path'";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return Optional.of(rs.getString("value"));
            }
            return Optional.empty();
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get data path from ducklake_metadata", e);
        }
    }

    // ==================== View operations ====================

    @Override
    public List<DucklakeView> listViews(long schemaId, long snapshotId)
    {
        String sql = "SELECT view_id, view_uuid, begin_snapshot, end_snapshot, schema_id, view_name, dialect, sql, column_aliases " +
                     "FROM ducklake_view " +
                     "WHERE schema_id = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)";

        List<DucklakeView> views = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, schemaId);
            stmt.setLong(2, snapshotId);
            stmt.setLong(3, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    views.add(readView(rs));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to list views for schema: " + schemaId + " at snapshot: " + snapshotId, e);
        }

        return views;
    }

    @Override
    public Optional<DucklakeView> getView(String schemaName, String viewName, long snapshotId)
    {
        Optional<DucklakeSchema> schema = getSchema(schemaName, snapshotId);
        if (schema.isEmpty()) {
            return Optional.empty();
        }

        String sql = "SELECT view_id, view_uuid, begin_snapshot, end_snapshot, schema_id, view_name, dialect, sql, column_aliases " +
                     "FROM ducklake_view " +
                     "WHERE schema_id = ? AND view_name = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, schema.get().schemaId());
            stmt.setString(2, viewName);
            stmt.setLong(3, snapshotId);
            stmt.setLong(4, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(readView(rs));
                }
                return Optional.empty();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get view: " + schemaName + "." + viewName + " at snapshot: " + snapshotId, e);
        }
    }

    // Write transaction infrastructure

    @FunctionalInterface
    interface WriteTransactionAction
    {
        void execute(DucklakeWriteTransaction transaction)
                throws SQLException;
    }

    /**
     * Executes a write operation within an atomic snapshot transaction.
     * Handles connection management, snapshot creation, change tracking,
     * and commit/rollback. The caller provides a callback that performs
     * its mutations using the transaction context.
     */
    private void executeWriteTransaction(String operationDescription, WriteTransactionAction action)
    {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                // 1. Read current snapshot state
                long currentSnapshotId;
                long schemaVersion;
                long nextCatalogId;
                long nextFileId;
                try (PreparedStatement stmt = conn.prepareStatement(
                        "SELECT snapshot_id, schema_version, next_catalog_id, next_file_id " +
                                "FROM ducklake_snapshot WHERE snapshot_id = (SELECT max(snapshot_id) FROM ducklake_snapshot)")) {
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (!rs.next()) {
                            throw new IllegalStateException("No snapshots found");
                        }
                        currentSnapshotId = rs.getLong("snapshot_id");
                        schemaVersion = rs.getLong("schema_version");
                        nextCatalogId = rs.getLong("next_catalog_id");
                        nextFileId = rs.getLong("next_file_id");
                    }
                }

                // 2. Execute the caller's mutations
                DucklakeWriteTransaction tx = new DucklakeWriteTransaction(
                        conn, currentSnapshotId, schemaVersion, nextCatalogId, nextFileId);
                action.execute(tx);

                // 3. Create new snapshot row (with final allocated IDs)
                try (PreparedStatement stmt = conn.prepareStatement(
                        "INSERT INTO ducklake_snapshot (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) " +
                                "VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?)")) {
                    stmt.setLong(1, tx.getNewSnapshotId());
                    stmt.setLong(2, tx.getSchemaVersion());
                    stmt.setLong(3, tx.getFinalNextCatalogId());
                    stmt.setLong(4, tx.getFinalNextFileId());
                    stmt.executeUpdate();
                }

                // 4. Insert schema_versions row if schema version changed
                if (tx.getSchemaVersion() != schemaVersion) {
                    try (PreparedStatement stmt = conn.prepareStatement(
                            "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES (?, ?)")) {
                        stmt.setLong(1, tx.getNewSnapshotId());
                        stmt.setLong(2, tx.getSchemaVersion());
                        stmt.executeUpdate();
                    }
                }

                // 5. Insert snapshot changes
                for (String change : tx.getChanges()) {
                    try (PreparedStatement stmt = conn.prepareStatement(
                            "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made) VALUES (?, ?)")) {
                        stmt.setLong(1, tx.getNewSnapshotId());
                        stmt.setString(2, change);
                        stmt.executeUpdate();
                    }
                }

                conn.commit();
            }
            catch (Exception e) {
                conn.rollback();
                throw new RuntimeException("Failed to " + operationDescription, e);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to " + operationDescription, e);
        }
    }

    @Override
    public void createView(String schemaName, String viewName, String viewSql, String dialect, String columnAliases)
    {
        executeWriteTransaction("create view " + schemaName + "." + viewName, tx -> {
            long schemaId = tx.resolveSchemaId(schemaName);
            long viewId = tx.allocateCatalogId();
            tx.addChange("created_view:" + viewName);

            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "INSERT INTO ducklake_view (view_id, view_uuid, begin_snapshot, end_snapshot, schema_id, view_name, dialect, sql, column_aliases) " +
                            "VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?)")) {
                stmt.setLong(1, viewId);
                stmt.setString(2, UUID.randomUUID().toString());
                stmt.setLong(3, tx.getNewSnapshotId());
                stmt.setLong(4, schemaId);
                stmt.setString(5, viewName);
                stmt.setString(6, dialect);
                stmt.setString(7, viewSql);
                stmt.setString(8, columnAliases);
                stmt.executeUpdate();
            }
        });
    }

    @Override
    public void dropView(String schemaName, String viewName)
    {
        executeWriteTransaction("drop view " + schemaName + "." + viewName, tx -> {
            long schemaId = tx.resolveSchemaId(schemaName);

            // Resolve view_id for the change string (spec requires ID, not name)
            long viewId;
            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "SELECT view_id FROM ducklake_view " +
                            "WHERE schema_id = ? AND view_name = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)")) {
                stmt.setLong(1, schemaId);
                stmt.setString(2, viewName);
                stmt.setLong(3, tx.getCurrentSnapshotId());
                stmt.setLong(4, tx.getCurrentSnapshotId());
                try (ResultSet rs = stmt.executeQuery()) {
                    if (!rs.next()) {
                        throw new RuntimeException("View not found: " + schemaName + "." + viewName);
                    }
                    viewId = rs.getLong("view_id");
                }
            }

            tx.addChange("dropped_view:" + viewId);

            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "UPDATE ducklake_view SET end_snapshot = ? WHERE view_id = ? AND end_snapshot IS NULL")) {
                stmt.setLong(1, tx.getNewSnapshotId());
                stmt.setLong(2, viewId);
                stmt.executeUpdate();
            }
        });
    }

    // ==================== Schema DDL ====================

    @Override
    public void createSchema(String schemaName)
    {
        executeWriteTransaction("create schema " + schemaName, tx -> {
            long schemaId = tx.allocateCatalogId();
            tx.addChange("created_schema:" + schemaName);

            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "INSERT INTO ducklake_schema (schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative) " +
                            "VALUES (?, ?, ?, NULL, ?, ?, true)")) {
                stmt.setLong(1, schemaId);
                stmt.setString(2, UUID.randomUUID().toString());
                stmt.setLong(3, tx.getNewSnapshotId());
                stmt.setString(4, schemaName);
                stmt.setString(5, schemaName + "/");
                stmt.executeUpdate();
            }
        });
    }

    @Override
    public void dropSchema(String schemaName)
    {
        executeWriteTransaction("drop schema " + schemaName, tx -> {
            long schemaId = tx.resolveSchemaId(schemaName);

            if (tx.hasTablesInSchema(schemaId)) {
                throw new RuntimeException("Cannot drop schema " + schemaName + ": schema is not empty");
            }

            tx.addChange("dropped_schema:" + schemaId);

            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "UPDATE ducklake_schema SET end_snapshot = ? WHERE schema_id = ? AND end_snapshot IS NULL")) {
                stmt.setLong(1, tx.getNewSnapshotId());
                stmt.setLong(2, schemaId);
                stmt.executeUpdate();
            }
        });
    }

    // ==================== Table DDL ====================

    @Override
    public void createTable(String schemaName, String tableName,
            List<TableColumnSpec> columns,
            Optional<List<PartitionFieldSpec>> partitionSpec)
    {
        executeWriteTransaction("create table " + schemaName + "." + tableName, tx -> {
            long schemaId = tx.resolveSchemaId(schemaName);
            long tableId = tx.allocateCatalogId();

            // 1. Insert table row
            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "INSERT INTO ducklake_table (table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative) " +
                            "VALUES (?, ?, ?, NULL, ?, ?, ?, true)")) {
                stmt.setLong(1, tableId);
                stmt.setString(2, UUID.randomUUID().toString());
                stmt.setLong(3, tx.getNewSnapshotId());
                stmt.setLong(4, schemaId);
                stmt.setString(5, tableName);
                stmt.setString(6, tableName + "/");
                stmt.executeUpdate();
            }

            // 2. Insert column rows (flattening nested types with parent links)
            Map<String, Long> topLevelColumnIds = new LinkedHashMap<>();
            long columnOrder = 0;
            for (TableColumnSpec column : columns) {
                long columnId = insertColumnTree(tx, tableId, column, columnOrder++, OptionalLong.empty());
                topLevelColumnIds.put(column.name(), columnId);
            }

            // 3. Insert table stats (empty table)
            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "INSERT INTO ducklake_table_stats (table_id, record_count, next_row_id, file_size_bytes) " +
                            "VALUES (?, 0, 0, 0)")) {
                stmt.setLong(1, tableId);
                stmt.executeUpdate();
            }

            // 4. Insert partition spec if provided
            if (partitionSpec.isPresent() && !partitionSpec.get().isEmpty()) {
                long partitionId = tx.allocateCatalogId();

                try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                        "INSERT INTO ducklake_partition_info (partition_id, table_id, begin_snapshot, end_snapshot) " +
                                "VALUES (?, ?, ?, NULL)")) {
                    stmt.setLong(1, partitionId);
                    stmt.setLong(2, tableId);
                    stmt.setLong(3, tx.getNewSnapshotId());
                    stmt.executeUpdate();
                }

                int keyIndex = 0;
                for (PartitionFieldSpec field : partitionSpec.get()) {
                    Long columnId = topLevelColumnIds.get(field.columnName());
                    if (columnId == null) {
                        throw new RuntimeException("Partition column not found: " + field.columnName());
                    }
                    try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                            "INSERT INTO ducklake_partition_column (partition_id, table_id, partition_key_index, column_id, transform) " +
                                    "VALUES (?, ?, ?, ?, ?)")) {
                        stmt.setLong(1, partitionId);
                        stmt.setLong(2, tableId);
                        stmt.setLong(3, keyIndex++);
                        stmt.setLong(4, columnId);
                        stmt.setString(5, field.transform().name().toLowerCase(java.util.Locale.ENGLISH));
                        stmt.executeUpdate();
                    }
                }
            }

            tx.incrementSchemaVersion();
            tx.addChange("created_table:" + tableName);
        });
    }

    /**
     * Recursively inserts a column and its children into ducklake_column.
     * Returns the column_id of the inserted column.
     */
    private long insertColumnTree(DucklakeWriteTransaction tx, long tableId,
            TableColumnSpec column, long columnOrder, OptionalLong parentColumnId)
            throws SQLException
    {
        long columnId = tx.allocateCatalogId();

        try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                "INSERT INTO ducklake_column (column_id, begin_snapshot, end_snapshot, table_id, column_order, " +
                        "column_name, column_type, initial_default, default_value, nulls_allowed, parent_column) " +
                        "VALUES (?, ?, NULL, ?, ?, ?, ?, NULL, NULL, ?, ?)")) {
            stmt.setLong(1, columnId);
            stmt.setLong(2, tx.getNewSnapshotId());
            stmt.setLong(3, tableId);
            stmt.setLong(4, columnOrder);
            stmt.setString(5, column.name());
            stmt.setString(6, column.ducklakeType());
            stmt.setBoolean(7, column.nullable());
            if (parentColumnId.isPresent()) {
                stmt.setLong(8, parentColumnId.getAsLong());
            }
            else {
                stmt.setNull(8, java.sql.Types.BIGINT);
            }
            stmt.executeUpdate();
        }

        // Insert children with their own column_order (0-based within parent)
        long childOrder = 0;
        for (TableColumnSpec child : column.children()) {
            insertColumnTree(tx, tableId, child, childOrder++, OptionalLong.of(columnId));
        }

        return columnId;
    }

    @Override
    public void dropTable(String schemaName, String tableName)
    {
        executeWriteTransaction("drop table " + schemaName + "." + tableName, tx -> {
            long schemaId = tx.resolveSchemaId(schemaName);
            long tableId = tx.resolveTableId(schemaId, tableName);

            // End-snapshot the table row
            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "UPDATE ducklake_table SET end_snapshot = ? WHERE table_id = ? AND end_snapshot IS NULL")) {
                stmt.setLong(1, tx.getNewSnapshotId());
                stmt.setLong(2, tableId);
                stmt.executeUpdate();
            }

            // End-snapshot all active columns
            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "UPDATE ducklake_column SET end_snapshot = ? WHERE table_id = ? AND end_snapshot IS NULL")) {
                stmt.setLong(1, tx.getNewSnapshotId());
                stmt.setLong(2, tableId);
                stmt.executeUpdate();
            }

            // End-snapshot all active data files
            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "UPDATE ducklake_data_file SET end_snapshot = ? WHERE table_id = ? AND end_snapshot IS NULL")) {
                stmt.setLong(1, tx.getNewSnapshotId());
                stmt.setLong(2, tableId);
                stmt.executeUpdate();
            }

            // End-snapshot all active delete files (via data_file join)
            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "UPDATE ducklake_delete_file SET end_snapshot = ? " +
                            "WHERE data_file_id IN (SELECT data_file_id FROM ducklake_data_file WHERE table_id = ?) " +
                            "AND end_snapshot IS NULL")) {
                stmt.setLong(1, tx.getNewSnapshotId());
                stmt.setLong(2, tableId);
                stmt.executeUpdate();
            }

            // End-snapshot partition info
            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "UPDATE ducklake_partition_info SET end_snapshot = ? WHERE table_id = ? AND end_snapshot IS NULL")) {
                stmt.setLong(1, tx.getNewSnapshotId());
                stmt.setLong(2, tableId);
                stmt.executeUpdate();
            }

            tx.incrementSchemaVersion();
            tx.addChange("dropped_table:" + tableId);
        });
    }

    private DucklakeView readView(ResultSet rs)
            throws SQLException
    {
        long endSnapshotRaw = rs.getLong("end_snapshot");
        OptionalLong endSnapshot = rs.wasNull() ? OptionalLong.empty() : OptionalLong.of(endSnapshotRaw);

        return new DucklakeView(
                rs.getLong("view_id"),
                rs.getString("view_uuid"),
                rs.getLong("schema_id"),
                rs.getString("view_name"),
                rs.getString("sql"),
                rs.getString("dialect"),
                getStringOptional(rs, "column_aliases"),
                rs.getLong("begin_snapshot"),
                endSnapshot);
    }

    @Override
    public void close()
    {
        if (hikariDataSource != null) {
            hikariDataSource.close();
        }
    }

    // Helper methods for handling nullable columns

    private Optional<Long> getLongOptional(ResultSet rs, String columnName)
            throws SQLException
    {
        long value = rs.getLong(columnName);
        return rs.wasNull() ? Optional.empty() : Optional.of(value);
    }

    private Optional<String> getStringOptional(ResultSet rs, String columnName)
            throws SQLException
    {
        String value = rs.getString(columnName);
        return Optional.ofNullable(value);
    }

    private Optional<Boolean> getBooleanOptional(ResultSet rs, String columnName)
            throws SQLException
    {
        boolean value = rs.getBoolean(columnName);
        return rs.wasNull() ? Optional.empty() : Optional.of(value);
    }

    private DucklakeSnapshot readSnapshot(ResultSet rs)
            throws SQLException
    {
        return new DucklakeSnapshot(
                rs.getLong("snapshot_id"),
                parseSnapshotTime(rs.getString("snapshot_time")),
                rs.getLong("schema_version"),
                rs.getLong("next_catalog_id"),
                rs.getLong("next_file_id"));
    }

    private static Instant parseSnapshotTime(String snapshotTime)
    {
        if (snapshotTime == null) {
            throw new IllegalStateException("DuckLake snapshot_time is null");
        }

        String normalized = snapshotTime.trim().replace(' ', 'T');
        if (normalized.matches(".*[+-][0-9]{2}$")) {
            normalized = normalized + ":00";
        }
        if (normalized.matches(".*[+-][0-9]{4}$")) {
            normalized = normalized.substring(0, normalized.length() - 5)
                    + normalized.substring(normalized.length() - 5, normalized.length() - 2)
                    + ":"
                    + normalized.substring(normalized.length() - 2);
        }

        // SQLite CURRENT_TIMESTAMP produces no timezone offset (e.g. "2026-04-03T17:45:02");
        // DuckDB-generated snapshots include timezone. Handle both.
        if (!normalized.contains("+") && !normalized.contains("Z") && !normalized.matches(".*-[0-9]{2}:[0-9]{2}$")) {
            return java.time.LocalDateTime.parse(normalized).toInstant(java.time.ZoneOffset.UTC);
        }
        return java.time.OffsetDateTime.parse(normalized).toInstant();
    }

    private String resolveColumnType(DucklakeColumn column, Map<Long, List<DucklakeColumn>> childrenByParent)
    {
        String columnType = column.columnType();
        switch (columnType.toLowerCase()) {
            case "list": {
                List<DucklakeColumn> children = childrenByParent.getOrDefault(column.columnId(), List.of());
                if (children.size() != 1) {
                    throw new IllegalStateException("List column must have exactly one child column: " + column.columnName());
                }
                return "list<" + resolveColumnType(children.get(0), childrenByParent) + ">";
            }
            case "struct": {
                List<DucklakeColumn> children = childrenByParent.getOrDefault(column.columnId(), List.of());
                String fields = children.stream()
                        .map(child -> child.columnName() + ":" + resolveColumnType(child, childrenByParent))
                        .collect(Collectors.joining(","));
                return "struct<" + fields + ">";
            }
            case "map": {
                List<DucklakeColumn> children = childrenByParent.getOrDefault(column.columnId(), List.of());
                if (children.size() != 2) {
                    throw new IllegalStateException("Map column must have exactly two child columns: " + column.columnName());
                }
                return "map<" + resolveColumnType(children.get(0), childrenByParent) + "," + resolveColumnType(children.get(1), childrenByParent) + ">";
            }
            default:
                return columnType;
        }
    }

    private Optional<String> getColumnType(long tableId, long columnId, long snapshotId)
    {
        String sql = "SELECT column_type " +
                     "FROM ducklake_column " +
                     "WHERE table_id = ? AND column_id = ? " +
                     "  AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, columnId);
            stmt.setLong(3, snapshotId);
            stmt.setLong(4, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(rs.getString("column_type"));
                }
                return Optional.empty();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get column type for table: " + tableId + ", column: " + columnId, e);
        }
    }

    private Comparable<?> normalizePredicateValue(String columnType, Object value)
    {
        if (value == null) {
            return null;
        }
        return parseStatValue(columnType, value.toString());
    }

    private Comparable<?> parseStatValue(String columnType, String value)
    {
        if (value == null) {
            return null;
        }

        String normalizedType = columnType.toLowerCase();
        try {
            if (isNumericType(normalizedType)) {
                return new java.math.BigDecimal(value);
            }
            if (normalizedType.equals("boolean")) {
                return parseBoolean(value);
            }
            return value;
        }
        catch (RuntimeException e) {
            // If parsing fails we avoid false negatives by not pruning on this value.
            return null;
        }
    }

    private boolean isNumericType(String type)
    {
        return type.equals("int8")
                || type.equals("int16")
                || type.equals("int32")
                || type.equals("int64")
                || type.equals("uint8")
                || type.equals("uint16")
                || type.equals("uint32")
                || type.equals("uint64")
                || type.equals("float32")
                || type.equals("float64")
                || type.startsWith("decimal(");
    }

    private Boolean parseBoolean(String value)
    {
        if (value.equalsIgnoreCase("true") || value.equals("1")) {
            return true;
        }
        if (value.equalsIgnoreCase("false") || value.equals("0")) {
            return false;
        }
        throw new IllegalArgumentException("Invalid boolean value: " + value);
    }

    private boolean isWithinBounds(
            Comparable<?> lowerBound,
            Comparable<?> upperBound,
            Comparable<?> minStat,
            Comparable<?> maxStat)
    {
        OptionalInt lowerVsMax = compareValues(lowerBound, maxStat);
        if (lowerVsMax.isPresent() && lowerVsMax.getAsInt() > 0) {
            return false;
        }

        OptionalInt upperVsMin = compareValues(upperBound, minStat);
        return upperVsMin.isEmpty() || upperVsMin.getAsInt() >= 0;
    }

    @SuppressWarnings("unchecked")
    private OptionalInt compareValues(Comparable<?> left, Comparable<?> right)
    {
        if (left == null || right == null) {
            return OptionalInt.empty();
        }
        try {
            return OptionalInt.of(((Comparable<Object>) left).compareTo(right));
        }
        catch (RuntimeException e) {
            // Type mismatch or non-comparable values: avoid pruning to prevent false negatives.
            return OptionalInt.empty();
        }
    }
}
