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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.ducklake.catalog.DucklakeCatalog;
import io.trino.plugin.ducklake.catalog.DucklakeColumn;
import io.trino.plugin.ducklake.catalog.DucklakeColumnStats;
import io.trino.plugin.ducklake.catalog.DucklakeInlinedDataInfo;
import io.trino.plugin.ducklake.catalog.DucklakePartitionField;
import io.trino.plugin.ducklake.catalog.DucklakePartitionSpec;
import io.trino.plugin.ducklake.catalog.DucklakeSchema;
import io.trino.plugin.ducklake.catalog.DucklakeTable;
import io.trino.plugin.ducklake.catalog.DucklakeTableStats;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.util.Objects.requireNonNull;

/**
 * Metadata implementation for Ducklake connector.
 * Provides read-only access to Ducklake tables via SQL catalog.
 */
public class DucklakeMetadata
        implements ConnectorMetadata
{
    private final DucklakeCatalog catalog;
    private final DucklakeTypeConverter typeConverter;
    private final DucklakeSnapshotResolver snapshotResolver;

    public DucklakeMetadata(DucklakeCatalog catalog, DucklakeTypeConverter typeConverter)
    {
        this(catalog, typeConverter, new DucklakeSnapshotResolver(catalog, OptionalLong.empty(), Optional.empty()));
    }

    public DucklakeMetadata(DucklakeCatalog catalog, DucklakeTypeConverter typeConverter, DucklakeSnapshotResolver snapshotResolver)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.typeConverter = requireNonNull(typeConverter, "typeConverter is null");
        this.snapshotResolver = requireNonNull(snapshotResolver, "snapshotResolver is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        long snapshotId = snapshotResolver.resolveSnapshotId(session);
        return catalog.listSchemas(snapshotId).stream()
                .map(DucklakeSchema::schemaName)
                .collect(toImmutableList());
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        requireNonNull(tableName, "tableName is null");
        requireNonNull(startVersion, "startVersion is null");
        requireNonNull(endVersion, "endVersion is null");

        if (startVersion.isPresent() && endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "DuckLake does not support version ranges; provide only one table version bound");
        }

        Optional<ConnectorTableVersion> queryVersion = endVersion.isPresent() ? endVersion : startVersion;
        OptionalLong querySnapshotId = OptionalLong.empty();
        Optional<Instant> querySnapshotTimestamp = Optional.empty();
        if (queryVersion.isPresent()) {
            ConnectorTableVersion version = queryVersion.get();
            switch (version.getPointerType()) {
                case TARGET_ID -> querySnapshotId = OptionalLong.of(getSnapshotIdFromVersion(version));
                case TEMPORAL -> querySnapshotTimestamp = Optional.of(getSnapshotTimestampFromVersion(session, version));
            }
        }

        long snapshotId = snapshotResolver.resolveSnapshotId(session, querySnapshotId, querySnapshotTimestamp);

        Optional<DucklakeTable> table = catalog.getTable(tableName, snapshotId);
        if (table.isEmpty()) {
            return null;
        }

        return new DucklakeTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                table.get().tableId(),
                snapshotId);
    }

    private static long getSnapshotIdFromVersion(ConnectorTableVersion version)
    {
        Type versionType = version.getVersionType();
        if (versionType == SMALLINT || versionType == TINYINT || versionType == INTEGER || versionType == BIGINT) {
            return ((Number) version.getVersion()).longValue();
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported type for table version: " + versionType.getDisplayName());
    }

    private static Instant getSnapshotTimestampFromVersion(ConnectorSession session, ConnectorTableVersion version)
    {
        Type versionType = version.getVersionType();
        if (versionType.equals(DATE)) {
            return LocalDate.ofEpochDay((Long) version.getVersion())
                    .atStartOfDay()
                    .atZone(session.getTimeZoneKey().getZoneId())
                    .toInstant();
        }
        if (versionType instanceof TimestampType timestampVersionType) {
            long epochMicrosUtc = timestampVersionType.isShort()
                    ? (long) version.getVersion()
                    : ((LongTimestamp) version.getVersion()).getEpochMicros();
            long epochSecondUtc = floorDiv(epochMicrosUtc, MICROSECONDS_PER_SECOND);
            int nanosOfSecond = (int) floorMod(epochMicrosUtc, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;
            return LocalDateTime.ofEpochSecond(epochSecondUtc, nanosOfSecond, ZoneOffset.UTC)
                    .atZone(session.getTimeZoneKey().getZoneId())
                    .toInstant();
        }
        if (versionType instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            long epochMillis = timestampWithTimeZoneType.isShort()
                    ? unpackMillisUtc((long) version.getVersion())
                    : ((LongTimestampWithTimeZone) version.getVersion()).getEpochMillis();
            return Instant.ofEpochMilli(epochMillis);
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported type for temporal table version: " + versionType.getDisplayName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DucklakeTableHandle ducklakeTableHandle = (DucklakeTableHandle) tableHandle;

        List<DucklakeColumn> columns = catalog.getTableColumns(
                ducklakeTableHandle.tableId(),
                ducklakeTableHandle.snapshotId());

        List<ColumnMetadata> columnMetadata = columns.stream()
                .map(column -> ColumnMetadata.builder()
                        .setName(column.columnName())
                        .setType(typeConverter.toTrinoType(column.columnType()))
                        .setNullable(column.nullsAllowed())
                        .build())
                .collect(toImmutableList());

        return new ConnectorTableMetadata(
                ducklakeTableHandle.getSchemaTableName(),
                columnMetadata);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        long snapshotId = snapshotResolver.resolveSnapshotId(session);

        if (schemaName.isPresent()) {
            Optional<DucklakeSchema> schema = catalog.getSchema(schemaName.get(), snapshotId);
            if (schema.isEmpty()) {
                return ImmutableList.of();
            }

            return catalog.listTables(schema.get().schemaId(), snapshotId).stream()
                    .map(table -> new SchemaTableName(schemaName.get(), table.tableName()))
                    .collect(toImmutableList());
        }

        // List all tables across all schemas
        ImmutableList.Builder<SchemaTableName> tables = ImmutableList.builder();
        for (DucklakeSchema schema : catalog.listSchemas(snapshotId)) {
            for (DucklakeTable table : catalog.listTables(schema.schemaId(), snapshotId)) {
                tables.add(new SchemaTableName(schema.schemaName(), table.tableName()));
            }
        }
        return tables.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DucklakeTableHandle ducklakeTableHandle = (DucklakeTableHandle) tableHandle;

        List<DucklakeColumn> columns = catalog.getTableColumns(
                ducklakeTableHandle.tableId(),
                ducklakeTableHandle.snapshotId());

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (DucklakeColumn column : columns) {
            columnHandles.put(
                    column.columnName(),
                    new DucklakeColumnHandle(
                            column.columnId(),
                            column.columnName(),
                            typeConverter.toTrinoType(column.columnType()),
                            column.nullsAllowed()));
        }
        return columnHandles.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        DucklakeColumnHandle ducklakeColumnHandle = (DucklakeColumnHandle) columnHandle;

        return ColumnMetadata.builder()
                .setName(ducklakeColumnHandle.columnName())
                .setType(ducklakeColumnHandle.columnType())
                .setNullable(ducklakeColumnHandle.nullable())
                .build();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DucklakeTableHandle table = (DucklakeTableHandle) tableHandle;

        Optional<DucklakeTableStats> tableStats = catalog.getTableStats(table.tableId());
        long recordCount;
        if (tableStats.isPresent()) {
            recordCount = tableStats.get().recordCount();
        }
        else {
            OptionalLong fallbackRecordCount = getFallbackRecordCount(table);
            if (fallbackRecordCount.isEmpty()) {
                return TableStatistics.empty();
            }
            recordCount = fallbackRecordCount.getAsLong();
        }

        TableStatistics.Builder stats = TableStatistics.builder()
                .setRowCount(Estimate.of(recordCount));

        if (recordCount == 0) {
            return stats.build();
        }

        Map<String, ColumnHandle> columnHandles = getColumnHandles(session, tableHandle);

        // Build column type map for typed min/max comparison
        Map<Long, String> columnTypes = catalog.getTableColumns(table.tableId(), table.snapshotId()).stream()
                .collect(toImmutableMap(DucklakeColumn::columnId, DucklakeColumn::columnType));
        List<DucklakeColumnStats> columnStatsList = catalog.getColumnStats(table.tableId(), table.snapshotId(), columnTypes);

        // Index column stats by column ID
        Map<Long, DucklakeColumnStats> statsById = columnStatsList.stream()
                .collect(toImmutableMap(DucklakeColumnStats::columnId, s -> s));

        for (ColumnHandle handle : columnHandles.values()) {
            DucklakeColumnHandle column = (DucklakeColumnHandle) handle;
            DucklakeColumnStats colStats = statsById.get(column.columnId());
            if (colStats == null) {
                continue;
            }

            ColumnStatistics.Builder colBuilder = ColumnStatistics.builder();

            long totalCount = colStats.totalValueCount() + colStats.totalNullCount();
            if (totalCount > 0) {
                colBuilder.setNullsFraction(Estimate.of((double) colStats.totalNullCount() / totalCount));
            }

            if (colStats.totalSizeBytes() > 0) {
                colBuilder.setDataSize(Estimate.of(colStats.totalSizeBytes()));
            }

            toDoubleRange(column.columnType(), colStats).ifPresent(colBuilder::setRange);

            stats.setColumnStatistics(column, colBuilder.build());
        }

        return stats.build();
    }

    private OptionalLong getFallbackRecordCount(DucklakeTableHandle table)
    {
        // Align with Iceberg/Delta behavior: if we can prove there is no data at this snapshot,
        // return row count 0 instead of unknown.
        if (!catalog.getDataFiles(table.tableId(), table.snapshotId()).isEmpty()) {
            // Data files exist but no table stats were found. Keep row count unknown.
            return OptionalLong.empty();
        }

        Optional<DucklakeInlinedDataInfo> inlinedInfo = catalog.getInlinedDataInfo(table.tableId(), table.snapshotId());
        if (inlinedInfo.isEmpty()) {
            return OptionalLong.of(0);
        }

        List<DucklakeColumn> tableColumns = catalog.getTableColumns(table.tableId(), table.snapshotId());
        if (tableColumns.isEmpty()) {
            return OptionalLong.of(0);
        }

        long inlinedRowCount = catalog.readInlinedData(
                table.tableId(),
                inlinedInfo.get().schemaVersion(),
                table.snapshotId(),
                ImmutableList.of(tableColumns.getFirst())).size();
        return OptionalLong.of(inlinedRowCount);
    }

    private static Optional<DoubleRange> toDoubleRange(Type type, DucklakeColumnStats stats)
    {
        if (stats.minValue().isEmpty() || stats.maxValue().isEmpty()) {
            return Optional.empty();
        }

        try {
            String minStr = stats.minValue().get();
            String maxStr = stats.maxValue().get();

            if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
                return Optional.of(new DoubleRange(Double.parseDouble(minStr), Double.parseDouble(maxStr)));
            }
            if (type.equals(DOUBLE) || type.equals(REAL)) {
                return Optional.of(new DoubleRange(Double.parseDouble(minStr), Double.parseDouble(maxStr)));
            }
            if (type.equals(DATE)) {
                long minDays = java.time.LocalDate.parse(minStr).toEpochDay();
                long maxDays = java.time.LocalDate.parse(maxStr).toEpochDay();
                return Optional.of(new DoubleRange(minDays, maxDays));
            }
        }
        catch (RuntimeException _) {
            // If parsing fails, skip range
        }
        return Optional.empty();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle handle,
            Constraint constraint)
    {
        DucklakeTableHandle table = (DucklakeTableHandle) handle;

        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        if (summary.isAll()) {
            return Optional.empty();
        }

        TupleDomain<DucklakeColumnHandle> newPredicate = extractDucklakePredicate(summary);

        // Classify predicates as enforced (partition-prunable) or unenforced (best-effort)
        List<DucklakePartitionSpec> partitionSpecs = catalog.getPartitionSpecs(
                table.tableId(), table.snapshotId());

        ImmutableMap.Builder<DucklakeColumnHandle, Domain> enforced = ImmutableMap.builder();
        ImmutableMap.Builder<DucklakeColumnHandle, Domain> unenforced = ImmutableMap.builder();

        if (!newPredicate.isNone()) {
            for (Map.Entry<DucklakeColumnHandle, Domain> entry : newPredicate.getDomains().orElse(Map.of()).entrySet()) {
                switch (classifyColumnConstraint(partitionSpecs, entry.getKey())) {
                    case FULLY_ENFORCED -> enforced.put(entry.getKey(), entry.getValue());
                    case PARTIALLY_ENFORCED -> {
                        // Keep in both predicates: connector can use partition transforms for pruning,
                        // but engine must still evaluate original predicate for correctness.
                        enforced.put(entry.getKey(), entry.getValue());
                        unenforced.put(entry.getKey(), entry.getValue());
                    }
                    case NOT_ENFORCED -> unenforced.put(entry.getKey(), entry.getValue());
                }
            }
        }

        TupleDomain<DucklakeColumnHandle> newEnforced = newPredicate.isNone()
                ? TupleDomain.none()
                : toTupleDomain(enforced.buildOrThrow());
        TupleDomain<DucklakeColumnHandle> newUnenforced = newPredicate.isNone()
                ? TupleDomain.all()
                : toTupleDomain(unenforced.buildOrThrow());

        TupleDomain<DucklakeColumnHandle> combinedEnforced = table.enforcedPredicate().intersect(newEnforced);
        TupleDomain<DucklakeColumnHandle> combinedUnenforced = table.unenforcedPredicate().intersect(newUnenforced);

        if (combinedEnforced.equals(table.enforcedPredicate())
                && combinedUnenforced.equals(table.unenforcedPredicate())) {
            return Optional.empty();
        }

        DucklakeTableHandle newHandle = new DucklakeTableHandle(
                table.schemaName(),
                table.tableName(),
                table.tableId(),
                table.snapshotId(),
                combinedUnenforced,
                combinedEnforced);

        // Fully enforced predicates are omitted from remaining filter.
        // Partially enforced predicates (e.g. temporal transforms) remain so engine verifies exact semantics.
        TupleDomain<ColumnHandle> remainingFilter = newUnenforced.transformKeys(ColumnHandle.class::cast);

        return Optional.of(new ConstraintApplicationResult<>(
                newHandle,
                remainingFilter,
                constraint.getExpression(),
                false));
    }

    private static ConstraintEnforcement classifyColumnConstraint(
            List<DucklakePartitionSpec> specs,
            DucklakeColumnHandle column)
    {
        if (specs.isEmpty()) {
            return ConstraintEnforcement.NOT_ENFORCED;
        }

        boolean fullyEnforced = true;
        // A predicate can only be enforced (fully or partially) if it is enforceable in EVERY active spec
        // (spec evolution means different files may have different partition schemes)
        for (DucklakePartitionSpec spec : specs) {
            Optional<DucklakePartitionField> field = spec.fields().stream()
                    .filter(partitionField -> partitionField.columnId() == column.columnId())
                    .findFirst();
            if (field.isEmpty()) {
                return ConstraintEnforcement.NOT_ENFORCED;
            }

            ConstraintEnforcement fieldEnforcement = classifyTransformEnforcement(field.get(), column);
            if (fieldEnforcement == ConstraintEnforcement.NOT_ENFORCED) {
                return ConstraintEnforcement.NOT_ENFORCED;
            }
            if (fieldEnforcement == ConstraintEnforcement.PARTIALLY_ENFORCED) {
                fullyEnforced = false;
            }
        }
        return fullyEnforced ? ConstraintEnforcement.FULLY_ENFORCED : ConstraintEnforcement.PARTIALLY_ENFORCED;
    }

    private static ConstraintEnforcement classifyTransformEnforcement(DucklakePartitionField field, DucklakeColumnHandle column)
    {
        if (field.transform().isIdentity()) {
            return ConstraintEnforcement.FULLY_ENFORCED;
        }
        if (field.transform().isTemporal()) {
            // Temporal transforms support safe partition pruning but do not fully enforce
            // original predicates (e.g. day equality with month transform).
            Type type = column.columnType();
            if (type.equals(DATE) || type.equals(TIMESTAMP_MILLIS) || type.equals(TIMESTAMP_MICROS)) {
                return ConstraintEnforcement.PARTIALLY_ENFORCED;
            }
        }
        return ConstraintEnforcement.NOT_ENFORCED;
    }

    private enum ConstraintEnforcement
    {
        FULLY_ENFORCED,
        PARTIALLY_ENFORCED,
        NOT_ENFORCED
    }

    private static TupleDomain<DucklakeColumnHandle> extractDucklakePredicate(TupleDomain<ColumnHandle> summary)
    {
        if (summary.isNone()) {
            return TupleDomain.none();
        }

        Optional<Map<ColumnHandle, Domain>> domains = summary.getDomains();
        if (domains.isEmpty()) {
            return TupleDomain.all();
        }

        ImmutableMap.Builder<DucklakeColumnHandle, Domain> ducklakeDomains = ImmutableMap.builder();
        for (Map.Entry<ColumnHandle, Domain> entry : domains.get().entrySet()) {
            if (entry.getKey() instanceof DucklakeColumnHandle columnHandle) {
                // Only push down primitive types (arrays/complex types can't be pruned)
                if (!columnHandle.columnType().getTypeParameters().isEmpty()) {
                    continue;
                }
                ducklakeDomains.put(columnHandle, entry.getValue());
            }
        }

        Map<DucklakeColumnHandle, Domain> result = ducklakeDomains.buildOrThrow();
        if (result.isEmpty()) {
            return TupleDomain.all();
        }
        return TupleDomain.withColumnDomains(result);
    }

    private static TupleDomain<DucklakeColumnHandle> toTupleDomain(Map<DucklakeColumnHandle, Domain> domains)
    {
        if (domains.isEmpty()) {
            return TupleDomain.all();
        }
        return TupleDomain.withColumnDomains(domains);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
            ConnectorSession session,
            SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        long snapshotId = catalog.getCurrentSnapshotId();
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        List<SchemaTableName> tables = prefix.getTable()
                .map(table -> List.of(prefix.toSchemaTableName()))
                .orElseGet(() -> listTables(session, prefix.getSchema()));

        for (SchemaTableName tableName : tables) {
            Optional<DucklakeTable> table = catalog.getTable(tableName, snapshotId);
            if (table.isPresent()) {
                List<DucklakeColumn> tableColumns = catalog.getTableColumns(table.get().tableId(), snapshotId);
                columns.put(
                        tableName,
                        tableColumns.stream()
                                .map(column -> ColumnMetadata.builder()
                                        .setName(column.columnName())
                                        .setType(typeConverter.toTrinoType(column.columnType()))
                                        .setNullable(column.nullsAllowed())
                                        .build())
                                .collect(toImmutableList()));
            }
        }

        return columns.buildOrThrow();
    }
}
