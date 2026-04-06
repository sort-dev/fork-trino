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
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slices;
import io.trino.plugin.ducklake.catalog.DucklakeCatalog;
import io.trino.plugin.ducklake.catalog.DucklakeColumn;
import io.trino.plugin.ducklake.catalog.DucklakeDataFile;
import io.trino.plugin.ducklake.catalog.DucklakeFilePartitionValue;
import io.trino.plugin.ducklake.catalog.DucklakePartitionField;
import io.trino.plugin.ducklake.catalog.DucklakePartitionSpec;
import io.trino.plugin.ducklake.catalog.DucklakePartitionTransform;
import io.trino.plugin.ducklake.catalog.DucklakeSchema;
import io.trino.plugin.ducklake.catalog.DucklakeTable;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

/**
 * Split manager for Ducklake connector.
 * Discovers data files from SQL catalog and creates splits for each Parquet file.
 */
public class DucklakeSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(DucklakeSplitManager.class);

    private final DucklakeCatalog catalog;
    private final DucklakePathResolver pathResolver;
    private final DucklakeTemporalPartitionEncoding temporalPartitionEncoding;
    private final boolean temporalPartitionEncodingReadLeniency;

    @Inject
    public DucklakeSplitManager(DucklakeCatalog catalog, DucklakeConfig config, DucklakePathResolver pathResolver)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.pathResolver = requireNonNull(pathResolver, "pathResolver is null");
        this.temporalPartitionEncoding = config.getTemporalPartitionEncoding();
        this.temporalPartitionEncodingReadLeniency = config.isTemporalPartitionEncodingReadLeniency();
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        if (table instanceof DucklakeMetadataTableHandle metadataTableHandle) {
            DucklakeMetadataSplit metadataSplit = new DucklakeMetadataSplit(
                    metadataTableHandle.baseTableId(),
                    metadataTableHandle.snapshotId(),
                    metadataTableHandle.metadataTableType());
            return new FixedSplitSource(List.of(metadataSplit));
        }

        DucklakeTableHandle tableHandle = (DucklakeTableHandle) table;

        log.debug("Getting splits for table %s at snapshot %d", tableHandle.tableName(), tableHandle.snapshotId());

        // Get all data files for this table at the snapshot
        List<DucklakeDataFile> dataFiles = catalog.getDataFiles(
                tableHandle.tableId(),
                tableHandle.snapshotId());

        log.debug("Found %d data files for table %s", dataFiles.size(), tableHandle.tableName());

        boolean hasParquetFiles = !dataFiles.isEmpty();
        Optional<DucklakeInlinedSplit> inlinedSplit = catalog.getInlinedDataInfo(
                        tableHandle.tableId(), tableHandle.snapshotId())
                .filter(info -> shouldIncludeInlinedSplit(tableHandle, info.schemaVersion(), hasParquetFiles))
                .map(info -> {
                    log.debug("Found inlined data for table %s (tableId=%d, schemaVersion=%d)",
                            tableHandle.tableName(), info.tableId(), info.schemaVersion());
                    return new DucklakeInlinedSplit(info.tableId(), info.schemaVersion(), tableHandle.snapshotId());
                });

        List<DucklakeSplit> parquetSplits = List.of();
        if (!dataFiles.isEmpty()) {
            DucklakeTable tableMetadata = catalog.getTableById(tableHandle.tableId(), tableHandle.snapshotId())
                    .orElseThrow(() -> new IllegalStateException("Table metadata missing for table ID: " + tableHandle.tableId()));
            DucklakeSchema schemaMetadata = catalog.getSchema(tableHandle.schemaName(), tableHandle.snapshotId())
                    .orElseThrow(() -> new IllegalStateException("Schema metadata missing for schema: " + tableHandle.schemaName()));
            String tableDataPath = pathResolver.resolveTableDataPath(schemaMetadata, tableMetadata);

            TupleDomain<DucklakeColumnHandle> fileStatisticsDomain = buildFileStatisticsDomain(constraint)
                    .intersect(tableHandle.unenforcedPredicate());
            dataFiles = pruneDataFiles(dataFiles, tableHandle, constraint);
            dataFiles = pruneByPartitionValues(dataFiles, tableHandle);

            parquetSplits = dataFiles.stream()
                    .map(dataFile -> createSplit(dataFile, tableDataPath, fileStatisticsDomain))
                    .collect(toImmutableList());
        }

        if (parquetSplits.isEmpty() && inlinedSplit.isEmpty()) {
            log.debug("No data files or inlined data found for table %s", tableHandle.tableName());
            return new FixedSplitSource(List.of());
        }

        List<ConnectorSplit> allSplits = new ArrayList<>(parquetSplits.size() + (inlinedSplit.isPresent() ? 1 : 0));
        allSplits.addAll(parquetSplits);
        inlinedSplit.ifPresent(allSplits::add);

        log.debug("Created %d splits for table %s (%d parquet, %d inlined)",
                allSplits.size(),
                tableHandle.tableName(),
                parquetSplits.size(),
                inlinedSplit.isPresent() ? 1 : 0);

        return new FixedSplitSource(allSplits);
    }

    private boolean shouldIncludeInlinedSplit(DucklakeTableHandle tableHandle, long schemaVersion, boolean hasParquetFiles)
    {
        if (!hasParquetFiles) {
            return true;
        }

        List<DucklakeColumn> tableColumns = catalog.getTableColumns(tableHandle.tableId(), tableHandle.snapshotId());
        if (tableColumns.isEmpty()) {
            return false;
        }

        List<List<Object>> probeRows = catalog.readInlinedData(
                tableHandle.tableId(),
                schemaVersion,
                tableHandle.snapshotId(),
                List.of(tableColumns.getFirst()));
        return !probeRows.isEmpty();
    }

    private List<DucklakeDataFile> pruneDataFiles(List<DucklakeDataFile> dataFiles, DucklakeTableHandle tableHandle, Constraint constraint)
    {
        if (dataFiles.isEmpty()) {
            return dataFiles;
        }

        if (constraint == null || constraint.getSummary().isAll()) {
            return dataFiles;
        }

        if (constraint.getSummary().isNone()) {
            return List.of();
        }

        Optional<Map<ColumnHandle, Domain>> domains = constraint.getSummary().getDomains();
        if (domains.isEmpty() || domains.get().isEmpty()) {
            return dataFiles;
        }

        Set<Long> candidateFileIds = dataFiles.stream()
                .map(DucklakeDataFile::dataFileId)
                .collect(toCollection(LinkedHashSet::new));
        boolean pruningApplied = false;

        for (Map.Entry<ColumnHandle, Domain> entry : domains.get().entrySet()) {
            if (!(entry.getKey() instanceof DucklakeColumnHandle columnHandle)) {
                continue;
            }

            Domain domain = entry.getValue();
            if (domain.isNone()) {
                return List.of();
            }

            Optional<PredicateBounds> predicateBounds = extractPredicateBounds(domain);
            if (predicateBounds.isEmpty()) {
                continue;
            }

            PredicateBounds bounds = predicateBounds.get();
            List<Long> matchingFileIds = catalog.getDataFileIdsForPredicate(
                    tableHandle.tableId(),
                    columnHandle.columnId(),
                    tableHandle.snapshotId(),
                    bounds.minValue(),
                    bounds.maxValue());

            pruningApplied = true;
            candidateFileIds.retainAll(matchingFileIds);

            if (candidateFileIds.isEmpty()) {
                log.debug("Pruned all data files for table %s using column %s", tableHandle.tableName(), columnHandle.columnName());
                return List.of();
            }
        }

        if (!pruningApplied) {
            return dataFiles;
        }

        List<DucklakeDataFile> prunedDataFiles = dataFiles.stream()
                .filter(file -> candidateFileIds.contains(file.dataFileId()))
                .collect(toImmutableList());

        log.debug("Pruned data files from %d to %d for table %s", dataFiles.size(), prunedDataFiles.size(), tableHandle.tableName());
        return prunedDataFiles;
    }

    private TupleDomain<DucklakeColumnHandle> buildFileStatisticsDomain(Constraint constraint)
    {
        if (constraint == null) {
            return TupleDomain.all();
        }

        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        if (summary.isAll()) {
            return TupleDomain.all();
        }
        if (summary.isNone()) {
            return TupleDomain.none();
        }

        Optional<Map<ColumnHandle, Domain>> domains = summary.getDomains();
        if (domains.isEmpty() || domains.get().isEmpty()) {
            return TupleDomain.all();
        }

        ImmutableMap.Builder<DucklakeColumnHandle, Domain> ducklakeDomains = ImmutableMap.builder();
        for (Map.Entry<ColumnHandle, Domain> entry : domains.get().entrySet()) {
            if (entry.getKey() instanceof DucklakeColumnHandle columnHandle) {
                ducklakeDomains.put(columnHandle, entry.getValue());
            }
        }

        Map<DucklakeColumnHandle, Domain> result = ducklakeDomains.buildOrThrow();
        if (result.isEmpty()) {
            return TupleDomain.all();
        }
        return TupleDomain.withColumnDomains(result);
    }

    private Optional<PredicateBounds> extractPredicateBounds(Domain domain)
    {
        if (domain.isOnlyNull() || domain.getValues().isAll()) {
            return Optional.empty();
        }

        return domain.getValues().getValuesProcessor().transform(
                ranges -> {
                    if (ranges.getRangeCount() == 0) {
                        return Optional.empty();
                    }

                    Range span = ranges.getSpan();
                    Object minValue = span.getLowValue()
                            .map(value -> normalizePredicateValue(domain.getType(), value))
                            .orElse(null);
                    Object maxValue = span.getHighValue()
                            .map(value -> normalizePredicateValue(domain.getType(), value))
                            .orElse(null);

                    if (minValue == null && maxValue == null) {
                        return Optional.empty();
                    }
                    return Optional.of(new PredicateBounds(minValue, maxValue));
                },
                discreteValues -> extractDiscreteValueBounds(domain.getType(), discreteValues),
                allOrNone -> Optional.empty());
    }

    private Optional<PredicateBounds> extractDiscreteValueBounds(Type type, io.trino.spi.predicate.DiscreteValues discreteValues)
    {
        if (discreteValues.getValuesCount() == 0) {
            return Optional.empty();
        }

        Object minValue = null;
        Object maxValue = null;
        for (Object value : discreteValues.getValues()) {
            Object normalized = normalizePredicateValue(type, value);
            if (minValue == null || compareNormalized(normalized, minValue) < 0) {
                minValue = normalized;
            }
            if (maxValue == null || compareNormalized(normalized, maxValue) > 0) {
                maxValue = normalized;
            }
        }
        return Optional.of(new PredicateBounds(minValue, maxValue));
    }

    @SuppressWarnings("unchecked")
    private static int compareNormalized(Object left, Object right)
    {
        return ((Comparable<Object>) left).compareTo(right);
    }

    private Object normalizePredicateValue(Type type, Object value)
    {
        if (value instanceof io.airlift.slice.Slice slice) {
            return slice.toStringUtf8();
        }
        if (type.equals(DATE) && value instanceof Long daysSinceEpoch) {
            return LocalDate.ofEpochDay(daysSinceEpoch).toString();
        }
        return value;
    }

    private List<DucklakeDataFile> pruneByPartitionValues(
            List<DucklakeDataFile> dataFiles,
            DucklakeTableHandle tableHandle)
    {
        TupleDomain<DucklakeColumnHandle> enforced = tableHandle.enforcedPredicate();
        if (enforced.isAll()) {
            return dataFiles;
        }
        if (enforced.isNone()) {
            return List.of();
        }
        if (dataFiles.isEmpty()) {
            return dataFiles;
        }

        List<DucklakePartitionSpec> specs = catalog.getPartitionSpecs(
                tableHandle.tableId(), tableHandle.snapshotId());
        if (specs.isEmpty()) {
            return dataFiles;
        }

        Map<Long, List<DucklakeFilePartitionValue>> filePartValues =
                catalog.getFilePartitionValues(tableHandle.tableId(), tableHandle.snapshotId());

        // Build columnId -> list of (partitionKeyIndex, transform) for all fields
        // A single column can have multiple transforms (e.g., year + month on the same date column)
        Map<Long, List<PartitionKeyMapping>> columnToPartKeys = new HashMap<>();
        for (DucklakePartitionSpec spec : specs) {
            for (DucklakePartitionField field : spec.fields()) {
                columnToPartKeys.computeIfAbsent(field.columnId(), _ -> new ArrayList<>())
                        .add(new PartitionKeyMapping(field.partitionKeyIndex(), field.transform()));
            }
        }

        Set<Long> candidateFileIds = dataFiles.stream()
                .map(DucklakeDataFile::dataFileId)
                .collect(toCollection(LinkedHashSet::new));

        for (Map.Entry<DucklakeColumnHandle, Domain> entry : enforced.getDomains().orElse(Map.of()).entrySet()) {
            DucklakeColumnHandle column = entry.getKey();
            Domain domain = entry.getValue();
            List<PartitionKeyMapping> mappings = columnToPartKeys.get(column.columnId());
            if (mappings == null) {
                continue;
            }

            candidateFileIds.removeIf(fileId -> {
                List<DucklakeFilePartitionValue> values = filePartValues.getOrDefault(fileId, List.of());
                // A file is pruned if ANY partition transform definitively excludes it
                for (PartitionKeyMapping mapping : mappings) {
                    Optional<String> partValue = values.stream()
                            .filter(v -> v.partitionKeyIndex() == mapping.keyIndex())
                            .map(DucklakeFilePartitionValue::partitionValue)
                            .findFirst();
                    if (partValue.isEmpty()) {
                        continue;
                    }
                    if (!partitionValueMatchesDomain(column.columnType(), partValue.get(), domain, mapping.transform())) {
                        return true; // this transform excludes the file
                    }
                }
                return false; // no transform excluded the file
            });

            if (candidateFileIds.isEmpty()) {
                log.debug("Pruned all data files by partition values for table %s", tableHandle.tableName());
                return List.of();
            }
        }

        List<DucklakeDataFile> result = dataFiles.stream()
                .filter(f -> candidateFileIds.contains(f.dataFileId()))
                .collect(toImmutableList());
        log.debug("Partition pruning: %d -> %d files for table %s", dataFiles.size(), result.size(), tableHandle.tableName());
        return result;
    }

    private boolean partitionValueMatchesDomain(Type columnType, String partitionValue, Domain domain, DucklakePartitionTransform transform)
    {
        try {
            if (transform.isIdentity()) {
                Object nativeValue = parsePartitionValue(columnType, partitionValue);
                return domain.includesNullableValue(nativeValue);
            }
            if (transform.isTemporal()) {
                return DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                        columnType,
                        partitionValue,
                        domain,
                        transform,
                        temporalPartitionEncoding,
                        temporalPartitionEncodingReadLeniency);
            }
            return true; // unknown transform — don't prune
        }
        catch (RuntimeException _) {
            return true; // parse failure — don't prune to avoid false negatives
        }
    }

    private static Object parsePartitionValue(Type type, String value)
    {
        if (type.equals(VARCHAR) || type instanceof io.trino.spi.type.VarcharType) {
            return Slices.utf8Slice(value);
        }
        if (type.equals(BIGINT)) {
            return Long.parseLong(value);
        }
        if (type.equals(INTEGER)) {
            return (long) Integer.parseInt(value);
        }
        if (type.equals(SMALLINT)) {
            return (long) Short.parseShort(value);
        }
        if (type.equals(TINYINT)) {
            return (long) Byte.parseByte(value);
        }
        if (type.equals(DOUBLE)) {
            return Double.parseDouble(value);
        }
        if (type.equals(REAL)) {
            return (long) Float.floatToIntBits(Float.parseFloat(value));
        }
        if (type.equals(DATE)) {
            return LocalDate.parse(value).toEpochDay();
        }
        if (type.equals(BOOLEAN)) {
            return Boolean.parseBoolean(value);
        }
        throw new IllegalArgumentException("Unsupported partition value type: " + type);
    }

    private record PartitionKeyMapping(int keyIndex, DucklakePartitionTransform transform) {}

    private DucklakeSplit createSplit(DucklakeDataFile dataFile, String tableDataPath, TupleDomain<DucklakeColumnHandle> fileStatisticsDomain)
    {
        // Resolve the full path for the data file
        String dataFilePath = pathResolver.resolveFilePath(dataFile.path(), dataFile.pathIsRelative(), tableDataPath);

        // Resolve delete file path if present
        Optional<String> deleteFilePath = dataFile.deleteFilePath()
                .map(path -> pathResolver.resolveFilePath(path, dataFile.deleteFilePathIsRelative().orElse(false), tableDataPath));

        return new DucklakeSplit(
                dataFilePath,
                deleteFilePath,
                dataFile.rowIdStart(),
                dataFile.recordCount(),
                dataFile.fileSizeBytes(),
                dataFile.fileFormat(),
                fileStatisticsDomain);
    }

    private record PredicateBounds(Object minValue, Object maxValue) {}
}
