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

import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.parquet.writer.ParquetSchemaConverter;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.type.Type;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.FileMetaData;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class DucklakePageSink
        implements ConnectorPageSink
{
    private static final Logger log = Logger.get(DucklakePageSink.class);

    private final DucklakeWritableTableHandle handle;
    private final TrinoFileSystem fileSystem;
    private final JsonCodec<DucklakeWriteFragment> fragmentCodec;
    private final ParquetWriterOptions writerOptions;
    private final String trinoVersion;
    private final long targetMaxFileSize;

    private final List<Type> columnTypes;
    private final List<String> columnNames;
    private final ParquetSchemaConverter schemaConverter;

    private final List<DucklakeWriteFragment> completedFragments = new ArrayList<>();
    private final List<Location> writtenFilePaths = new ArrayList<>();

    private ParquetWriter currentWriter;
    private OutputStream currentOutputStream;
    private String currentFileName;
    private long currentWrittenBytes;

    public DucklakePageSink(
            DucklakeWritableTableHandle handle,
            TrinoFileSystem fileSystem,
            JsonCodec<DucklakeWriteFragment> fragmentCodec,
            ParquetWriterConfig parquetWriterConfig,
            String trinoVersion)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.fragmentCodec = requireNonNull(fragmentCodec, "fragmentCodec is null");
        this.trinoVersion = requireNonNull(trinoVersion, "trinoVersion is null");

        this.writerOptions = ParquetWriterOptions.builder()
                .setMaxBlockSize(parquetWriterConfig.getBlockSize())
                .setMaxPageSize(parquetWriterConfig.getPageSize())
                .setMaxPageValueCount(parquetWriterConfig.getPageValueCount())
                .setBatchSize(parquetWriterConfig.getBatchSize())
                .build();

        this.targetMaxFileSize = parquetWriterConfig.getBlockSize().toBytes();

        this.columnTypes = handle.columns().stream()
                .map(DucklakeColumnHandle::columnType)
                .collect(toImmutableList());
        this.columnNames = handle.columns().stream()
                .map(DucklakeColumnHandle::columnName)
                .collect(toImmutableList());

        this.schemaConverter = new ParquetSchemaConverter(
                columnTypes,
                columnNames,
                false,
                false);
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        if (page.getPositionCount() == 0) {
            return NOT_BLOCKED;
        }

        try {
            if (currentWriter == null) {
                openNewWriter();
            }

            currentWriter.write(page);
            currentWrittenBytes = currentWriter.getWrittenBytes() + currentWriter.getBufferedBytes();

            if (currentWrittenBytes >= targetMaxFileSize) {
                closeCurrentWriter();
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to write page", e);
        }

        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        try {
            if (currentWriter != null) {
                closeCurrentWriter();
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to close writer", e);
        }

        List<Slice> fragments = completedFragments.stream()
                .map(fragment -> Slices.wrappedBuffer(fragmentCodec.toJsonBytes(fragment)))
                .collect(toImmutableList());

        return completedFuture(fragments);
    }

    @Override
    public void abort()
    {
        // Close writer if open
        if (currentWriter != null) {
            try {
                currentWriter.close();
            }
            catch (IOException e) {
                log.warn(e, "Failed to close writer during abort");
            }
            currentWriter = null;
        }

        // Best-effort delete all written files
        for (Location path : writtenFilePaths) {
            try {
                fileSystem.deleteFile(path);
            }
            catch (IOException e) {
                log.warn(e, "Failed to delete file during abort: %s", path);
            }
        }
    }

    private void openNewWriter()
            throws IOException
    {
        currentFileName = "ducklake-" + UUID.randomUUID() + ".parquet";
        Location filePath = Location.of(handle.tableDataPath()).appendPath(currentFileName);
        writtenFilePaths.add(filePath);

        TrinoOutputFile outputFile = fileSystem.newOutputFile(filePath);
        currentOutputStream = outputFile.create();

        currentWriter = new ParquetWriter(
                currentOutputStream,
                schemaConverter.getMessageType(),
                schemaConverter.getPrimitiveTypes(),
                writerOptions,
                CompressionCodec.ZSTD,
                trinoVersion,
                Optional.empty(),
                Optional.empty());

        currentWrittenBytes = 0;
    }

    private void closeCurrentWriter()
            throws IOException
    {
        if (currentWriter == null) {
            return;
        }

        currentWriter.close();

        FileMetaData fileMetaData = currentWriter.getFileMetaData();
        long recordCount = fileMetaData.getNum_rows();
        long fileSize = currentWriter.getWrittenBytes();

        List<DucklakeFileColumnStats> columnStats =
                DucklakeStatsExtractor.extractStats(fileMetaData, handle.columns());

        completedFragments.add(new DucklakeWriteFragment(
                currentFileName,
                fileSize,
                recordCount,
                columnStats));

        currentWriter = null;
        currentOutputStream = null;
        currentFileName = null;
        currentWrittenBytes = 0;
    }
}
