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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;

import java.util.List;

import static java.util.Objects.requireNonNull;

public record DucklakeWritableTableHandle(
        @JsonProperty("schemaName") String schemaName,
        @JsonProperty("tableName") String tableName,
        @JsonProperty("tableId") long tableId,
        @JsonProperty("columns") List<DucklakeColumnHandle> columns,
        @JsonProperty("tableDataPath") String tableDataPath)
        implements ConnectorInsertTableHandle, ConnectorOutputTableHandle
{
    @JsonCreator
    public DucklakeWritableTableHandle
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(columns, "columns is null");
        columns = List.copyOf(columns);
        requireNonNull(tableDataPath, "tableDataPath is null");
    }

    @Override
    public String toString()
    {
        return schemaName + "." + tableName + "#" + tableId;
    }
}
