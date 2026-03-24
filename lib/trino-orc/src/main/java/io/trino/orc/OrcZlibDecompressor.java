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
package io.trino.orc;

import io.airlift.compress.v3.MalformedInputException;
import io.airlift.compress.v3.deflate.DeflateDecompressor;

import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

class OrcZlibDecompressor
        implements OrcDecompressor
{
    private final DeflateDecompressor decompressor = DeflateDecompressor.create();
    private final OrcDataSourceId orcDataSourceId;
    private final int maxBufferSize;

    public OrcZlibDecompressor(OrcDataSourceId orcDataSourceId, int maxBufferSize)
    {
        this.orcDataSourceId = requireNonNull(orcDataSourceId, "orcDataSourceId is null");
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public int decompress(byte[] input, int offset, int length, OutputBuffer output)
            throws OrcCorruptionException
    {
        int bufferSize = min(length, maxBufferSize);
        byte[] buffer = output.initialize(bufferSize);
        while (bufferSize <= maxBufferSize) {
            try {
                return decompressor.decompress(input, offset, length, buffer, 0, buffer.length);
            }
            catch (MalformedInputException e) {
                if (bufferSize == maxBufferSize) {
                    throw new OrcCorruptionException(e, orcDataSourceId, "Invalid compressed stream");
                }
                bufferSize = min(bufferSize * 2, maxBufferSize);
                buffer = output.initialize(bufferSize);
            }
        }
        throw new OrcCorruptionException(orcDataSourceId, "Uncompressed size exceeds max buffer size");
    }

    @Override
    public String toString()
    {
        return "zlib";
    }
}
