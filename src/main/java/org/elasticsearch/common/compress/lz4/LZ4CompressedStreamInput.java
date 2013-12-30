/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.compress.lz4;

import com.ning.compress.BufferRecycler;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.compress.CompressedStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

final class LZ4CompressedStreamInput extends CompressedStreamInput<LZ4CompressorContext>  {

    private final BufferRecycler recycler;
    private final LZ4FastDecompressor decompressor;
    private byte[] compressed;

    public LZ4CompressedStreamInput(StreamInput in, LZ4FastDecompressor decompressor) throws IOException {
        super(in, LZ4CompressorContext.INSTANCE);
        this.recycler = BufferRecycler.instance();
        this.decompressor = decompressor;
        compressed = uncompressed = BytesRef.EMPTY_BYTES;
    }

    @Override
    protected void doClose() throws IOException {
        byte[] buf = compressed;
        if (buf != null) {
            compressed = null;
            recycler.releaseInputBuffer(buf);
        }
        buf = uncompressed;
        if (buf != null) {
            uncompressed = null;
            recycler.releaseDecodeBuffer(buf);
        }
    }

    @Override
    protected void readHeader(StreamInput in) throws IOException {
        for (int i = 0; i < LZ4Compressor.HEADER.length; ++i) {
            if (in.readByte() != LZ4Compressor.HEADER[i]) {
                throw new IOException("Invalid header");
            }
        }
    }

    @Override
    protected int uncompress(StreamInput in) throws IOException {
        final int uncompressedLen = in.readVInt();
        if (uncompressedLen == 0) {
            return -1;
        }
        final int compressedLen = uncompressedLen - in.readVInt();

        if (uncompressedLen > uncompressed.length) {
            uncompressed = recycler.allocDecodeBuffer(Math.max(uncompressedLen, ArrayUtil.oversize(uncompressed.length, RamUsageEstimator.NUM_BYTES_BYTE)));
        }

        if (uncompressedLen == compressedLen) {
            // means no compression
            in.readBytes(uncompressed, 0, uncompressedLen);
            return uncompressedLen;
        }

        if (compressedLen > compressed.length) {
            compressed = recycler.allocInputBuffer(Math.max(compressedLen, ArrayUtil.oversize(compressed.length, RamUsageEstimator.NUM_BYTES_BYTE)));
        }

        in.readBytes(compressed, 0, compressedLen);
        final int compressedLen2 = decompressor.decompress(compressed, 0, uncompressed, 0, uncompressedLen);
        if (compressedLen != compressedLen2) {
            throw new IOException("Stream is corrupted");
        }
        return uncompressedLen;
    }

}
