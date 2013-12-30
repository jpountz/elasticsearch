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
import org.elasticsearch.common.compress.CompressedStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

final class LZ4CompressedStreamOutput extends CompressedStreamOutput<LZ4CompressorContext> {

    private final net.jpountz.lz4.LZ4Compressor compressor;
    private final BufferRecycler recycler;
    private final float minCompressionRatio;
    private byte[] compressed;

    public LZ4CompressedStreamOutput(StreamOutput out, net.jpountz.lz4.LZ4Compressor compressor, int chunkSize, float minCompressionRatio) throws IOException {
        super(out, LZ4CompressorContext.INSTANCE);
        this.compressor = compressor;
        this.recycler = BufferRecycler.instance();
        uncompressed = BufferRecycler.instance().allocOutputBuffer(chunkSize);
        uncompressedLength = chunkSize;
        compressed = BufferRecycler.instance().allocEncodingBuffer(compressor.maxCompressedLength(chunkSize));
        this.minCompressionRatio = minCompressionRatio;
    }

    @Override
    protected void doClose() throws IOException {
        out.writeVInt(0); // end marker
        byte[] buf = uncompressed;
        if (buf != null) {
            uncompressed = null;
            recycler.releaseOutputBuffer(buf);
        }
        buf = compressed;
        if (buf != null) {
            compressed = null;
            recycler.releaseEncodeBuffer(buf);
        }
    }

    @Override
    protected void writeHeader(StreamOutput out) throws IOException {
        out.writeBytes(LZ4Compressor.HEADER);
    }

    @Override
    protected void compress(byte[] data, int offset, int len, StreamOutput out) throws IOException {
        out.writeVInt(len);
        final int compressedLen = compressor.compress(data, offset, len, compressed, 0);
        assert minCompressionRatio <= 1;
        if (compressedLen >= len * minCompressionRatio) {
            out.writeVInt(0);
            out.writeBytes(data, offset, len);
        } else {
            out.writeVInt(len - compressedLen);
            out.writeBytes(compressed, compressedLen);
        }
    }

}
