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

import com.google.common.collect.ImmutableMap;
import com.ning.compress.BufferRecycler;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.AbstractCompressor;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.ByteUtils;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

public class LZ4Compressor extends AbstractCompressor {

    public static final String IMPL_KEY = "compress.lz4.impl";
    public static final Collection<String> IMPL_VALUES = Impl.IMPLS.keySet();
    public static final String CHUNK_SIZE_KEY = "compress.lz4.chunk_size";
    public static final String MIN_COMPRESSION_RATIO_KEY = "compress.lz4.min_compression_ratio";
    private static final float MIN_COMPRESSION_RATIO_DEFAULT_VALUE = 0.9f;
    private static final int VINT_BYTES_0 = ByteUtils.vIntBytes(0);

    private static enum Impl {
        NATIVE {
            @Override
            LZ4Factory factory() {
                return LZ4Factory.nativeInstance();
            }
        },
        UNSAFE {
            @Override
            LZ4Factory factory() {
                return LZ4Factory.unsafeInstance();
            }
        },
        SAFE {
            @Override
            LZ4Factory factory() {
                return LZ4Factory.safeInstance();
            }
        },
        DEFAULT {
            @Override
            LZ4Factory factory() {
                return LZ4Factory.fastestInstance();
            }
        };

        private static ImmutableMap<String, Impl> IMPLS = ImmutableMap.of("native", NATIVE, "unsafe", UNSAFE, "safe", SAFE);

        public static Impl of(String implString) {
            if (implString == null) {
                return DEFAULT;
            }
            Impl impl = IMPLS.get(implString);
            if (impl == null) {
                Loggers.getLogger(LZ4Compressor.class).warn("unknown lz4 impl: [{}], using default", implString);
                impl = DEFAULT;
            }
            return impl;
        }

        abstract LZ4Factory factory();
    }

    private static final String TYPE = "lz4";
    static final byte[] HEADER = {'l', 'z', '4'};

    private net.jpountz.lz4.LZ4Compressor compressor;
    private LZ4FastDecompressor decompressor;
    private int chunkSize;
    private float minCompressionRatio;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public void configure(Settings settings) {
        final String implString = settings.get(IMPL_KEY, null);
        final LZ4Factory factory = Impl.of(implString).factory();
        compressor = factory.fastCompressor();
        decompressor = factory.fastDecompressor();
        chunkSize = settings.getAsBytesSize(CHUNK_SIZE_KEY, new ByteSizeValue(1 << 12)).bytesAsInt();
        final float minCompressionRatio = settings.getAsFloat(MIN_COMPRESSION_RATIO_KEY, MIN_COMPRESSION_RATIO_DEFAULT_VALUE);
        if (minCompressionRatio > 1) {
            Loggers.getLogger(LZ4Compressor.class).warn(MIN_COMPRESSION_RATIO_KEY + " must be <= 1, using default", implString);
            this.minCompressionRatio = MIN_COMPRESSION_RATIO_DEFAULT_VALUE;
        } else {
            this.minCompressionRatio = minCompressionRatio;
        }
    }

    @Override
    public boolean isCompressed(BytesReference bytes) {
        return checkHeader(bytes, HEADER);
    }

    @Override
    public boolean isCompressed(byte[] data, int offset, int length) {
        return checkHeader(data, offset, length, HEADER);
    }

    @Override
    public boolean isCompressed(ChannelBuffer buffer) {
        return checkHeader(buffer, HEADER);
    }

    @Override
    public boolean isCompressed(IndexInput in) throws IOException {
        return checkHeader(in, HEADER);
    }

    @Override
    public LZ4CompressedStreamInput streamInput(StreamInput in) throws IOException {
        return new LZ4CompressedStreamInput(in, decompressor);
    }

    @Override
    public LZ4CompressedStreamOutput streamOutput(StreamOutput out) throws IOException {
        return new LZ4CompressedStreamOutput(out, compressor, chunkSize, minCompressionRatio);
    }

    @Override
    public byte[] compress(byte[] data, int offset, int length) throws IOException {
        if (length == 0) {
            return Arrays.copyOf(HEADER, HEADER.length + 1);
        }
        // decompress directly in the result buffer
        final byte[] compressed = BufferRecycler.instance().allocEncodingBuffer(compressor.maxCompressedLength(length));
        final int cpLen = compressor.compress(data, offset, length, compressed, 0);
        final byte[] ret;
        final BytesStreamOutput out;
        if (cpLen >= length * minCompressionRatio) {
            final int totalLen = HEADER.length + ByteUtils.vIntBytes(length) + VINT_BYTES_0 + length + VINT_BYTES_0;
            ret = new byte[totalLen];
            out = new BytesStreamOutput(ret);
            out.writeBytes(HEADER);
            out.writeVInt(length);
            out.writeVInt(0);
            out.writeBytes(data, offset, length);
        } else {
            final int totalLen = HEADER.length + ByteUtils.vIntBytes(length) + ByteUtils.vIntBytes(length - cpLen) + cpLen + VINT_BYTES_0;
            ret = new byte[totalLen];
            out = new BytesStreamOutput(ret);
            out.writeBytes(HEADER);
            out.writeVInt(length);
            out.writeVInt(length - cpLen);
            out.writeBytes(compressed, 0, cpLen);
        }
        BufferRecycler.instance().releaseEncodeBuffer(compressed);
        out.writeVInt(0);
        assert out.position() == ret.length : out.position() + " " + ret.length;
        return ret;
    }

    @Override
    public byte[] uncompress(byte[] data, int offset, int length) throws IOException {
        checkHeader(data, offset, length, HEADER);
        final BytesStreamInput in = new BytesStreamInput(data, offset + HEADER.length, length - HEADER.length, false);
        int chunkLen = in.readVInt();
        byte[] uncompressed = new byte[chunkLen];
        int uncompressedLen = chunkLen;
        while (chunkLen > 0) {
            final int cpLen = chunkLen - in.readVInt();
            if (cpLen == chunkLen) {
                System.arraycopy(data, in.position(), uncompressed, uncompressedLen - chunkLen, chunkLen);
            } else {
                final int cpLen2 = decompressor.decompress(data, in.position(), uncompressed, uncompressedLen - chunkLen, chunkLen);
                assert cpLen2 == cpLen;
            }
            in.skip(cpLen);
            chunkLen = in.readVInt();
            uncompressedLen += chunkLen;
            uncompressed = ArrayUtil.grow(uncompressed, uncompressedLen);
        }
        in.close();
        if (uncompressed.length == uncompressedLen) {
            return uncompressed;
        } else {
            return Arrays.copyOf(uncompressed, uncompressedLen);
        }
    }

}
