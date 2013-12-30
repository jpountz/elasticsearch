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

package org.elasticsearch.common.compress.lzf;

import com.ning.compress.lzf.ChunkDecoder;
import com.ning.compress.lzf.LZFChunk;
import com.ning.compress.lzf.LZFEncoder;
import com.ning.compress.lzf.util.ChunkDecoderFactory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.AbstractCompressor;
import org.elasticsearch.common.compress.CompressedIndexInput;
import org.elasticsearch.common.compress.CompressedStreamInput;
import org.elasticsearch.common.compress.CompressedStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;

/**
 */
public class LZFCompressor extends AbstractCompressor {

    static final byte[] LUCENE_HEADER = {'L', 'Z', 'F', 0};

    public static final String DECODER_KEY = "compress.lzf.decoder";
    public static final String DECODER_VALUE_OPTIMAL = "optimal";
    public static final String DECODER_VALUE_SAFE = "safe";
    public static final String TYPE = "lzf";

    private ChunkDecoder decoder;

    public LZFCompressor() {
        if (Constants.SUN_OS) {
            this.decoder = ChunkDecoderFactory.safeInstance();
        } else {
            this.decoder = ChunkDecoderFactory.optimalInstance();
        }
        Loggers.getLogger(LZFCompressor.class).debug("using [{}] decoder", this.decoder.getClass().getSimpleName());
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public void configure(Settings settings) {
        String decoderType = settings.get(DECODER_KEY, null);
        if (decoderType != null) {
            if (DECODER_VALUE_OPTIMAL.equalsIgnoreCase(decoderType)) {
                this.decoder = ChunkDecoderFactory.optimalInstance();
                Loggers.getLogger(LZFCompressor.class).debug("using [{}] decoder", this.decoder.getClass().getSimpleName());
            } else if (DECODER_VALUE_SAFE.equalsIgnoreCase(decoderType)) {
                this.decoder = ChunkDecoderFactory.safeInstance();
                Loggers.getLogger(LZFCompressor.class).debug("using [{}] decoder", this.decoder.getClass().getSimpleName());
            } else {
                Loggers.getLogger(LZFCompressor.class).warn("decoder type not recognized [{}], still using [{}]", decoderType, this.decoder.getClass().getSimpleName());
            }
        }
    }

    @Override
    public boolean isCompressed(BytesReference bytes) {
        return bytes.length() >= 3 &&
                bytes.get(0) == LZFChunk.BYTE_Z &&
                bytes.get(1) == LZFChunk.BYTE_V &&
                (bytes.get(2) == LZFChunk.BLOCK_TYPE_COMPRESSED || bytes.get(2) == LZFChunk.BLOCK_TYPE_NON_COMPRESSED);
    }

    @Override
    public boolean isCompressed(byte[] data, int offset, int length) {
        return length >= 3 &&
                data[offset] == LZFChunk.BYTE_Z &&
                data[offset + 1] == LZFChunk.BYTE_V &&
                (data[offset + 2] == LZFChunk.BLOCK_TYPE_COMPRESSED || data[offset + 2] == LZFChunk.BLOCK_TYPE_NON_COMPRESSED);
    }

    @Override
    public boolean isCompressed(ChannelBuffer buffer) {
        int offset = buffer.readerIndex();
        return buffer.readableBytes() >= 3 &&
                buffer.getByte(offset) == LZFChunk.BYTE_Z &&
                buffer.getByte(offset + 1) == LZFChunk.BYTE_V &&
                (buffer.getByte(offset + 2) == LZFChunk.BLOCK_TYPE_COMPRESSED || buffer.getByte(offset + 2) == LZFChunk.BLOCK_TYPE_NON_COMPRESSED);
    }

    @Override
    public boolean isCompressed(IndexInput in) throws IOException {
        return checkHeader(in, LUCENE_HEADER);
    }

    @Override
    public byte[] uncompress(byte[] data, int offset, int length) throws IOException {
        return decoder.decode(data, offset, length);
    }

    @Override
    public byte[] compress(byte[] data, int offset, int length) throws IOException {
        return LZFEncoder.encode(data, offset, length);
    }

    @Override
    public CompressedStreamInput streamInput(StreamInput in) throws IOException {
        return new LZFCompressedStreamInput(in, decoder);
    }

    @Override
    public CompressedStreamOutput streamOutput(StreamOutput out) throws IOException {
        return new LZFCompressedStreamOutput(out);
    }

    @Override
    public CompressedIndexInput indexInput(IndexInput in) throws IOException {
        return new LZFCompressedIndexInput(in, decoder);
    }
}
