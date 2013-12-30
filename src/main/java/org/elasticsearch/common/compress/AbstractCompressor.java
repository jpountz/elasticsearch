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

package org.elasticsearch.common.compress;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;

/** Base implementation of a {@link Compressor}. */
public abstract class AbstractCompressor implements Compressor {

    protected final boolean checkHeader(BytesReference bytes, byte[] header) {
        if (bytes.length() < header.length) {
            return false;
        }
        for (int i = 0; i < header.length; ++i) {
            if (bytes.get(i) != header[i]) {
                return false;
            }
        }
        return true;
    }

    protected final boolean checkHeader(byte[] data, int offset, int length, byte[] header) {
        if (length < header.length) {
            return false;
        }
        for (int i = 0; i < header.length; ++i) {
            if (data[offset + i] != header[i]) {
                return false;
            }
        }
        return true;
    }

    protected final boolean checkHeader(ChannelBuffer buffer, byte[] header) {
        int offset = buffer.readerIndex();
        if (buffer.readableBytes() < header.length) {
            return false;
        }
        for (int i = 0; i < header.length; ++i) {
            if (buffer.getByte(offset + i) != header[i]) {
                return false;
            }
        }
        return true;
    }

    protected final boolean checkHeader(IndexInput in, byte[] header) throws IOException {
        long currentPointer = in.getFilePointer();
        // since we have some metdata before the first compressed header, we check on our specific header
        if (in.length() - currentPointer < header.length) {
            return false;
        }
        boolean equal = true;
        for (int i = 0; i < header.length; i++) {
            if (in.readByte() != header[i]) {
                equal = false;
                break;
            }
        }
        in.seek(currentPointer);
        return equal;
    }

    @Override
    public CompressedIndexInput indexInput(IndexInput in) throws IOException {
        throw new UnsupportedOperationException("Deprecated operation");
    }

    // public for test
    public static byte[] compress(Compressor compressor, byte[] data, int offset, int length) throws IOException {
        final BytesStreamOutput bytesOout = new BytesStreamOutput(length + 16);
        final StreamOutput streamOut = compressor.streamOutput(bytesOout);
        streamOut.writeBytes(data, offset, length);
        streamOut.close();
        return bytesOout.bytes().toBytes();
    }

    @Override
    public byte[] compress(byte[] data, int offset, int length) throws IOException {
        return compress(this, data, offset, length);
    }

    // public for test
    public static byte[] uncompress(Compressor compressor, byte[] data, int offset, int length) throws IOException {
        final BytesStreamInput in = new BytesStreamInput(data, offset, length, false);
        final StreamInput streamIn = compressor.streamInput(in);
        final BytesStreamOutput out = new BytesStreamOutput(length * 2);
        Streams.copy(streamIn, out);
        streamIn.close();
        return out.bytes().toBytes();
    }

    @Override
    public byte[] uncompress(byte[] data, int offset, int length) throws IOException {
        return uncompress(this, data, offset, length);
    }

    @Override
    public String toString() {
        return type();
    }

}
