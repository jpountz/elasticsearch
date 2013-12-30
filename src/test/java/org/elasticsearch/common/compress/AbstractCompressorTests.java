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

import org.elasticsearch.test.ElasticsearchTestCase;

import java.util.Arrays;

public abstract class AbstractCompressorTests extends ElasticsearchTestCase {

    public abstract Compressor getCompressor();

    // high values of max make the data incompressible
    private static byte[] randomBytes(int offset, int length, int max) {
        final byte[] bytes = new byte[offset + length + randomInt(10)];
        for (int i = 0; i < bytes.length; ++i) {
            bytes[i] = (byte) randomInt(max);
        }
        return bytes;
    }

    public void testRoundtrip(byte[] data, int offset, int length) throws Exception {
        final Compressor compressor = getCompressor();
        final byte[] compressed0;
        if (randomBoolean()) { // block compression
            compressed0 = compressor.compress(data, offset, length);
        } else { // stream compression
            compressed0 = AbstractCompressor.compress(compressor, data, offset, length);
        }

        int compressedLen = compressed0.length;
        final byte[] compressed = new byte[compressedLen + 10];
        getRandom().nextBytes(compressed);
        int compressedOffset = randomInt(10);
        System.arraycopy(compressed0, 0, compressed, compressedOffset, compressedLen);

        // block uncompression
        final byte[] uncompressed1 = compressor.uncompress(compressed, compressedOffset, compressedLen);
        assertArrayEquals(Arrays.copyOfRange(data, offset, offset + length), uncompressed1);

        // stream uncompression
        final byte[] uncompressed2 = AbstractCompressor.uncompress(compressor, compressed, compressedOffset, compressedLen);
        assertArrayEquals(uncompressed1, uncompressed2);
    }

    public void testRoundtrip(int max) throws Exception {
        final int offset = randomInt(10);
        final int length = randomBoolean() ? randomInt(200) : randomInt(1 << 18);
        final byte[] bytes = randomBytes(offset, length, max);
        testRoundtrip(bytes, offset, length);
    }

    public void testEmpty() throws Exception {
        testRoundtrip(new byte[10], randomInt(10), 0);
    }

    public void testLengthMultipleOfChunkSize() throws Exception {
        final int offset = randomInt(10);
        final int length = 1 << 18;
        for (int max : new int[] {3, 100}) {
            final byte[] bytes = randomBytes(offset, length, max);
            testRoundtrip(bytes, offset, length);
        }
    }

    public void testRandomCompressibleBytes() throws Exception {
        testRoundtrip(3);
    }

    public void testRandomIncompressibleBytes() throws Exception {
        testRoundtrip(200);
    }

}
