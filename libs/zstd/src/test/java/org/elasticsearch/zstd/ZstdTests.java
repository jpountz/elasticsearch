/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.zstd;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.Arrays;

public class ZstdTests extends ESTestCase {

    public void testMaxCompressedLength() {
        assertThat(Zstd.maxCompressedLength(0), Matchers.greaterThanOrEqualTo(0));
        assertThat(Zstd.maxCompressedLength(100), Matchers.greaterThanOrEqualTo(100));
        assertThat(Zstd.maxCompressedLength(Integer.MAX_VALUE / 2), Matchers.greaterThanOrEqualTo(Integer.MAX_VALUE / 2));

        expectThrows(IllegalArgumentException.class, () -> Zstd.maxCompressedLength(-1));
        expectThrows(IllegalArgumentException.class, () -> Zstd.maxCompressedLength(Integer.MIN_VALUE));
        expectThrows(ArithmeticException.class, () -> Zstd.maxCompressedLength(Integer.MAX_VALUE));
    }

    public void testCompressValidation() {
        assertEquals(
            "Null dst",
            expectThrows(IllegalArgumentException.class, () -> Zstd.compress(null, 0, new byte[1000], 0, 10, 0)).getMessage()
        );
        assertEquals(
            "Null src",
            expectThrows(IllegalArgumentException.class, () -> Zstd.compress(new byte[1000], 0, null, 0, 10, 0)).getMessage()
        );
        assertEquals(
            "Negative dstOff",
            expectThrows(IllegalArgumentException.class, () -> Zstd.compress(new byte[1000], -3, new byte[1000], 0, 10, 0)).getMessage()
        );
        assertEquals(
            "Negative srcOff",
            expectThrows(IllegalArgumentException.class, () -> Zstd.compress(new byte[1000], 0, new byte[1000], -1, 10, 0)).getMessage()
        );
        assertEquals(
            "Negative srcLen",
            expectThrows(IllegalArgumentException.class, () -> Zstd.compress(new byte[1000], 0, new byte[1000], 0, -3, 0)).getMessage()
        );
        assertEquals(
            "Out of bounds dstOff",
            expectThrows(IllegalArgumentException.class, () -> Zstd.compress(new byte[1000], 1005, new byte[1000], 0, 10, 0)).getMessage()
        );
        assertEquals(
            "Out of bounds srcOff + srcLen",
            expectThrows(IllegalArgumentException.class, () -> Zstd.compress(new byte[1000], 0, new byte[1000], 998, 10, 0)).getMessage()
        );
        assertEquals(
            "Out of bounds srcOff + srcLen",
            expectThrows(IllegalArgumentException.class, () -> Zstd.compress(new byte[1000], 0, new byte[1000], 1005, 0, 0)).getMessage()
        );
        // srcOff + srcLen overflows
        assertEquals(
            "Out of bounds srcOff + srcLen",
            expectThrows(
                IllegalArgumentException.class,
                () -> Zstd.compress(new byte[1000], 0, new byte[1000], 500, Integer.MAX_VALUE - 200, 0)
            ).getMessage()
        );
        // dst capacity too low
        byte[] toCompress = new byte[1000];
        for (int i = 0; i < toCompress.length; ++i) {
            toCompress[i] = randomByte();
        }
        assertEquals(
            "Destination buffer is too small",
            expectThrows(IllegalArgumentException.class, () -> Zstd.compress(new byte[500], 0, toCompress, 0, 1000, 0)).getMessage()
        );
    }

    public void testDecompressValidation() {
        assertEquals(
            "Null dst",
            expectThrows(IllegalArgumentException.class, () -> Zstd.decompress(null, 0, new byte[1000], 0, 10)).getMessage()
        );
        assertEquals(
            "Null src",
            expectThrows(IllegalArgumentException.class, () -> Zstd.decompress(new byte[1000], 0, null, 0, 10)).getMessage()
        );
        assertEquals(
            "Negative dstOff",
            expectThrows(IllegalArgumentException.class, () -> Zstd.decompress(new byte[1000], -3, new byte[1000], 0, 10)).getMessage()
        );
        assertEquals(
            "Negative srcOff",
            expectThrows(IllegalArgumentException.class, () -> Zstd.decompress(new byte[1000], 0, new byte[1000], -1, 10)).getMessage()
        );
        assertEquals(
            "Negative srcLen",
            expectThrows(IllegalArgumentException.class, () -> Zstd.decompress(new byte[1000], 0, new byte[1000], 0, -3)).getMessage()
        );
        assertEquals(
            "Out of bounds dstOff",
            expectThrows(IllegalArgumentException.class, () -> Zstd.decompress(new byte[1000], 1005, new byte[1000], 0, 10)).getMessage()
        );
        assertEquals(
            "Out of bounds srcOff + srcLen",
            expectThrows(IllegalArgumentException.class, () -> Zstd.decompress(new byte[1000], 0, new byte[1000], 998, 10)).getMessage()
        );
        assertEquals(
            "Out of bounds srcOff + srcLen",
            expectThrows(IllegalArgumentException.class, () -> Zstd.decompress(new byte[1000], 0, new byte[1000], 1005, 0)).getMessage()
        );
        // srcOff + srcLen overflows
        assertEquals(
            "Out of bounds srcOff + srcLen",
            expectThrows(
                IllegalArgumentException.class,
                () -> Zstd.decompress(new byte[1000], 0, new byte[1000], 500, Integer.MAX_VALUE - 200)
            ).getMessage()
        );
        // Invalid compressed format
        byte[] toCompress = new byte[1000];
        for (int i = 0; i < toCompress.length; ++i) {
            toCompress[i] = (byte) i;
        }
        assertEquals(
            "Unknown frame descriptor",
            expectThrows(IllegalArgumentException.class, () -> Zstd.decompress(new byte[500], 0, toCompress, 0, 1000)).getMessage()
        );
        final int compressedLength = Zstd.compress(toCompress, 0, new byte[1000], 0, 1000, 0);
        assertEquals(
            "Destination buffer is too small",
            expectThrows(IllegalArgumentException.class, () -> Zstd.decompress(new byte[500], 0, toCompress, 0, compressedLength))
                .getMessage()
        );
    }

    public void testEmptyArray() {
        doTestRoundtrip(new byte[0]);
    }

    public void testSingleByteArray() {
        doTestRoundtrip(new byte[] { randomByte() });
    }

    public void testConstantByteArray() {
        byte[] b = new byte[randomIntBetween(100, 1000)];
        Arrays.fill(b, randomByte());
        doTestRoundtrip(b);
    }

    public void testCycle() {
        byte[] b = new byte[randomIntBetween(100, 1000)];
        for (int i = 0; i < b.length; ++i) {
            b[i] = (byte) (i & 0x0F);
        }
        doTestRoundtrip(b);
    }

    private void doTestRoundtrip(byte[] data) {
        {
            byte[] compressed = new byte[Zstd.maxCompressedLength(data.length)];
            final int compressedLength = Zstd.compress(compressed, 0, data, 0, data.length, randomIntBetween(-3, 9));
            compressed = Arrays.copyOf(compressed, compressedLength);
            byte[] restored = new byte[data.length];
            Zstd.decompress(restored, 0, compressed, 0, compressed.length);
            assertArrayEquals(data, restored);
        }
        // Now with non-zero offsets
        {
            final int compressedOffset = randomIntBetween(1, 1000);
            final int decompressedOffset = randomIntBetween(1, 1000);
            byte[] dataCopy = new byte[decompressedOffset + data.length];
            System.arraycopy(data, 0, dataCopy, decompressedOffset, data.length);
            byte[] compressed = new byte[compressedOffset + Zstd.maxCompressedLength(data.length)];
            final int compressedLength = Zstd.compress(compressed, compressedOffset, dataCopy, decompressedOffset, data.length, randomIntBetween(-3, 9));
            byte[] restored = new byte[decompressedOffset + data.length];
            Zstd.decompress(restored, decompressedOffset, compressed, compressedOffset, compressedLength);
            assertArrayEquals(data, Arrays.copyOfRange(restored, decompressedOffset, decompressedOffset + data.length));
        }
    }
}
