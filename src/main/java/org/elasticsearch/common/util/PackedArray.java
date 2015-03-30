/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.common.util;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.lease.Releasable;

import java.util.Collection;
import java.util.Collections;

/** A long array wrapper that packs values. */
public class PackedArray implements LongArray, Releasable {

    private static long mask(int b) {
        return (1L << b) - 1;
    }

    private final BigArrays bigArrays;
    private final int bpv;
    private LongArray longs;
    private long size;

    public PackedArray(BigArrays bigArrays, int bpv, long length) {
        if (bpv < 1 || bpv > 63) {
            throw new ElasticsearchIllegalArgumentException("bpv must be in [1-63], got [" + bpv + "]");
        }
        if (length < 0) {
            throw new ElasticsearchIllegalArgumentException("length must be positive, got [" + length + "]");
        }
        this.bigArrays = bigArrays;
        this.bpv = bpv;
        this.size = length;
        longs = bigArrays.newLongArray(toLongLength(length));
    }

    private long toLongLength(long length) {
        return ((length * bpv + 63) / 64);
    }

    /** Grow this array to the configured size. Does nothing is the array is
     *  already large enough. */
    public void grow(long length) {
        if (length > this.size) {
            final long longsLength = toLongLength(length);
            longs = bigArrays.grow(longs, longsLength);
            this.size = length;
        }
    }

    @Override
    public void close() throws ElasticsearchException {
        longs.close();
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public long ramBytesUsed() {
        return longs.ramBytesUsed();
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
    }

    @Override
    public long get(long index) {
        final long bitIndex = index * bpv;
        final long longIndex = bitIndex >>> 6;
        final int indexWithinLong = (int) (bitIndex & 0x3F);

        if (indexWithinLong + bpv <= 64) {
            // everything is within a single block
            final long bits = longs.get(longIndex);
            return (bits >>> (64 - bpv - indexWithinLong)) & mask(bpv);
        } else {
            // scattered across 2 blocks
            final long bits1 = longs.get(longIndex);
            final long bits2 = longs.get(longIndex + 1);
            // number of bits in the 2nd block
            final int bitCount2 = indexWithinLong + bpv - 64;
            return ((bits1 << bitCount2) | (bits2 >>> (64 - bitCount2))) & mask(bpv);
        }
    }

    @Override
    public long set(long index, long value) {
        assert value >= 0;
        assert value <= mask(bpv);
        final long bitIndex = index * bpv;
        final long longIndex = bitIndex >>> 6;
        final int indexWithinLong = (int) (bitIndex & 0x3F);

        if (indexWithinLong + bpv <= 64) {
            // everything is within a single block
            long bits = longs.get(longIndex);
            final int rightBitCount = 64 - bpv - indexWithinLong;
            final long ret = (bits >>> rightBitCount) & mask(bpv);
            bits = bits & ~(mask(bpv) << rightBitCount) | (value << rightBitCount);
            longs.set(longIndex, bits);
            return ret;
        } else {
            // scattered across 2 blocks
            long bits1 = longs.get(longIndex);
            long bits2 = longs.get(longIndex + 1);
            // number of bits in the 2nd block
            final int bitCount2 = indexWithinLong + bpv - 64;
            final long ret = ((bits1 << bitCount2) | (bits2 >>> (64 - bitCount2))) & mask(bpv);
            bits1 = bits1 & (~0L << (bpv - bitCount2)) | (value >>> bitCount2);
            bits2 = bits2 & (~0L >>> bitCount2) | (value << 64 - bitCount2);
            longs.set(longIndex, bits1);
            longs.set(longIndex + 1, bits2);
            return ret;
        }
    }

    @Override
    public long increment(long index, long inc) {
        return set(index, get(index) + inc);
    }

    @Override
    public void fill(long fromIndex, long toIndex, long value) {
        for (long i = fromIndex; i < toIndex; ++i) {
            set(i, value);
        }
    }

}
