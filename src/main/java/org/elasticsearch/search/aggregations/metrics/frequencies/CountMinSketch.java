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

package org.elasticsearch.search.aggregations.metrics.frequencies;

import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PackedArray;

import java.io.IOException;
import java.util.Arrays;

/**
 * Implementation of the count-min sketch data-structure that can be used for
 * frequency estimation.
 */
public final class CountMinSketch implements Releasable {

    final int d;
    final int lgW;
    final long maxFreq;
    final PackedArray freqs;

    /**
     * Create a new CountMinSketch instance. Frequencies above {@code maxFreq}
     * will just be assumed to be equal to {@code maxFreq}.
     * @param d the number of hash functions to use
     * @param lgW the log in base 2 of the number of bits per
     */
    public CountMinSketch(int d, int lgW, long maxFreq, BigArrays bigArrays) {
        if (d < 1) {
            throw new ElasticsearchIllegalArgumentException("Must use at least one hash function");
        }
        if (lgW < 1) {
            throw new ElasticsearchIllegalArgumentException("Must use at least 2 buckets per hash");
        }
        if (maxFreq <= 1) {
            throw new ElasticsearchIllegalArgumentException("maxFreq must be at least 1");
        }
        this.d = d;
        this.lgW = lgW;
        this.maxFreq = maxFreq;
        this.freqs = new PackedArray(bigArrays, PackedInts.bitsRequired(maxFreq), 0L);
    }

    long baseAddress(long bucket) {
        return d * (bucket << lgW);
    }

    private void grow(long numBuckets) {
        freqs.grow(baseAddress(numBuckets));
    }

    public void collect(long bucket, long hash) {
        grow(bucket + 1);
        final long bucketBaseIndex = baseAddress(bucket);
        for (int i = 0; i < d; ++i) {
            // same as Random.next
            hash = hash * 0x5DEECE66DL + 0xBL;
            final long index = bucketBaseIndex    // base address for the bucket
                    + i * (1L << lgW)             // base address for the hash
                    + (hash & ((1L << lgW) - 1)); // hash remainder
            final long freq = freqs.get(index);
            assert freq <= maxFreq;
            if (freq < maxFreq) {
                freqs.set(index, freq + 1);
            }
        }
    }

    private long[] cardinalities(long bucket, int d, long... frequencies) {
        long[] cardinalities = new long[frequencies.length];
        final long baseIndex = baseAddress(bucket) + d * (1L << lgW);
        for (long i = 0; i < 1L << lgW; ++i) {
            final long freq = freqs.get(baseIndex + i);
            int index = Arrays.binarySearch(frequencies, freq);
            if (index < 0) {
                index = -2 - index;
            }
            if (index >= 0) {
                cardinalities[index] += 1;
            }
        }
        for (int i = cardinalities.length - 2; i >= 0; --i) {
            cardinalities[i] += cardinalities[i + 1];
        }
        return cardinalities;
    }

    private static long[] merge(long[][] cardinalities) {
        final long[] merged = Arrays.copyOf(cardinalities[0], cardinalities[0].length);
        for (int i = 1; i < cardinalities.length; ++i) {
            final long[] c = cardinalities[i];
            for (int j = 0; j < merged.length; ++j) {
                merged[j] = Math.min(merged[j], c[j]);
            }
        }
        return merged;
    }

    public long[] cardinalities(long bucket, long... frequencies) {
        for (long frequency : frequencies) {
            if (frequency <= 0) {
                throw new ElasticsearchIllegalArgumentException("Frequencies must be >= 0");
            }
            if (frequency > maxFreq) {
                throw new ElasticsearchIllegalArgumentException("Cannot request cardinalities for frequencies that are greater than maxFreq");
            }
        }
        for (int i = 1; i < frequencies.length; ++i) {
            if (frequencies[i] <= frequencies[i - 1]) {
                throw new ElasticsearchIllegalArgumentException("Frequencies must be in strict ascending order");
            }
        }

        if (freqs.size() < baseAddress(bucket + 1)) {
            return new long[frequencies.length]; // empty
        }

        final long[][] cardinalities = new long[d][];
        for (int i = 0; i < d; ++i) {
            cardinalities[i] = cardinalities(bucket, i, frequencies);
        }
        return merge(cardinalities);
    }

    @Override
    public void close() throws ElasticsearchException {
        freqs.close();
    }

    public void writeTo(long bucket, StreamOutput out) throws IOException {
        out.writeVInt(d);
        out.writeVInt(lgW);
        out.writeVLong(maxFreq);
        // Runs of zeros or ones are very likely so we try to compress them
        int zeroOneCount = 0;
        long tmpBits = 0;
        for (long i = baseAddress(bucket), end = baseAddress(bucket+1); i < end; ++i) {

            if (zeroOneCount == 64) {
                writeZeroOneRun(tmpBits, zeroOneCount, out);
                zeroOneCount = 0;
                tmpBits = 0;
            }

            final long freq = freqs.get(i);
            if (freq <= 1) {
                tmpBits |= freq << zeroOneCount;
                zeroOneCount += 1;
            } else {
                if (zeroOneCount > 0) {
                    writeZeroOneRun(tmpBits, zeroOneCount, out);
                    zeroOneCount = 0;
                    tmpBits = 0;
                }
                writeRegularFreq(freq, out);
            }

        }
        if (zeroOneCount > 0) {
            writeZeroOneRun(tmpBits, zeroOneCount, out);
        }
    }

    private static void writeRegularFreq(long freq, StreamOutput out) throws IOException {
        out.writeVLong(64 + freq);
    }

    private static void writeZeroOneRun(long bits, int bitCount, StreamOutput out) throws IOException {
        assert bitCount > 0 && bitCount <= 64;
        if (bitCount == 1) {
            // compression would use 2 bytes instead of 1
            writeRegularFreq(bits, out);
        } else {
            out.writeVLong(bitCount - 1);
            while (bitCount > 0) {
                out.writeByte((byte) bits);
                bits >>>= 8;
                bitCount -= 8;
            }
        }
    }

    public static CountMinSketch readFrom(StreamInput in, BigArrays bigArrays) throws IOException {
        final int d = in.readVInt();
        final int lgW = in.readVInt();
        final long maxFreq = in.readVLong();
        final CountMinSketch sketch = new CountMinSketch(d, lgW, maxFreq, bigArrays);
        sketch.grow(1);
        final PackedArray freqs = sketch.freqs;
        final long totalNumFreqs = sketch.baseAddress(1);
        for (long i = 0; i < totalNumFreqs; ) {
            final long token = in.readVLong();
            if (token >= 64) { // regular freq
                final long freq = token - 64;
                freqs.set(i++, freq);
            } else {
                final int bitCount = (int) (token + 1);
                long bits = 0;
                for (int j = 0; j < bitCount; j += 8) {
                    bits |= (in.readByte() & 0xFFL) << j;
                }
                assert bitCount == 64 || (bits >>> bitCount) == 0;
                for (int j = 0; bits != 0; ) {
                    final int ntz = Long.numberOfTrailingZeros(bits);
                    freqs.set(i + j + ntz, 1);
                    j += ntz + 1;
                    bits = bits >>> ntz >>> 1;
                }
                i += bitCount;
            }
        }
        return sketch;
    }

}
