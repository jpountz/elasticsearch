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

import com.carrotsearch.hppc.hash.MurmurHash3;
import com.google.common.base.Preconditions;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PackedArray;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

/**
 * Implementation of the count-min sketch data-structure that can be used for
 * frequency estimation. This implementation uses conservative adds in order
 * to reduce noise.
 */
public final class CountMinSketch implements Releasable {

    final int d;
    final int lgW, lgMaxFreq;
    final long maxFreq;
    final PackedArray freqs;

    /**
     * Create a new CountMinSketch instance. Frequencies above {@code maxFreq}
     * will just be assumed to be equal to {@code maxFreq}.
     * @param d the number of hash functions to use
     * @param lgW the log in base 2 of the number of bits per
     */
    public CountMinSketch(int d, int lgW, int lgMaxFreq, BigArrays bigArrays) {
        if (d < 1) {
            throw new ElasticsearchIllegalArgumentException("Must use at least one hash function");
        }
        if (lgW < 1) {
            throw new ElasticsearchIllegalArgumentException("Must use at least 2 buckets per hash");
        }
        if (lgMaxFreq <= 1) {
            throw new ElasticsearchIllegalArgumentException("lgMaxFreq must be at least 1");
        }
        this.d = d;
        this.lgW = lgW;
        this.lgMaxFreq = lgMaxFreq;
        this.maxFreq = 1L << lgMaxFreq;
        this.freqs = new PackedArray(bigArrays, lgMaxFreq, 0L);
    }

    public int d() {
        return d;
    }
    
    public int lgW() {
        return lgW;
    }
    
    public int lgMaxFreq() {
        return lgMaxFreq;
    }
    
    public long maxBucket() {
        return (freqs.size() >>> lgW) / d;
    }

    long baseAddress(long bucket) {
        return d * (bucket << lgW);
    }

    private long address(long base, int d, long hash) {
        return base                           // base address for the bucket
                + d * (1L << lgW)             // base address for the hash
                + (hash & ((1L << lgW) - 1)); // hash remainder
    }

    private void grow(long numBuckets) {
        freqs.grow(baseAddress(numBuckets));
    }

    private long freq(long bucket, long hash) {
        final long bucketBaseIndex = baseAddress(bucket);
        long minFreq = Long.MAX_VALUE;
        for (int i = 0; i < d; ++i, hash = hash * 0x5DEECE66DL + 0xBL) {
            final long index = address(bucketBaseIndex, i, hash);
            final long freq = freqs.get(index);
            assert freq <= maxFreq;
            minFreq = Math.min(freq, minFreq);
        }
        return minFreq;
    }

    private void updateFreq(long bucket, long hash, long newFreq) {
        final long bucketBaseIndex = baseAddress(bucket);
        for (int i = 0; i < d; ++i, hash = hash * 0x5DEECE66DL + 0xBL) {
            final long index = address(bucketBaseIndex, i, hash);
            final long freq = freqs.get(index);
            if (newFreq > freq) {
                freqs.set(index, newFreq);
            }
        }
    }

    public void collect(long bucket, long hash, long inc) {
        grow(bucket + 1);
        final long freq = freq(bucket, hash);
        if (freq < maxFreq) {
            updateFreq(bucket, hash, Math.min(maxFreq, freq + inc));
        }
    }

    public void collect(long bucket, long hash) {
        collect(bucket, hash, 1);
    }

    private long[] cardinalities(long bucket, int d, long... frequencies) {
        long[] cardinalities = new long[frequencies.length];
        final long baseIndex = baseAddress(bucket) + d * (1L << lgW);

        // Since we use a good hash function, the minimum freq in the table
        // is almost noise for sure. We can use it to fix the actual frequency
        // of heavy hitters
        long noise = Long.MAX_VALUE;
        for (long i = 0; noise != 0 && i < 1L << lgW; ++i) {
            final long freq = freqs.get(baseIndex + i);
            noise = Math.min(freq, noise);
        }

        for (long i = 0; i < 1L << lgW; ++i) {
            long freq = freqs.get(baseIndex + i);
            // try to remove some noise
            if (freq < maxFreq) {
                if (freq / 2 >= noise) {
                    freq -= noise;
                } else if (freq >= noise) {
                    freq -= noise / 2;
                }
            }
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

    private long[] merge(long[][] cardinalities, long[] frequencies) {
        final long[] merged = Arrays.copyOf(cardinalities[0], cardinalities[0].length);
        for (int i = 1; i < d; ++i) {
            final long[] c = cardinalities[i];
            for (int j = 0; j < merged.length; ++j) {
                merged[j] = Math.min(merged[j], c[j]);
            }
        }

        final int index1 = Arrays.binarySearch(frequencies, 1L);
        if (index1 >= 0) {
            final double w = 1L << lgW;
            final double[] uniqueValueCounts = new double[d];
            for (int i = 0; i < d; ++i) {
                final double zeros = w - cardinalities[i][index1];
                uniqueValueCounts[i] = w * Math.log(w / zeros);
            }
            // Then we take the median
            Arrays.sort(uniqueValueCounts);
            if ((d & 1) == 1) {
                merged[index1] = Math.round(uniqueValueCounts[d >>> 1]);
            } else {
                merged[index1] = Math.round((uniqueValueCounts[(d >>> 1) - 1] + uniqueValueCounts[d >>> 1]) / 2);
            }
        }

        return merged;
    }

    public void merge(long thisBucket, CountMinSketch other, long otherBucket) {
        Preconditions.checkArgument(d == other.d);
        Preconditions.checkArgument(lgW == other.lgW);
        final long thisBaseAddress = baseAddress(thisBucket);
        final long otherBaseAddress = other.baseAddress(otherBucket);
        final long count = baseAddress(1);
        for (long i = 0; i < count; ++i) {
            final long newFreq = freqs.get(thisBaseAddress + i) + other.freqs.get(otherBaseAddress + i);
            freqs.set(thisBaseAddress + i, Math.min(newFreq, maxFreq));
        }
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
        return merge(cardinalities, frequencies);
    }

    @Override
    public void close() throws ElasticsearchException {
        freqs.close();
    }

    public void writeTo(long bucket, StreamOutput out) throws IOException {
        out.writeVInt(d);
        out.writeVInt(lgW);
        out.writeVInt(lgMaxFreq);
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
        final int lgMaxFreq = in.readVInt();
        final CountMinSketch sketch = new CountMinSketch(d, lgW, lgMaxFreq, bigArrays);
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

    // nocommit: for testing
    public static void main(String[] args) {
        long[] actualFreqs = new long[1024];
        final CountMinSketch sketch = new CountMinSketch(3, 8, 25, BigArrays.NON_RECYCLING_INSTANCE);
        Random r = new Random(0);
        for (int i = 0; i < 10000; ++i) {
            final int rint = r.nextInt(1 << r.nextInt(10));
            actualFreqs[rint]++;
            sketch.collect(0, MurmurHash3.hash((long) rint));
        }
        long[] frequencies = new long[] { 1, 2, 3, 5, 10, 20, 100, 1000, 10000 };
        long[] actualFreqTable = new long[frequencies.length];
        for (int i = 0; i < frequencies.length; ++i) {
            long c = 0;
            for (long freq : actualFreqs) {
                if (freq >= frequencies[i]) {
                    c += 1;
                }
            }
            actualFreqTable[i] = c;
        }
        System.out.println(Arrays.toString(actualFreqTable));
        System.out.println(Arrays.toString(sketch.cardinalities(0, frequencies)));
    }

}
