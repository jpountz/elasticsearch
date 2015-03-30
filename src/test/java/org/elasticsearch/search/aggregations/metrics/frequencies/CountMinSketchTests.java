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

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.test.ElasticsearchTestCase;

public class CountMinSketchTests extends ElasticsearchTestCase {

    public void testSimple() {
        final int d = randomIntBetween(1, 10);
        final int lgW = randomIntBetween(10, 20);
        final long maxFreq = randomIntBetween(10, 1 << randomIntBetween(5, 30));
        CountMinSketch sketch = new CountMinSketch(d, lgW, maxFreq, BigArrays.NON_RECYCLING_INSTANCE);

        assertArrayEquals(
                new long[] {0, 0, 0},
                sketch.cardinalities(0, 1, 2, 4));

        sketch.collect(0, MurmurHash3.hash(1L));
        assertArrayEquals(
                new long[] {1, 0, 0},
                sketch.cardinalities(0, 1, 2, 4));

        sketch.collect(0, MurmurHash3.hash(2L));
        assertArrayEquals(
                new long[] {2, 0, 0},
                sketch.cardinalities(0, 1, 2, 4));

        sketch.collect(0, MurmurHash3.hash(2L));
        assertArrayEquals(
                new long[] {2, 1, 0},
                sketch.cardinalities(0, 1, 2, 4));

        for (int i = 0; i < 100000; ++i) {
            sketch.collect(0, MurmurHash3.hash(3L));
        }
        assertArrayEquals(
                new long[] {3, 2, 1},
                sketch.cardinalities(0, 1, 2, 4));

        try {
            sketch.cardinalities(0, 2, 1); // out of order
        } catch (ElasticsearchIllegalArgumentException e) {
            // ok
        }

        try {
            sketch.cardinalities(0, 2, 1); // out of order
        } catch (ElasticsearchIllegalArgumentException e) {
            // ok
        }
    }

    public void testDuelBucket() {
        final int iters = 10;
        for (int i = 0; i < iters; ++i) {
            final int d = randomIntBetween(1, 10);
            final int lgW = randomIntBetween(5, 20);
            final int maxFreq = randomIntBetween(10, 1 << randomIntBetween(5, 30));
            final long bucket = randomIntBetween(1, 5);
            final CountMinSketch sketch = new CountMinSketch(d, lgW, maxFreq, BigArrays.NON_RECYCLING_INSTANCE);
            for (int j = randomIntBetween(0, 10000); j >= 0; --j) {
                final long hash = MurmurHash3.hash((long) randomIntBetween(0, 1 << randomIntBetween(0, 10)));
                sketch.collect(bucket, hash);
                sketch.collect(0, hash);
            }
            final long[] frequencies = new long[randomIntBetween(1, 100)];
            frequencies[frequencies.length - 1] = randomIntBetween(frequencies.length, maxFreq);
            for (int j = frequencies.length - 2; j >= 0; --j) {
                frequencies[j] = randomIntBetween(j + 1, (int) frequencies[j + 1] - 1);
            }
            assertArrayEquals(
                    sketch.cardinalities(0, frequencies),
                    sketch.cardinalities(bucket, frequencies));
        }
    }
    
    public void testSerialization() throws Exception {
        final int iters = 10;
        for (int i = 0; i < iters; ++i) {
            final int d = randomIntBetween(1, 10);
            final int lgW = randomIntBetween(5, 20);
            final int maxFreq = randomIntBetween(10, 1 << randomIntBetween(5, 30));
            final long bucket = randomIntBetween(0, 5);
            final CountMinSketch sketch = new CountMinSketch(d, lgW, maxFreq, BigArrays.NON_RECYCLING_INSTANCE);
            for (int j = randomIntBetween(0, 10000); j >= 0; --j) {
                sketch.collect(bucket, MurmurHash3.hash((long) randomIntBetween(0, 1 << randomIntBetween(0, 10))));
            }

            BytesStreamOutput out = new BytesStreamOutput();
            sketch.writeTo(bucket, out);
            BytesStreamInput in = new BytesStreamInput(out.bytes());

            final CountMinSketch sketch2 = CountMinSketch.readFrom(in, BigArrays.NON_RECYCLING_INSTANCE);
            assertEquals(sketch.d, sketch2.d);
            assertEquals(sketch.lgW, sketch2.lgW);
            assertEquals(sketch.d, sketch2.d);
            for (long j = 0; j < sketch2.baseAddress(1); ++j) {
                assertEquals(sketch.freqs.get(sketch.baseAddress(bucket) + j), sketch2.freqs.get(j));
            }
        }
    }

}
