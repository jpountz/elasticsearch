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

package org.elasticsearch.search.aggregations.metrics.termcount;

import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.hash.MurmurHash3;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.elasticsearch.search.aggregations.metrics.termcount.HyperLogLogPlusPlus.MAX_PRECISION;
import static org.elasticsearch.search.aggregations.metrics.termcount.HyperLogLogPlusPlus.MIN_PRECISION;
import static org.hamcrest.Matchers.closeTo;

public class HyperLogLogPlusPlusTests extends ElasticsearchTestCase {

    @Test
    public void encodeDecode() {
        final int iters = atLeast(100000);
        for (int i = 0; i < iters; ++i) {
            final int p1 = randomIntBetween(4, 24);
            final long hash = randomLong();
            final long index = HyperLogLogPlusPlus.index(hash, p1);
            final int runLen = HyperLogLogPlusPlus.runLen(hash, p1);
            final int encoded = HyperLogLogPlusPlus.encodeHash(hash, p1);
            assertEquals(index, HyperLogLogPlusPlus.decodeIndex(encoded, p1));
            assertEquals(runLen, HyperLogLogPlusPlus.decodeRunLen(encoded, p1));
        }
    }

    @Test
    public void accuracy() {
        final long bucket = randomInt(20);
        final int numValues = randomIntBetween(1, 1000000);
        final int maxValue = randomIntBetween(1, randomBoolean() ? 1000: 1000000);
        final int p = randomIntBetween(14, MAX_PRECISION);
        IntOpenHashSet set = new IntOpenHashSet();
        HyperLogLogPlusPlus e = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 1);
        for (int i = 0; i < numValues; ++i) {
            final int n = randomInt(maxValue);
            set.add(n);
            final long hash = MurmurHash3.hash((long) n);
            e.collect(bucket, hash);
            if (randomInt(100) == 0) {
                //System.out.println(e.cardinality(bucket) + " <> " + set.size());
                assertThat((double) e.cardinality(bucket), closeTo(set.size(), 0.1 * set.size()));
            }
        }
        assertThat((double) e.cardinality(bucket), closeTo(set.size(), 0.1 * set.size()));
    }

    @Test
    public void merge() {
        final int p = randomIntBetween(MIN_PRECISION, MAX_PRECISION);
        final HyperLogLogPlusPlus single = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 0);
        final HyperLogLogPlusPlus[] multi = new HyperLogLogPlusPlus[randomIntBetween(2, 100)];
        final long[] bucketOrds = new long[multi.length];
        for (int i = 0; i < multi.length; ++i) {
            bucketOrds[i] = randomInt(20);
            multi[i] = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 5);
        }
        final int numValues = randomIntBetween(1, 100000);
        final int maxValue = randomIntBetween(1, randomBoolean() ? 1000: 1000000);
        for (int i = 0; i < numValues; ++i) {
            final int n = randomInt(maxValue);
            final long hash = MurmurHash3.hash((long) n);
            single.collect(0, hash);
            // use a gaussian so that all instances don't collect as many hashes
            final int index = (int) (Math.pow(randomDouble(), 2));
            multi[index].collect(bucketOrds[index], hash);
            if (randomInt(100) == 0) {
                HyperLogLogPlusPlus merged = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 0);
                for (int j = 0; j < multi.length; ++j) {
                    merged.merge(multi[j], bucketOrds[j], 0);
                }
                assertEquals(single.cardinality(0), merged.cardinality(0));
            }
        }
    }

}
