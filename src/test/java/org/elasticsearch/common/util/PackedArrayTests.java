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

import org.apache.lucene.util.TestUtil;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.elasticsearch.test.cache.recycler.MockBigArrays;
import org.junit.Before;

public class PackedArrayTests extends ElasticsearchSingleNodeTest {

    public static BigArrays randombigArrays() {
        final PageCacheRecycler recycler = randomBoolean() ? null : ElasticsearchSingleNodeTest.getInstanceFromNode(PageCacheRecycler.class);
        return new MockBigArrays(ImmutableSettings.EMPTY, recycler, new NoneCircuitBreakerService());
    }

    private BigArrays bigArrays;

    @Before
    public void init() {
        bigArrays = randombigArrays();
    }

    public void testDuel() {
        final int iters = 10;
        for (int i = 0; i < iters; ++i) {
            final long length = TestUtil.nextLong(getRandom(), 1, 1L << getRandom().nextInt(22));
            final int bpv = TestUtil.nextInt(getRandom(), 1, 63);
            final long maxValue = (1L << bpv) - 1;

            final long[] expected = new long[(int) length];
            try (final PackedArray actual = new PackedArray(bigArrays, bpv, length)) {
                for (int j = 0; j < 1000; ++j) {
                    final int index = getRandom().nextInt(expected.length);
                    final long value = TestUtil.nextLong(getRandom(), 0, maxValue);
                    expected[index] = value;
                    actual.set(index, value);
                }
                for (int j = 0; j < length; ++j) {
                    assertEquals(expected[j], actual.get(j));
                }
            }
        }
    }

}
