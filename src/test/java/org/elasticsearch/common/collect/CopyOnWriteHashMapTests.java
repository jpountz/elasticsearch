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

package org.elasticsearch.common.collect;

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.collect.Iterables;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CopyOnWriteHashMapTests extends ElasticsearchTestCase {

    private static class O {

        private final int value, hashCode;

        O(int value, int hashCode) {
            super();
            this.value = value;
            this.hashCode = hashCode;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || !(obj instanceof O)) {
                return false;
            }
            return value == ((O) obj).value;
        }
    }

    private static <K, V> void assertEquals(Map<K, V> map1, CopyOnWriteHashMap<K, V> map2) {
        final Set<Map.Entry<K, V>> set2 = new HashSet<>();
        Iterables.addAll(set2, map2.entrySet());
        assertEquals(map1.entrySet(), set2);

        for (Map.Entry<K, V> entry : map1.entrySet()) {
            assertEquals(entry.getValue(), map2.get(entry.getKey()));
        }
    }

    public void testDuel() {
        final int iters = scaledRandomIntBetween(2, 5);
        for (int iter = 0; iter < iters; ++iter) {
            final int numOps = randomInt(5000);
            final int valueBits = randomIntBetween(1, 30);
            final int hashBits = randomInt(valueBits);

            Map<O, Integer> ref = new HashMap<>();
            CopyOnWriteHashMap<O, Integer> map = new CopyOnWriteHashMap<>();
            assertEquals(ref, map);
            final int hashBase = randomInt();
            for (int i = 0; i < numOps; ++i) {
                final int v = randomInt(1 << valueBits);
                final int h = (v & ((1 << hashBits) - 1)) ^ hashBase;
                O key = new O(v, h);

                Map<O, Integer> newRef = new HashMap<>(ref);
                final CopyOnWriteHashMap<O, Integer> newMap;

                if (randomBoolean()) {
                    // ADD
                    Integer value = v;
                    newRef.put(key, value);
                    newMap = map.put(key, value);
                } else {
                    // REMOVE
                    final Integer removed = newRef.remove(key);
                    newMap = map.remove(key);
                    if (removed == null) {
                        assertSame(map, newMap);
                    }
                }

                assertEquals(ref, map); // make sure that the old copy has not been modified
                assertEquals(newRef, newMap);
                assertEquals(ref.isEmpty(), map.isEmpty());
                assertEquals(newRef.isEmpty(), newMap.isEmpty());
                assertEquals(ref.size(), map.size());
                assertEquals(newRef.size(), newMap.size());

                ref = newRef;
                map = newMap;
            }
        }
    }

}
