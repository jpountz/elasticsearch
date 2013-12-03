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

package org.elasticsearch.search.aggregations.bucket;

import com.carrotsearch.hppc.hash.MurmurHash3;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;

/**
 * Specialized hash table implementation similar to BytesRefHash that maps
 *  long values to ids. Collisions are resolved with open addressing and linear
 *  probing, growth is smooth thanks to {@link BigArrays} and capacity is always
 *  a multiple of 2 for faster identification of buckets.
 */
// IDs are internally stored as id + 1 so that 0 encodes for an empty slot
public final class LongHash extends AbstractHash {

    private LongArray keys;

    // Constructor with configurable capacity and default maximum load factor.
    public LongHash(long capacity) {
        this(capacity, DEFAULT_MAX_LOAD_FACTOR);
    }

    //Constructor with configurable capacity and load factor.
    public LongHash(long capacity, float maxLoadFactor) {
        super(capacity, maxLoadFactor);
        keys = BigArrays.newLongArray(capacity());
    }

    private static long hash(long value) {
        // Don't use the value directly. Under some cases eg dates, it could be that the low bits don't carry much value and we would like
        // all bits of the hash to carry as much value
        return MurmurHash3.hash(value);
    }

    /**
     * Return the key at <code>0 &lte; index &lte; capacity()</code>. The result is undefined if the slot is unused.
     */
    public long key(long index) {
        return keys.get(index);
    }

    /**
     * Get the id associated with <code>key</code>
     */
    public long get(long key) {
        final long slot = slot(hash(key), mask);
        for (long index = slot; ; index = nextSlot(index, mask)) {
            final long id = ids.get(index);
            if (id == 0L || keys.get(index) == key) {
                return id - 1;
            }
        }
    }

    private long set(long key, long id) {
        assert size < maxSize;
        final long slot = slot(hash(key), mask);
        for (long index = slot; ; index = nextSlot(index, mask)) {
            final long curId = ids.get(index);
            if (curId == 0) { // means unset
                ids.set(index, id + 1);
                keys.set(index, key);
                ++size;
                return id;
            } else if (keys.get(index) == key) {
                return - curId;
            }
        }
    }

    /**
     * Try to add <code>key</code>. Return its newly allocated id if it wasn't in the hash table yet, or </code>-1-id</code>
     * if it was already present in the hash table.
     */
    public long add(long key) {
        if (size >= maxSize) {
            assert size == maxSize;
            grow();
        }
        assert size < maxSize;
        return set(key, size);
    }

    @Override
    protected void resizeKeys(long capacity) {
        keys = BigArrays.resize(keys, capacity);
    }

    @Override
    protected long removeAndAdd(long index, long id) {
        final long key = keys.set(index, 0);
        final long newId = set(key, id);
        return newId;
    }
    
}
