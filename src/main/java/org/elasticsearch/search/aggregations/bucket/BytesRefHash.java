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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.MonotonicAppendingLongBuffer;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;

/**
 *  Specialized hash table implementation similar to Lucene's BytesRefHash that maps
 *  BytesRef values to ids. Collisions are resolved with open addressing and linear
 *  probing, growth is smooth thanks to {@link BigArrays}, hashes are cached for faster
 *  re-hashing and capacity is always a multiple of 2 for faster identification of buckets.
 *  This class is not thread-safe.
 */
public final class BytesRefHash extends AbstractHash {

    private final PagedBytes bytes;
    private final PagedBytes.PagedBytesDataInput bytesIn;
    private final MonotonicAppendingLongBuffer startOffsets;
    private IntArray hashes; // we cache hashes for faster re-hashing

    // Constructor with configurable capacity and default maximum load factor.
    public BytesRefHash(long capacity) {
        this(capacity, DEFAULT_MAX_LOAD_FACTOR);
    }

    //Constructor with configurable capacity and load factor.
    public BytesRefHash(long capacity, float maxLoadFactor) {
        super(capacity, maxLoadFactor);
        bytes = new PagedBytes(14);
        bytesIn = bytes.getDataInput();
        startOffsets = new MonotonicAppendingLongBuffer(PackedInts.FAST);
        hashes = BigArrays.newIntArray(capacity());
    }

    // BytesRef has a weak hashCode function so we try to improve it by rehashing using Murmur3
    // Feel free to remove rehashing if BytesRef gets a better hash function
    private static int rehash(int hash) {
        return MurmurHash3.hash(hash);
    }

    /**
     * Return the key at <code>0 &lte; index &lte; capacity()</code>. The result is undefined if the slot is unused.
     */
    public void key(long id, BytesRef dest) {
        final long startOffset = startOffsets.get(id);
        bytesIn.setPosition(startOffset);
        final int len = bytesIn.readVInt();
    }

    /**
     * Get the id associated with <code>key</code>
     */
    public long get(BytesRef key, int code) {
        final long slot = slot(rehash(code), mask);
        for (long index = slot; ; index = nextSlot(index, mask)) {
            final long id = ids.get(index);
            if (id == 0L || keys.get(index) == key) {
                return id - 1;
            }
        }
    }

    /** Make the key safe for internal storage. */
    protected BytesRef makeSafe(BytesRef key) {
        return key;
    }

    private long set(BytesRef key, int code, long id) {
        assert key.hashCode() == code;
        assert size < maxSize;
        final long slot = slot(rehash(code), mask);
        for (long index = slot; ; index = nextSlot(index, mask)) {
            final long curId = ids.get(index);
            if (curId == 0) { // means unset
                ids.set(index, id + 1);
                keys.set(index, makeSafe(key));
                hashes.set(index, code);
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
    public long add(BytesRef key, int code) {
        if (size >= maxSize) {
            assert size == maxSize;
            grow();
        }
        assert size < maxSize;
        return set(key, code, size);
    }

    @Override
    protected void resizeKeys(long capacity) {
        keys = BigArrays.resize(keys, capacity);
        hashes = BigArrays.resize(hashes, capacity);
    }

    @Override
    protected long removeAndAdd(long index, long id) {
        final BytesRef key = keys.set(index, null);
        final int code = hashes.set(index, 0);
        final long newId = set(key, code, id);
        return newId;
    }

}
