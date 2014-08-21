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

import java.util.Iterator;
import java.util.Set;

/**
 * Copy-on-write set implementation based on {@link CopyOnWriteHashMap}.
 */
public class CopyOnWriteHashSet<T> implements Iterable<T> {

    private final CopyOnWriteHashMap<T, Boolean> map;

    public CopyOnWriteHashSet() {
        this(new CopyOnWriteHashMap<T, Boolean>());
    }

    private CopyOnWriteHashSet(CopyOnWriteHashMap<T, Boolean> map) {
        this.map = map;
    }

    /**
     * Return whether the given entry is in the set.
     */
    public boolean contains(T entry) {
        return map.get(entry) != null;
    }

    /**
     * Add an entry to the set. The current set is not modified.
     */
    public CopyOnWriteHashSet<T> add(T entry) {
        return new CopyOnWriteHashSet<>(map.put(entry, true));
    }

    /**
     * Remove an entry to the set. The current set is not modified.
     */
    public CopyOnWriteHashSet<T> remove(T entry) {
        final CopyOnWriteHashMap<T, Boolean> updated = map.remove(entry);
        if (updated == map) {
            return this;
        } else {
            return new CopyOnWriteHashSet<>(updated);
        }
    }

    @Override
    public Iterator<T> iterator() {
        return map.keySet().iterator();
    }

    /**
     * Return the number of elements in this set.
     */
    public int size() {
        return map.size();
    }

    /**
     * Return whether this set contains no entries.
     */
    public boolean isEmpty() {
        return map.isEmpty();
    }

    /**
     * Return a view over the content of this set as a {@link Set}.
     */
    public Set<T> asSet() {
        return map.keySet();
    }

}
