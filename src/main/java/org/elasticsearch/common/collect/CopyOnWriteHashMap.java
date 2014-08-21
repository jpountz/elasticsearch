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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.UnmodifiableIterator;
import org.elasticsearch.common.Preconditions;

import java.lang.reflect.Array;
import java.util.*;

/**
 * An immutable map whose writes result in a new copy of the map to be created.
 *
 * This is essentially a hash array mapped trie: inner nodes use a bitmap in
 * order to map hashes to slots by counting ones. In case of a collision (two
 * values having the same 32-bits hash), a leaf node is created which stores
 * and searches for values sequentially.
 *
 * Reads and writes both perform in logarithmic time. Null keys and values are
 * not supported.
 *
 * @see <a href="http://en.wikipedia.org/wiki/Hash_array_mapped_trie">the wikipedia page</a>
 */
public final class CopyOnWriteHashMap<K, V> {

    private static final int TOTAL_HASH_BITS = 32;
    private static final Object[] EMPTY_ARRAY = new Object[0];

    private static final int HASH_BITS = 6;
    private static final int HASH_MASK = 0x3F;

    /**
     * Insert <code>o</code> into <code>array</code> at <code>pos</code>.
     */
    private static <T> T[] insertAt(T[] array, int pos, T o) {
        @SuppressWarnings("unchecked")
        final T[] result = (T[]) Array.newInstance(array.getClass().getComponentType(), array.length + 1);
        System.arraycopy(array, 0, result, 0, pos);
        result[pos] = o;
        System.arraycopy(array, pos, result, pos + 1, array.length - pos);
        return result;
    }

    /**
     * Remove the entry at position <code>pos</code> from <code>array</code>.
     */
    private static <T> T[] removeAt(T[] array, int pos) {
        @SuppressWarnings("unchecked")
        final T[] result = (T[]) Array.newInstance(array.getClass().getComponentType(), array.length - 1);
        System.arraycopy(array, 0, result, 0, pos);
        System.arraycopy(array, pos + 1, result, pos, result.length - pos);
        return result;
    }

    /**
     * Abstraction of a node, implemented by both inner and leaf nodes.
     */
    private static abstract class Node<K, V> {

        /**
         * Recursively get the key with the given hash.
         */
        abstract V get(K key, int hash);

        /**
         * Recursively add a new entry to this node. <code>hashBits</code> is
         * the number of bits that are still set in the hash. When this value
         * reaches a number that is less than or equal to <tt>0</tt>, a leaf
         * node needs to be created since it means that a collision occurred.
         */
        abstract Node<K, V> put(K key, int hash, int hashBits, V value);

        /**
         * Recursively remove an entry from this node.
         */
        abstract Node<K, V> remove(K key, int hash);

        /**
         * For the current node only, append entries that are stored on this
         * node to <code>entries</code> and sub nodes to <code>nodes</code>.
         */
        abstract void visit(Deque<Map.Entry<K, V>> entries, Deque<Node<K, V>> nodes);

        /**
         * Whether this node stores nothing under it.
         */
        abstract boolean isEmpty();

    }

    /**
     * An entry in this hash map. Only used at iteration time.
     */
    private static class Entry<K, V> implements Map.Entry<K, V> {

        private final K key;
        private final V value;

        Entry(K key, V value) {
            super();
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || !(obj instanceof Map.Entry)) {
                return false;
            }
            Map.Entry<?, ?> other = (java.util.Map.Entry<?, ?>) obj;
            return Objects.equals(key, other.getKey()) && Objects.equals(value, other.getValue());
        }

        @Override
        public int hashCode() {
            return 31 * key.hashCode() + value.hashCode();
        }

    }

    /**
     * A leaf of the tree where all hashes are equal. Values are added and retrieved in linear time.
     */
    private static class Leaf<K, V> extends Node<K, V> {

        private final K[] keys;
        private final V[] values;

        Leaf(K[] keys, V[] values) {
            this.keys = keys;
            this.values = values;
        }

        @SuppressWarnings("unchecked")
        Leaf() {
            this((K[]) EMPTY_ARRAY, (V[]) EMPTY_ARRAY);
        }

        @Override
        boolean isEmpty() {
            return keys.length == 0;
        }

        @Override
        void visit(Deque<Map.Entry<K, V>> entries, Deque<Node<K, V>> nodes) {
            for (int i = 0; i < keys.length; ++i) {
                entries.add(new Entry<K, V>(keys[i], values[i]));
            }
        }

        private int slot(Object key) {
            for (int i = 0; i < keys.length; ++i) {
                if (keys[i].equals(key)) {
                    return i;
                }
            }
            return -1;
        }

        @Override
        V get(K key, int hash) {
            final int slot = slot(key);
            if (slot < 0) {
                return null;
            } else {
                return values[slot];
            }
        }

        @Override
        Leaf<K, V> put(K key, int hash, int hashBits, V value) {
            assert hashBits <= 0 : hashBits;
            final int slot = slot(key);

            final K[] keys2;
            final V[] values2;
            final int index, newLength;
            if (slot < 0) {
                // append
                newLength = keys.length + 1;
                index = newLength - 1;
            } else {
                // replace
                newLength = keys.length;
                index = slot;
            }
            keys2 = Arrays.copyOf(keys, newLength);
            values2 = Arrays.copyOf(values, newLength);
            keys2[index] = key;
            values2[index] = value;
            return new Leaf<>(keys2, values2);
        }

        @Override
        Leaf<K, V> remove(K key, int hash) {
            final int slot = slot(key);
            if (slot < 0) {
                return this;
            }
            final K[] keys2 = removeAt(keys, slot);
            final V[] values2 = removeAt(values, slot);
            return new Leaf<>(keys2, values2);
        }
    }

    /**
     * An inner node in this trie. Inner nodes store up to 64 key-value pairs
     * and use a bitmap in order to associate hashes to them. For example, if
     * an inner node contains 5 values, then 5 bits will be set in the bitmap
     * and the ordinal of the bit set in this bit map will be the slot number.
     *
     * As a consequence, the number of slots in an inner node is equal to the
     * number of one bits in the bitmap.
     */
    private static class InnerNode<K, V> extends Node<K, V> {

        private final long mask; // the bitmap
        private final K[] keys;
        final Object[] subNodes; // subNodes[slot] is either a value or a sub node in case of a hash collision

        InnerNode(long mask, K[] keys, Object[] subNodes) {
            this.mask = mask;
            this.keys = keys;
            this.subNodes = subNodes;
            assert consistent();
        }

        private boolean consistent() {
            assert Long.bitCount(mask) == keys.length;
            assert Long.bitCount(mask) == subNodes.length;
            for (int i = 0; i < keys.length; ++i) {
                if (subNodes[i] instanceof Node) {
                    assert keys[i] == null;
                } else {
                    assert keys[i] != null;
                }
            }
            return true;
        }

        @Override
        boolean isEmpty() {
            return mask == 0;
        }

        @SuppressWarnings("unchecked")
        InnerNode() {
            this(0, (K[]) EMPTY_ARRAY, EMPTY_ARRAY);
        }

        @Override
        void visit(Deque<Map.Entry<K, V>> entries, Deque<Node<K, V>> nodes) {
            for (int i = 0; i < keys.length; ++i) {
                final Object sub = subNodes[i];
                if (sub instanceof Node) {
                    @SuppressWarnings("unchecked")
                    final Node<K, V> subNode = (Node<K, V>) sub;
                    assert keys[i] == null;
                    nodes.add(subNode);
                } else {
                    @SuppressWarnings("unchecked")
                    final V value = (V) sub;
                    entries.add(new Entry<K, V>(keys[i], value));
                }
            }
        }

        /**
         * For a given hash on 6 bits, its value is set if the bitmap has a one
         * at the corresponding index.
         */
        private boolean exists(int hash6) {
            return (mask & (1L << hash6)) != 0;
        }

        /**
         * For a given hash on 6 bits, the slot number is the number of one
         * bits on the right of <code>hash6</code>.
         */
        private int slot(int hash6) {
            return Long.bitCount(mask & ((1L << hash6) - 1));
        }

        @Override
        V get(K key, int hash) {
            final int hash6 = hash & HASH_MASK;
            if (!exists(hash6)) {
                return null;
            }
            final int slot = slot(hash6);
            final Object sub = subNodes[slot];
            assert sub != null;
            if (sub instanceof Node) {
                assert keys[slot] == null; // keys don't make sense on inner nodes
                @SuppressWarnings("unchecked")
                final Node<K, V> subNode = (Node<K, V>) sub;
                return subNode.get(key, hash >>> HASH_BITS);
            } else {
                if (keys[slot].equals(key)) {
                    @SuppressWarnings("unchecked")
                    final V v = (V) sub;
                    return v;
                } else {
                    // we have an entry for this hash, but the value is different
                    return null;
                }
            }
        }

        private Node<K, V> newSubNode(int hashBits) {
            if (hashBits <= 0) {
                return new Leaf<K, V>();
            } else {
                return new InnerNode<K, V>();
            }
        }

        private InnerNode<K, V> putExisting(K key, int hash, int hashBits, int slot, V value) {
            final K[] keys2 = Arrays.copyOf(keys, keys.length);
            final Object[] subNodes2 = Arrays.copyOf(subNodes, subNodes.length);

            final Object previousValue = subNodes2[slot];
            if (previousValue instanceof Node) {
                // insert recursively
                assert keys[slot] == null;
                subNodes2[slot] = ((Node<K, V>) previousValue).put(key, hash, hashBits, value);
            } else if (keys[slot].equals(key)) {
                // replace the existing entry
                subNodes2[slot] = value;
            } else {
                // hash collision
                final K previousKey = keys[slot];
                final int previousHash = previousKey.hashCode() >>> (TOTAL_HASH_BITS - hashBits);
                Node<K, V> subNode = newSubNode(hashBits);
                subNode = subNode.put(previousKey, previousHash, hashBits, (V) previousValue);
                subNode = subNode.put(key, hash, hashBits, value);
                keys2[slot] = null;
                subNodes2[slot] = subNode;
            }
            return new InnerNode<>(mask, keys2, subNodes2);
        }

        private InnerNode<K, V> putNew(K key, int hash6, int slot, V value) {
            final long mask2 = mask | (1L << hash6);
            final K[] keys2 = insertAt(keys, slot, key);
            final Object[] subNodes2 = insertAt(subNodes, slot, value);
            return new InnerNode<>(mask2, keys2, subNodes2);
        }

        @Override
        InnerNode<K, V> put(K key, int hash, int hashBits, V value) {
            final int hash6 = hash & HASH_MASK;
            final int slot = slot(hash6);

            if (exists(hash6)) {
                hash >>>= HASH_BITS;
                hashBits -= HASH_BITS;
                return putExisting(key, hash, hashBits, slot, value);
            } else {
                return putNew(key, hash6, slot, value);
            }
        }

        private InnerNode<K, V> removeSlot(int hash6, int slot) {
            final long mask2 = mask  & ~(1L << hash6);
            final K[] keys2 = removeAt(keys, slot);
            final Object[] subNodes2 = removeAt(subNodes, slot);
            return new InnerNode<>(mask2, keys2, subNodes2);
        }

        @Override
        InnerNode<K, V> remove(K key, int hash) {
            final int hash6 = hash & HASH_MASK;
            if (!exists(hash6)) {
                return this;
            }
            final int slot = slot(hash6);
            final Object previousValue = subNodes[slot];
            if (previousValue instanceof Node) {
                @SuppressWarnings("unchecked")
                final Node<K, V> subNode = (Node<K, V>) previousValue;
                final Node<K, V> removed = subNode.remove(key, hash >>> HASH_BITS);
                if (removed == subNode) {
                    // not in sub-nodes
                    return this;
                }
                if (removed.isEmpty()) {
                    return removeSlot(hash6, slot);
                }
                final K[] keys2 = Arrays.copyOf(keys, keys.length);
                final Object[] subNodes2 = Arrays.copyOf(subNodes, subNodes.length);
                subNodes2[slot] = removed;
                return new InnerNode<>(mask, keys2, subNodes2);
            } else if (keys[slot].equals(key)) {
                // remove entry
                return removeSlot(hash6, slot);
            } else {
                // hash collision, nothing to remove
                return this;
            }
        }

    }

    private static class EntryIterator<K, V> extends UnmodifiableIterator<Map.Entry<K, V>> {

        private final Deque<Map.Entry<K, V>> entries;
        private final Deque<Node<K, V>> nodes;

        public EntryIterator(Node<K, V> node) {
            entries = new ArrayDeque<>();
            nodes = new ArrayDeque<>();
            node.visit(entries, nodes);
        }

        @Override
        public boolean hasNext() {
            return !entries.isEmpty() || !nodes.isEmpty();
        }

        @Override
        public Map.Entry<K, V> next() {
            while (entries.isEmpty()) {
                if (nodes.isEmpty()) {
                    throw new NoSuchElementException();
                }
                final Node<K, V> nextNode = nodes.pop();
                nextNode.visit(entries, nodes);
            }
            return entries.pop();
        }

    }

    private final InnerNode<K, V> root;

    /**
     * Create a new empty map.
     */
    public CopyOnWriteHashMap() {
        this(new InnerNode<K, V>());
    }

    private CopyOnWriteHashMap(InnerNode<K, V> root) {
        this.root = root;
    }

    /**
     * Get the value associated with <code>key</code> or <tt>null</tt> if this
     * key is not in the hash table.
     */
    public V get(K key) {
        final int hash = key.hashCode();
        return root.get(key, hash);
    }

    /**
     * Returns whether this hash map contains no entry.
     */
    public boolean isEmpty() {
        return root.isEmpty();
    }

    /**
     * Associate <code>key</code> with <code>value</code> and return a new copy
     * of the hash table. The current hash table is not modified.
     */
    public CopyOnWriteHashMap<K, V> put(K key, V value) {
        Preconditions.checkArgument(key != null, "null keys are not supported");
        Preconditions.checkArgument(value != null, "null values are not supported");
        final int hash = key.hashCode();
        return new CopyOnWriteHashMap<>(root.put(key, hash, TOTAL_HASH_BITS, value));
    }

    /**
     * Remove the given key from this map. The current hash table is not modified.
     */
    public CopyOnWriteHashMap<K, V> remove(K key) {
        Preconditions.checkArgument(key != null, "null keys are not supported");
        final int hash = key.hashCode();
        final InnerNode<K, V> newRoot = root.remove(key, hash);
        if (root == newRoot) {
            return this;
        } else {
            return new CopyOnWriteHashMap<>(newRoot);
        }
    }

    /**
     * Return an {@link Iterable} over the entries in this hash map.
     */
    public Iterable<Map.Entry<K, V>> entrySet() {
        return new Iterable<Map.Entry<K, V>>() {
            @Override
            public Iterator<java.util.Map.Entry<K, V>> iterator() {
                return new EntryIterator<>(root);
            }
        };
    }

    /**
     * Return an {@link Iterable} over the keys in this hash map.
     */
    public Iterable<K> keySet() {
        return Iterables.transform(entrySet(), new Function<Map.Entry<K, V>, K>() {
            @Override
            public K apply(java.util.Map.Entry<K, V> input) {
                return input.getKey();
            }
        });
    }

    /**
     * Return an {@link Iterable} over the values in this hash map.
     */
    public Iterable<V> values() {
        return Iterables.transform(entrySet(), new Function<Map.Entry<K, V>, V>() {
            @Override
            public V apply(java.util.Map.Entry<K, V> input) {
                return input.getValue();
            }
        });
    }

}
