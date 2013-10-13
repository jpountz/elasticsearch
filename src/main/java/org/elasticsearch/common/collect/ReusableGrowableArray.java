/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.collect;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

import java.lang.reflect.Array;

/**
 * A simple reusable list/array which enables adding elements and provides random access to these elements. A single instance is reusable,
 * by calling {@link #reset()}, which will not clear the underlying array, but only reset its current size.
 *
 * THis class is <strong>not</strong> thread safe!
 */
public class ReusableGrowableArray<T> {

    private final Class<T> elementType;

    private T[] values;

    private int size = 0;

    public ReusableGrowableArray(Class<T> elementType) {
        this(elementType, 10);
    }

    public ReusableGrowableArray(Class<T> elementType, int capacity) {
        this.elementType = elementType;
        values = (T[]) Array.newInstance(elementType, capacity);
    }

    public void reset() {
        size = 0;
    }

    public void clear() {
        for (int i = 0; i < values.length; i++) {
            values[i] = null;
        }
        size = 0;
    }

    public T[] innerValues() {
        return values;
    }

    public void add(T t) {
        ensureCapacity(size+1);
        values[size++] = t;
    }

    public T get(int i) {
        if (i >= size) {
            throw new IndexOutOfBoundsException();
        }
        return (T) values[i];
    }

    public int size() {
        return size;
    }

    private void ensureCapacity(int capacity) {
        if (values.length < capacity) {
            T[] newArray = (T[]) Array.newInstance(elementType, ArrayUtil.oversize(capacity, RamUsageEstimator.NUM_BYTES_INT));
            System.arraycopy(values, 0, newArray, 0, values.length );
            values = newArray;
        }
    }
}
