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

package org.elasticsearch.search.aggregations.context.numeric;

import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.context.ScriptValues;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * {@link DoubleValues} implementation which is based on a script
 */
public class ScriptDoubleValues extends DoubleValues implements ScriptValues {

    final SearchScript script;

    private Object value;
    private final Iter iter = new Iter();


    public ScriptDoubleValues(SearchScript script, boolean multiValue) {
        super(multiValue);
        this.script = script;
    }

    @Override
    public void clearCache() {
        value = null;
    }

    @Override
    public SearchScript script() {
        return script;
    }

    @Override
    public int setDocument(int docId) {
        if (this.docId != docId || value == null) {
            this.docId = docId;
            script.setNextDocId(docId);
            value = script.run();
        }

        if (value == null) {
            return 0;
        }

        // shortcutting on single valued
        if (!isMultiValued()) {
            return 1;
        }

        if (value.getClass().isArray()) {
            iter.reset(value);
            return Array.getLength(value);
        }
        if (value instanceof Collection) {
            iter.reset(((Collection) value).iterator());
            return ((Collection) value).size();
        }

        throw new AggregationExecutionException("Unsupported script value [" + value + "]");
    }

    @Override
    public boolean hasValue(int docId) {
        script.setNextDocId(docId);
        Object value = script.run();

        if (value == null) {
            return false;
        }

        // shortcutting on single valued
        if (!isMultiValued()) {
            return true;
        }

        if (value.getClass().isArray()) {
            return Array.getLength(value) != 0;
        }

        if (value instanceof Collection) {
            return !((Collection) value).isEmpty();
        }

        return false;
    }

    @Override
    public double getValue(int docId) {
        script.setNextDocId(docId);
        Object value = script.run();
        assert value != null : "expected value to exists. call ScriptDoubleValues#hasValue(int) must be called before ScriptDoubleValues#getValue(int)";

        // shortcutting on single valued
        if (!isMultiValued()) {
            return ((Number) value).doubleValue();
        }

        if (value.getClass().isArray()) {
            return ((Number) Array.get(value, 0)).doubleValue();
        }
        if (value instanceof List) {
            return (((List<Number>) value).get(0)).doubleValue();
        }
        if (value instanceof Collection) {
            return (((Collection<Number>) value).iterator().next()).doubleValue();
        }
        return ((Number) value).doubleValue();
    }

    @Override
    public double nextValue() {
        if (!isMultiValued()) {
            return ((Number) value).doubleValue();
        }
        return iter.next();
    }

    static class Iter {

        Object array;
        int arrayLength;
        int i = 0;

        Iterator<Number> iterator;

        void reset(Object array) {
            this.array = array;
            this.i = 0;
            this.arrayLength = Array.getLength(array);
            this.iterator = null;
        }

        void reset(Iterator<Number> iterator) {
            this.iterator = iterator;
            this.array = null;
        }

        public boolean hasNext() {
            if (iterator != null) {
                return iterator.hasNext();
            }
            return i < arrayLength;
        }

        public double next() {
            if (iterator != null) {
                return iterator.next().doubleValue();
            }
            return ((Number) Array.get(array, i++)).doubleValue();
        }
    }
}
