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
import org.elasticsearch.search.aggregations.context.ScriptValues;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.List;

/**
 * {@link DoubleValues} implementation which is based on a script
 */
public class ScriptDoubleValues extends DoubleValues implements ScriptValues {

    final SearchScript script;
    final InternalIter iter;

    private int docId = -1;
    private Object value;

    public ScriptDoubleValues(SearchScript script) {
        this(script, true);
    }

    public ScriptDoubleValues(SearchScript script, boolean multiValue) {
        super(multiValue);
        this.script = script;
        this.iter = new InternalIter();
    }

    @Override
    public void clearCache() {
        docId = -1;
        value = null;
    }

    @Override
    public SearchScript script() {
        return script;
    }

    @Override
    public boolean hasValue(int docId) {
        if (this.docId != docId) {
            this.docId = docId;
            script.setNextDocId(docId);
            value = script.run();
        }
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
        if (value instanceof List) {
            return !((List) value).isEmpty();
        }
        if (value instanceof Iterator) {
            return ((Iterator<Number>) value).hasNext();
        }

        return true;
    }

    @Override
    public double getValue(int docId) {
        if (this.docId != docId) {
            this.docId = docId;
            script.setNextDocId(docId);
            value = script.run();
        }

        // shortcutting on single valued
        if (!isMultiValued()) {
            return ((Number) value).doubleValue();
        }

        if (value.getClass().isArray()) {
            return ((Number) Array.get(value, 0)).doubleValue();
        }
        if (value instanceof List) {
            return ((Number) ((List) value).get(0)).doubleValue();
        }
        if (value instanceof Iterator) {
            return ((Iterator<Number>) value).next().doubleValue();
        }
        return ((Number) value).doubleValue();
    }

    @Override
    public Iter getIter(int docId) {
        if (this.docId != docId) {
            this.docId = docId;
            script.setNextDocId(docId);
            value = script.run();
        }

        // shortcutting on single valued
        if (!isMultiValued()) {
            return super.getIter(docId);
        }

        if (value.getClass().isArray()) {
            iter.reset(value);
            return iter;
        }
        if (value instanceof List) {
            iter.reset(((List<Number>) value).iterator());
            return iter;
        }
        if (value instanceof Iterator) {
            iter.reset((Iterator<Number>) value);
            return iter;
        }

        // falling back to single value iterator
        return super.getIter(docId);
    }

    static class InternalIter implements Iter {

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

        @Override
        public boolean hasNext() {
            if (iterator != null) {
                return iterator.hasNext();
            }
            return i < arrayLength;
        }

        @Override
        public double next() {
            if (iterator != null) {
                return iterator.next().doubleValue();
            }
            return ((Number) Array.get(array, i++)).doubleValue();
        }
    }
}
