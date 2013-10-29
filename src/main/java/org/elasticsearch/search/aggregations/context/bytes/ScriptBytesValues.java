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

package org.elasticsearch.search.aggregations.context.bytes;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.context.ScriptValues;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class ScriptBytesValues extends BytesValues implements ScriptValues {

    final SearchScript script;

    final Iter iter = new Iter();
    private Object value;
    private BytesRef scratch = new BytesRef();

    public ScriptBytesValues(SearchScript script, boolean multiValue) {
        super(multiValue);
        this.script = script;
    }

    @Override
    public SearchScript script() {
        return script;
    }

    @Override
    public void clearCache() {
        value = null;
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

    public BytesRef getValueScratch(int docId, BytesRef ret) {
        script.setNextDocId(docId);
        Object value = script.run();
        assert value != null : "expected value to exists. call ScriptDoubleValues#hasValue(int) must be called before ScriptDoubleValues#getValue(int)";

        // shortcutting single valued
        if (!isMultiValued()) {
            ret.copyChars(value.toString());
            return ret;
        }

        if (value.getClass().isArray()) {
            ret.copyChars(Array.get(value, 0).toString());
            return ret;
        }
        if (value instanceof List) {
            ret.copyChars(((List) value).get(0).toString());
            return ret;
        }
        if (value instanceof Collection) {
            ret.copyChars(((Collection) value).iterator().next().toString());
            return ret;
        }
        ret.copyChars(value.toString());
        return ret;
    }

    @Override
    public BytesRef getValue(int docId) {
        return getValueScratch(docId, scratch);
    }

    @Override
    public int setDocument(int docId) {
        if (this.docId != docId) {
            this.docId = docId;
            script.setNextDocId(docId);
            value = script.run();
        }

        if (value == null) {
            return 0;
        }

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
    public BytesRef nextValue() {
        if (!isMultiValued()) {
            scratch.copyChars(value.toString());
            return scratch;
        }
        return iter.next();
    }

    static class Iter {

        Object array;
        int arrayLength;
        int i = 0;

        Iterator iterator;

        final BytesRef scratch = new BytesRef();

        void reset(Object array) {
            this.array = array;
            this.arrayLength = Array.getLength(array);
            this.iterator = null;
        }

        void reset(Iterator iterator) {
            this.iterator = iterator;
            this.array = null;
        }

        public boolean hasNext() {
            if (iterator != null) {
                return iterator.hasNext();
            }
            return i + 1 < arrayLength;
        }

        public BytesRef next() {
            if (iterator != null) {
                scratch.copyChars(iterator.next().toString());
                return scratch;
            }
            scratch.copyChars(Array.get(array, ++i).toString());
            return scratch;
        }

        public int hash() {
            return scratch.hashCode();
        }
    }
}
