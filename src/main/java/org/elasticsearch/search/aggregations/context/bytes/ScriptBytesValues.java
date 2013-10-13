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
import org.elasticsearch.search.aggregations.context.ScriptValues;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class ScriptBytesValues extends BytesValues implements ScriptValues {

    final SearchScript script;
    final InternalIter iter;
    final Iter.Single singleIter = new Iter.Single();

    private int docId = -1;
    private Object value;
    private BytesRef scratch = new BytesRef();

    public ScriptBytesValues(SearchScript script) {
        this(script, true);
    }

    public ScriptBytesValues(SearchScript script, boolean multiValue) {
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
            return Array.getLength(value) > 0;
        }
        if (value instanceof List) {
            return !((List) value).isEmpty();
        }
        if (value instanceof Iterator) {
            return ((Iterator) value).hasNext();
        }
        return true;
    }

    @Override
    public BytesRef getValueScratch(int docId, BytesRef ret) {
        if (this.docId != docId) {
            this.docId = docId;
            script.setNextDocId(docId);
            value = script.run();
        }

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
        if (value instanceof Iterator) {
            ret.copyChars(((Iterator) value).next().toString());
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
    public Iter getIter(int docId) {
        if (this.docId != docId) {
            this.docId = docId;
            script.setNextDocId(docId);
            value = script.run();
        }

        // shortcutting single valued
        if (!isMultiValued()) {
            scratch.copyChars(value.toString());
            singleIter.reset(scratch, 0);
            return singleIter;
        }

        if (value.getClass().isArray()) {
            iter.reset(value);
            return iter;
        }
        if (value instanceof List) {
            iter.reset(((List<Long>) value).iterator());
            return iter;
        }
        if (value instanceof Iterator) {
            iter.reset((Iterator<Long>) value);
            return iter;
        }

        scratch.copyChars(value.toString());
        singleIter.reset(scratch, 0);
        return singleIter;
    }

    static class InternalIter implements Iter {

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

        @Override
        public boolean hasNext() {
            if (iterator != null) {
                return iterator.hasNext();
            }
            return i + 1 < arrayLength;
        }

        @Override
        public BytesRef next() {
            if (iterator != null) {
                scratch.copyChars(iterator.next().toString());
                return scratch;
            }
            scratch.copyChars(Array.get(array, ++i).toString());
            return scratch;
        }

        @Override
        public int hash() {
            return scratch.hashCode();
        }
    }
}
