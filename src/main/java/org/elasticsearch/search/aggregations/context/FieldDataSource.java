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

package org.elasticsearch.search.aggregations.context;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.ReaderContextAware;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.script.SearchScript;

/**
 *
 */
public abstract class FieldDataSource implements ReaderContextAware {

    protected final String field;
    protected final IndexFieldData<?> indexFieldData;
    protected final boolean needsHashes;
    protected AtomicFieldData<?> fieldData;
    protected BytesValues bytesValues;

    public FieldDataSource(String field, IndexFieldData<?> indexFieldData, boolean needsHashes) {
        this.field = field;
        this.indexFieldData = indexFieldData;
        this.needsHashes = needsHashes;
    }

    public void setNextReader(AtomicReaderContext reader) {
        fieldData = indexFieldData.load(reader);
        if (bytesValues != null) {
            bytesValues = fieldData.getBytesValues(needsHashes);
        }
    }

    public String field() {
        return field;
    }

    public BytesValues bytesValues() {
        if (bytesValues == null) {
            bytesValues = fieldData.getBytesValues(needsHashes);
        }
        return bytesValues;
    }

    public static class WithScript extends FieldDataSource {

        private final FieldDataSource delegate;
        private final BytesValues bytesValues;

        public WithScript(FieldDataSource delegate, SearchScript script) {
            super(null, null, false); // hashes can't be cached with scripts anyway
            this.delegate = delegate;
            this.bytesValues = new BytesValues(delegate, script);

        }

        @Override
        public void setNextReader(AtomicReaderContext reader) {
            // no need to do anything... already taken care of by the delegate
        }

        @Override
        public String field() {
            return delegate.field();
        }

        @Override
        public BytesValues bytesValues() {
            return bytesValues;
        }

        static class BytesValues extends org.elasticsearch.index.fielddata.BytesValues {

            private final FieldDataSource source;
            private final SearchScript script;
            private final BytesRef scratch;

            public BytesValues(FieldDataSource source, SearchScript script) {
                super(true);
                this.source = source;
                this.script = script;
                scratch = new BytesRef();
            }

            @Override
            public int setDocument(int docId) {
                return source.bytesValues().setDocument(docId);
            }

            @Override
            public BytesRef nextValue() {
                BytesRef value = source.bytesValues().nextValue();
                script.setNextVar("_value", value.utf8ToString());
                scratch.copyChars(script.run().toString());
                return scratch;
            }
        }
    }

    public static class Numeric extends FieldDataSource {

        private DoubleValues doubleValues;
        private LongValues longValues;

        public Numeric(String field, IndexFieldData<?> indexFieldData) {
            super(field, indexFieldData, false); // don't cache hashes with numerics, they can be hashed very quickly
        }

        @Override
        public void setNextReader(AtomicReaderContext reader) {
            super.setNextReader(reader);
            if (doubleValues != null) {
                doubleValues = ((AtomicNumericFieldData) fieldData).getDoubleValues();
            }
            if (longValues != null) {
                longValues = ((AtomicNumericFieldData) fieldData).getLongValues();
            }
        }

        public DoubleValues doubleValues() {
            if (doubleValues == null) {
                doubleValues = ((AtomicNumericFieldData) fieldData).getDoubleValues();
            }
            return doubleValues;
        }

        public LongValues longValues() {
            if (longValues == null) {
                longValues = ((AtomicNumericFieldData) fieldData).getLongValues();
            }
            return longValues;
        }

        public boolean isFloatingPoint() {
            return ((IndexNumericFieldData<?>) indexFieldData).getNumericType().isFloatingPoint();
        }

        public static class WithScript extends Numeric {

            private final Numeric delegate;
            private final LongValues longValues;
            private final DoubleValues doubleValues;
            private final FieldDataSource.WithScript.BytesValues bytesValues;

            public WithScript(Numeric delegate, SearchScript script) {
                super(null, null);
                this.delegate = delegate;
                this.longValues = new LongValues(delegate, script);
                this.doubleValues = new DoubleValues(delegate, script);
                this.bytesValues = new FieldDataSource.WithScript.BytesValues(delegate, script);

            }

            @Override
            public boolean isFloatingPoint() {
                return delegate.isFloatingPoint();
            }

            @Override
            public void setNextReader(AtomicReaderContext reader) {
                // no need to do anything... already taken care of by the delegate
            }

            @Override
            public String field() {
                return delegate.field();
            }

            @Override
            public BytesValues bytesValues() {
                return bytesValues;
            }

            @Override
            public LongValues longValues() {
                return longValues;
            }

            @Override
            public DoubleValues doubleValues() {
                return doubleValues;
            }

            static class LongValues extends org.elasticsearch.index.fielddata.LongValues {

                private final Numeric source;
                private final SearchScript script;

                public LongValues(Numeric source, SearchScript script) {
                    super(true);
                    this.source = source;
                    this.script = script;
                }

                @Override
                public int setDocument(int docId) {
                    return source.longValues().setDocument(docId);
                }

                @Override
                public long nextValue() {
                    script.setNextVar("_value", source.longValues().nextValue());
                    return script.runAsLong();
                }
            }

            static class DoubleValues extends org.elasticsearch.index.fielddata.DoubleValues {

                private final Numeric source;
                private final SearchScript script;

                public DoubleValues(Numeric source, SearchScript script) {
                    super(true);
                    this.source = source;
                    this.script = script;
                }

                @Override
                public int setDocument(int docId) {
                    return source.doubleValues().setDocument(docId);
                }

                @Override
                public double nextValue() {
                    script.setNextVar("_value", source.doubleValues().nextValue());
                    return script.runAsDouble();
                }
            }
        }

    }

    public static class Bytes extends FieldDataSource {

        public Bytes(String field, IndexFieldData<?> indexFieldData, boolean needsHashes) {
            super(field, indexFieldData, needsHashes);
        }

    }

    public static class GeoPoint extends FieldDataSource {

        private GeoPointValues geoPointValues;

        public GeoPoint(String field, IndexFieldData<?> indexFieldData) {
            super(field, indexFieldData, false); // geo points are useful for distances, no hashes needed
        }

        @Override
        public void setNextReader(AtomicReaderContext reader) {
            super.setNextReader(reader);
            if (geoPointValues != null) {
                geoPointValues = ((AtomicGeoPointFieldData<?>) fieldData).getGeoPointValues();
            }
        }

        public GeoPointValues geoPointValues() {
            if (geoPointValues == null) {
                geoPointValues = ((AtomicGeoPointFieldData<?>) fieldData).getGeoPointValues();
            }
            return geoPointValues;
        }
    }

}
