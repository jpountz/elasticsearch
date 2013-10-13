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
    protected final IndexFieldData indexFieldData;
    protected AtomicFieldData fieldData;
    protected BytesValues bytesValues;

    public FieldDataSource(String field, IndexFieldData indexFieldData) {
        this.field = field;
        this.indexFieldData = indexFieldData;
    }

    public void setNextReader(AtomicReaderContext reader) {
        fieldData = indexFieldData.load(reader);
        if (bytesValues != null) {
            bytesValues = fieldData.getBytesValues();
        }
    }

    public String field() {
        return field;
    }

    public BytesValues bytesValues() {
        if (bytesValues == null) {
            bytesValues = fieldData.getBytesValues();
        }
        return bytesValues;
    }

    public static class WithScript extends FieldDataSource {

        private final FieldDataSource delegate;
        private final BytesValues bytesValues;

        public WithScript(FieldDataSource delegate, SearchScript script) {
            super(null, null);
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
            private final InternalIter iter;

            public BytesValues(FieldDataSource source, SearchScript script) {
                super(true);
                this.source = source;
                this.script = script;
                this.iter = new InternalIter(script);
            }

            @Override
            public boolean hasValue(int docId) {
                return source.bytesValues().hasValue(docId);
            }

            @Override
            public BytesRef getValueScratch(int docId, BytesRef ret) {
                source.bytesValues().getValueScratch(docId, ret);
                script.setNextVar("_value", ret.utf8ToString());
                ret.copyChars(script.run().toString());
                return ret;
            }

            @Override
            public Iter getIter(int docId) {
                this.iter.iter = source.bytesValues().getIter(docId);
                return this.iter;
            }

            static class InternalIter implements Iter {

                private final SearchScript script;
                private Iter iter;
                private final BytesRef scratch = new BytesRef();

                InternalIter(SearchScript script) {
                    this.script = script;
                }

                @Override
                public boolean hasNext() {
                    return iter.hasNext();
                }

                @Override
                public int hash() {
                    return iter.hashCode();
                }

                @Override
                public BytesRef next() {
                    BytesRef scratch = iter.next();
                    script.setNextVar("_value", scratch.utf8ToString());
                    this.scratch.copyChars(script.run().toString());
                    return this.scratch;
                }
            }
        }
    }

    public static class Numeric extends FieldDataSource {

        private DoubleValues doubleValues;
        private LongValues longValues;

        public Numeric(String field, IndexFieldData indexFieldData) {
            super(field, indexFieldData);
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
            return ((IndexNumericFieldData) indexFieldData).getNumericType().isFloatingPoint();
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
                private final InternalIter iter;

                public LongValues(Numeric source, SearchScript script) {
                    super(true);
                    this.source = source;
                    this.script = script;
                    this.iter = new InternalIter(script);
                }

                @Override
                public boolean hasValue(int docId) {
                    return source.longValues().hasValue(docId);
                }

                @Override
                public long getValue(int docId) {
                    script.setNextVar("_value", source.longValues().getValue(docId));
                    return script.runAsLong();
                }

                @Override
                public Iter getIter(int docId) {
                    this.iter.iter = source.longValues().getIter(docId);
                    return this.iter;
                }

                static class InternalIter implements Iter {

                    private final SearchScript script;
                    private Iter iter;

                    InternalIter(SearchScript script) {
                        this.script = script;
                    }

                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public long next() {
                        script.setNextVar("_value", iter.next());
                        return script.runAsLong();
                    }
                }
            }

            static class DoubleValues extends org.elasticsearch.index.fielddata.DoubleValues {

                private final Numeric source;
                private final SearchScript script;
                private final InternalIter iter;

                public DoubleValues(Numeric source, SearchScript script) {
                    super(true);
                    this.source = source;
                    this.script = script;
                    this.iter = new InternalIter(script);
                }

                @Override
                public boolean hasValue(int docId) {
                    return source.doubleValues().hasValue(docId);
                }

                @Override
                public double getValue(int docId) {
                    script.setNextVar("_value", source.doubleValues().getValue(docId));
                    return script.runAsDouble();
                }

                @Override
                public Iter getIter(int docId) {
                    this.iter.iter = source.doubleValues().getIter(docId);
                    return this.iter;
                }

                static class InternalIter implements Iter {

                    private final SearchScript script;
                    private Iter iter;

                    InternalIter(SearchScript script) {
                        this.script = script;
                    }

                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public double next() {
                        script.setNextVar("_value", iter.next());
                        return script.runAsDouble();
                    }
                }
            }
        }

    }

    public static class Bytes extends FieldDataSource {

        public Bytes(String field, IndexFieldData indexFieldData) {
            super(field, indexFieldData);
        }

    }

    public static class GeoPoint extends FieldDataSource {

        private GeoPointValues geoPointValues;

        public GeoPoint(String field, IndexFieldData indexFieldData) {
            super(field, indexFieldData);
        }

        @Override
        public void setNextReader(AtomicReaderContext reader) {
            super.setNextReader(reader);
            if (geoPointValues != null) {
                geoPointValues = ((AtomicGeoPointFieldData) fieldData).getGeoPointValues();
            }
        }

        public GeoPointValues geoPointValues() {
            if (geoPointValues == null) {
                geoPointValues = ((AtomicGeoPointFieldData) fieldData).getGeoPointValues();
            }
            return geoPointValues;
        }
    }

}
