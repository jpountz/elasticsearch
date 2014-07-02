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

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;

/**
 */
public abstract class DoubleArrayAtomicFieldData extends AbstractAtomicDoubleFieldData {

    @Override
    public void close() {
    }

    public static class WithOrdinals extends DoubleArrayAtomicFieldData {

        private final DoubleArray values;
        private final Ordinals ordinals;
        private final int maxDoc;

        public WithOrdinals(DoubleArray values, Ordinals ordinals, int maxDoc) {
            super();
            this.values = values;
            this.ordinals = ordinals;
            this.maxDoc = maxDoc;
        }

        @Override
        public long ramBytesUsed() {
            return values.ramBytesUsed() + ordinals.ramBytesUsed();
        }


        @Override
        public SortedNumericDoubleValues getDoubleValues() {
            final RandomAccessOrds ords = ordinals.ordinals();
            final SortedDocValues singleOrds = DocValues.unwrapSingleton(ords);
            if (singleOrds != null) {
                final NumericDoubleValues singleValues = new NumericDoubleValues() {
                    @Override
                    public double get(int docID) {
                        final int ord = singleOrds.getOrd(docID);
                        if (ord >= 0) {
                            return values.get(singleOrds.getOrd(docID));
                        } else {
                            return 0;
                        }
                    }
                };
                return FieldData.singleton(singleValues, DocValues.docsWithValue(ords, maxDoc));
            } else {
                return new SortedNumericDoubleValues() {
                    @Override
                    public double valueAt(int index) {
                        return values.get(ords.ordAt(index));
                    }

                    @Override
                    public void setDocument(int doc) {
                        ords.setDocument(doc);
                    }

                    @Override
                    public int count() {
                        return ords.cardinality();
                    }
                };
            }
        }
    }

    /**
     * A single valued case, where not all values are "set", so we have a FixedBitSet that
     * indicates which values have an actual value.
     */
    public static class Single extends DoubleArrayAtomicFieldData {

        private final DoubleArray values;
        private final FixedBitSet set;

        public Single(DoubleArray values, FixedBitSet set) {
            super();
            this.values = values;
            this.set = set;
        }

        @Override
        public long ramBytesUsed() {
            return values.ramBytesUsed() + set.ramBytesUsed();
        }

        @Override
        public SortedNumericDoubleValues getDoubleValues() {
            final NumericDoubleValues values = new NumericDoubleValues() {
                @Override
                public double get(int docID) {
                    return Single.this.values.get(docID);
                }
            };
            return FieldData.singleton(values, set);
        }
    }

}
