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

import org.apache.lucene.index.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.AppendingDeltaPackedLongBuffer;
import org.apache.lucene.util.packed.MonotonicAppendingLongBuffer;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;

/**
 * {@link AtomicNumericFieldData} implementation which stores data in packed arrays to save memory.
 */
public abstract class PackedArrayAtomicFieldData extends AbstractAtomicLongFieldData {

    @Override
    public void close() {
    }

    public static class WithOrdinals extends PackedArrayAtomicFieldData {

        private final MonotonicAppendingLongBuffer values;
        private final Ordinals ordinals;
        private final int maxDoc;

        public WithOrdinals(MonotonicAppendingLongBuffer values, Ordinals ordinals, int maxDoc) {
            super();
            this.values = values;
            this.ordinals = ordinals;
            this.maxDoc = maxDoc;
        }

        @Override
        public long ramBytesUsed() {
            return RamUsageEstimator.NUM_BYTES_INT/*size*/ + values.ramBytesUsed() + ordinals.ramBytesUsed();
        }

        @Override
        public SortedNumericDocValues getLongValues() {
            final RandomAccessOrds ords = ordinals.ordinals();
            final SortedDocValues singleOrds = DocValues.unwrapSingleton(ords);
            if (singleOrds != null) {
                final NumericDocValues singleValues = new NumericDocValues() {
                    @Override
                    public long get(int docID) {
                        final int ord = singleOrds.getOrd(docID);
                        if (ord >= 0) {
                            return values.get(singleOrds.getOrd(docID));
                        } else {
                            return 0;
                        }
                    }
                };
                return DocValues.singleton(singleValues, DocValues.docsWithValue(ords, maxDoc));
            } else {
                return new SortedNumericDocValues() {
                    @Override
                    public long valueAt(int index) {
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
     * A single valued case, where not all values are "set", so we have a special
     * value which encodes the fact that the document has no value.
     */
    public static class SingleSparse extends PackedArrayAtomicFieldData {

        private final PackedInts.Mutable values;
        private final long minValue;
        private final long missingValue;
        private final int maxDoc;

        public SingleSparse(PackedInts.Mutable values, long minValue, long missingValue, int maxDoc) {
            super();
            this.values = values;
            this.minValue = minValue;
            this.missingValue = missingValue;
            this.maxDoc = maxDoc;
        }

        @Override
        public long ramBytesUsed() {
            return values.ramBytesUsed() + 2 * RamUsageEstimator.NUM_BYTES_LONG;
        }

        @Override
        public SortedNumericDocValues getLongValues() {
            final NumericDocValues values = new NumericDocValues() {
                @Override
                public long get(int docID) {
                    final long delta = SingleSparse.this.values.get(docID);
                    if (delta == missingValue) {
                        return 0;
                    }
                    return minValue + delta;
                }
            };
            final Bits docsWithFields = new Bits() {
                @Override
                public boolean get(int index) {
                    return values.get(index) != missingValue;
                }
                @Override
                public int length() {
                    return maxDoc;
                }
            };
            return DocValues.singleton(values, docsWithFields);
        }
    }

    /**
     * Assumes all the values are "set", and docId is used as the index to the value array.
     */
    public static class Single extends PackedArrayAtomicFieldData {

        private final PackedInts.Mutable values;
        private final long minValue;

        /**
         * Note, here, we assume that there is no offset by 1 from docId, so position 0
         * is the value for docId 0.
         */
        public Single(PackedInts.Mutable values, long minValue) {
            super();
            this.values = values;
            this.minValue = minValue;
        }

        @Override
        public long ramBytesUsed() {
            return values.ramBytesUsed();
        }

        @Override
        public SortedNumericDocValues getLongValues() {
            final NumericDocValues values = new NumericDocValues() {
                @Override
                public long get(int docID) {
                    return minValue + Single.this.values.get(docID);
                }
            };
            return DocValues.singleton(values, null);
        }
    }

    /**
     * A single valued case, where not all values are "set", so we have a special
     * value which encodes the fact that the document has no value. The data is stored in
     * a paged wise manner for better compression.
     */
    public static class PagedSingle extends PackedArrayAtomicFieldData {

        private final AppendingDeltaPackedLongBuffer values;
        private final FixedBitSet docsWithValue;

        public PagedSingle(AppendingDeltaPackedLongBuffer values, FixedBitSet docsWithValue) {
            super();
            this.values = values;
            this.docsWithValue = docsWithValue;
        }

        @Override
        public long ramBytesUsed() {
            return values.ramBytesUsed() + 2 * RamUsageEstimator.NUM_BYTES_LONG;
        }

        @Override
        public SortedNumericDocValues getLongValues() {
            return DocValues.singleton(values, docsWithValue);
        }
    }

}
