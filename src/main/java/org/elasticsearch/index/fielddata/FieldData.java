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

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.*;
import org.apache.lucene.util.*;
import org.elasticsearch.common.geo.GeoPoint;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Utility methods, similar to Lucene's {@link DocValues}.
 */
public enum FieldData {
    ;

    /**
     * Return a {@link SortedNumericDoubleValues} that doesn't contain any value.
     */
    public static SortedNumericDoubleValues emptySortedNumericDoubles() {
        return new SortedNumericDoubleValues() {

            @Override
            public void setDocument(int doc) {
            }

            @Override
            public double valueAt(int index) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int count() {
                return 0;
            }

        };
    }

    /**
     * Return a {@link SortedNumericDoubleValues} that doesn't contain any value.
     */
    public static MultiGeoPointValues emptyMultiGeoPoints() {
        final GeoPoint point = new GeoPoint();
        return new MultiGeoPointValues() {
            @Override
            public GeoPoint valueAt(int index) {
                return point;
            }
            
            @Override
            public void setDocument(int docId) {
            }
            
            @Override
            public int count() {
                return 0;
            }
        };
    }

    /**
     * Returns a Bits representing all documents from <code>dv</code> that have a value.
     */
    public static Bits docsWithValue(final SortedBinaryDocValues dv, final int maxDoc) {
      return new Bits() {
        @Override
        public boolean get(int index) {
          dv.setDocument(index);
          return dv.count() != 0;
        }

        @Override
        public int length() {
          return maxDoc;
        }
      };
    }

    /**
     * Returns a Bits representing all documents from <code>dv</code> that have a value.
     */
    public static Bits docsWithValue(final MultiGeoPointValues dv, final int maxDoc) {
      return new Bits() {
        @Override
        public boolean get(int index) {
          dv.setDocument(index);
          return dv.count() != 0;
        }

        @Override
        public int length() {
          return maxDoc;
        }
      };
    }

    /**
     * Returns a Bits representing all documents from <code>dv</code> that have a value.
     */
    public static Bits docsWithValue(final SortedNumericDoubleValues dv, final int maxDoc) {
      return new Bits() {
        @Override
        public boolean get(int index) {
          dv.setDocument(index);
          return dv.count() != 0;
        }

        @Override
        public int length() {
          return maxDoc;
        }
      };
    }

    /**
     * Wrap the provided {@link SortedNumericDocValues} instance to cast all values to doubles.
     */
    public static SortedNumericDoubleValues castToDouble(final SortedNumericDocValues values) {
        return new DoubleCastedValues(values);
    }

    /**
     * Wrap the provided {@link SortedNumericDoubleValues} instance to cast all values to longs.
     */
    public static SortedNumericDocValues castToLong(final SortedNumericDoubleValues values) {
        return new LongCastedValues(values);
    }

    public static SortedNumericDoubleValues singleton(NumericDoubleValues values, Bits docsWithField) {
        return new SingletonSortedNumericDoubleValues(values, docsWithField);
    }

    public static NumericDoubleValues unwrapSingleton(SortedNumericDoubleValues values) {
        if (values instanceof SingletonSortedNumericDoubleValues) {
            return ((SingletonSortedNumericDoubleValues) values).getNumericDoubleValues();
        }
        return null;
    }

    public static Bits unwrapSingletonBits(SortedNumericDoubleValues dv) {
      if (dv instanceof SingletonSortedNumericDoubleValues) {
        return ((SingletonSortedNumericDoubleValues)dv).getDocsWithField();
      } else {
        return null;
      }
    }

    public static MultiGeoPointValues singleton(GeoPointValues values, Bits docsWithField) {
        return new SingletonMultiGeoPointValues(values, docsWithField);
    }

    public static GeoPointValues unwrapSingleton(MultiGeoPointValues values) {
        if (values instanceof SingletonMultiGeoPointValues) {
            return ((SingletonMultiGeoPointValues) values).getGeoPointValues();
        }
        return null;
    }

    public static Bits unwrapSingletonBits(MultiGeoPointValues values) {
        if (values instanceof SingletonMultiGeoPointValues) {
            return ((SingletonMultiGeoPointValues) values).getDocsWithField();
        }
        return null;
    }

    public static SortedBinaryDocValues singleton(BinaryDocValues values, Bits docsWithField) {
        return new SingletonSortedBinaryDocValues(values, docsWithField);
    }

    public static BinaryDocValues unwrapSingleton(SortedBinaryDocValues values) {
        if (values instanceof SingletonSortedBinaryDocValues) {
            return ((SingletonSortedBinaryDocValues) values).getBinaryDocValues();
        }
        return null;
    }

    public static Bits unwrapSingletonBits(SortedBinaryDocValues values) {
        if (values instanceof SingletonSortedBinaryDocValues) {
            return ((SingletonSortedBinaryDocValues) values).getDocsWithField();
        }
        return null;
    }

    public static boolean isMultiValued(SortedSetDocValues values) {
        return DocValues.unwrapSingleton(values) == null;
    }

    public static boolean isMultiValued(SortedNumericDocValues values) {
        if (values instanceof LongCastedValues) {
            return isMultiValued(((LongCastedValues) values).values);
        }
        return DocValues.unwrapSingleton(values) == null;
    }

    public static boolean isMultiValued(SortedNumericDoubleValues values) {
        if (values instanceof DoubleCastedValues) {
            return isMultiValued(((DoubleCastedValues) values).values);
        }
        return unwrapSingleton(values) == null;
    }

    public static boolean isMultiValued(SortedBinaryDocValues values) {
        return unwrapSingleton(values) != null;
    }

    public static boolean isMultiValued(MultiGeoPointValues values) {
        return unwrapSingleton(values) == null;
    }

    public static SortedBinaryDocValues toString(final SortedNumericDocValues values) {
        return toString(new ToStringValues() {
            @Override
            public void get(int docID, List<CharSequence> list) {
                values.setDocument(docID);
                for (int i = 0, count = values.count(); i < count; ++i) {
                    list.add(Long.toString(values.valueAt(i)));
                }
            }
        });
    }

    public static SortedBinaryDocValues toString(final SortedNumericDoubleValues values) {
        return toString(new ToStringValues() {
            @Override
            public void get(int docID, List<CharSequence> list) {
                values.setDocument(docID);
                for (int i = 0, count = values.count(); i < count; ++i) {
                    list.add(Double.toString(values.valueAt(i)));
                }
            }
        });
    }

    public static SortedBinaryDocValues toString(final RandomAccessOrds values) {
        return toString(new ToStringValues() {
            @Override
            public void get(int docID, List<CharSequence> list) {
                values.setDocument(docID);
                for (int i = 0, count = values.cardinality(); i < count; ++i) {
                    list.add(values.lookupOrd(values.ordAt(i)).utf8ToString());
                }
            }
        });
    }

    public static SortedBinaryDocValues toString(final MultiGeoPointValues values) {
        return toString(new ToStringValues() { 
            @Override
            public void get(int docID, List<CharSequence> list) {
                values.setDocument(docID);
                for (int i = 0, count = values.count(); i < count; ++i) {
                    list.add(values.valueAt(i).toString());
                }
            }
        });
    }

    /**
     * If <code>dv</code> is an instance of {@link RandomAccessOrds}, then return
     * it, otherwise wrap it into a slow wrapper that implements random access.
     */
    public static RandomAccessOrds maybeSlowRandomAccessOrds(final SortedSetDocValues dv) {
        if (dv instanceof RandomAccessOrds) {
            return (RandomAccessOrds) dv;
        } else {
            return new RandomAccessOrds() {

                int cardinality;
                long[] ords = new long[0];
                int ord;

                @Override
                public void setDocument(int docID) {
                    cardinality = 0;
                    dv.setDocument(docID);
                    for (long ord = dv.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = dv.nextOrd()) {
                        ords = ArrayUtil.grow(ords, cardinality + 1);
                        ords[cardinality++] = ord;
                    }
                    ord = 0;
                }

                @Override
                public long nextOrd() {
                    return ords[ord++];
                }

                @Override
                public BytesRef lookupOrd(long ord) {
                    return dv.lookupOrd(ord);
                }

                @Override
                public long getValueCount() {
                    return dv.getValueCount();
                }

                @Override
                public long ordAt(int index) {
                    return ords[index];
                }

                @Override
                public int cardinality() {
                    return cardinality;
                }
            };
        }
    }

    private static SortedBinaryDocValues toString(final ToStringValues values) {
        return new SortedBinaryDocValues() {

            final List<CharSequence> list = new ArrayList<>();
            BytesRef[] refs = new BytesRef[0];
            Sorter sorter = new BytesRefSorter(refs);
            int count;

            @Override
            public void setDocument(int docID) {
                list.clear();
                values.get(docID, list);
                count = list.size();
                if (count > refs.length) {
                    final int prevLen = refs.length;
                    refs = Arrays.copyOf(refs, ArrayUtil.oversize(count, RamUsageEstimator.NUM_BYTES_OBJECT_REF));
                    for (int i = prevLen; i < refs.length; ++i) {
                        refs[i] = new BytesRef();
                    }
                    sorter = new BytesRefSorter(refs);
                }
                for (int i = 0; i < count; ++i) {
                    final CharSequence s = list.get(i);
                    refs[i].copyChars(s);
                }
                sorter.sort(0, count);
            }

            @Override
            public int count() {
                return count;
            }

            @Override
            public BytesRef valueAt(int index) {
                return refs[index];
            }

        };
    }

    private static interface ToStringValues {

        void get(int docID, List<CharSequence> values);

    }

    private static class DoubleCastedValues extends SortedNumericDoubleValues {

        private final SortedNumericDocValues values;

        DoubleCastedValues(SortedNumericDocValues in) {
            this.values = in;
        }

        @Override
        public double valueAt(int index) {
            return values.valueAt(index);
        }

        @Override
        public void setDocument(int doc) {
            values.setDocument(doc);
        }

        @Override
        public int count() {
            return values.count();
        }

    }

    private static class LongCastedValues extends SortedNumericDocValues {

        private final SortedNumericDoubleValues values;

        LongCastedValues(SortedNumericDoubleValues in) {
            this.values = in;
        }

        @Override
        public long valueAt(int index) {
            return (long) values.valueAt(index);
        }

        @Override
        public void setDocument(int doc) {
            values.setDocument(doc);
        }

        @Override
        public int count() {
            return values.count();
        }

    }
}
