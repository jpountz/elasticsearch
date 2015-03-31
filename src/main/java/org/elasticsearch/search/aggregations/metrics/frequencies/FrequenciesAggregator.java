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

package org.elasticsearch.search.aggregations.metrics.frequencies;

import com.carrotsearch.hppc.hash.MurmurHash3;
import com.google.common.base.Preconditions;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.io.IOException;
import java.util.Map;

/**
 * An aggregator that computes approximate counts of unique values that have a given frequency.
 */
public class FrequenciesAggregator extends NumericMetricsAggregator.MultiValue {

    private final int d, lgW, lgMaxFreq;
    private final boolean rehash;
    private final ValuesSource valuesSource;

    // Expensive to initialize, so we only initialize it when we have an actual value source
    @Nullable
    private CountMinSketch frequencies;

    private ValueFormatter formatter;

    public FrequenciesAggregator(String name, ValuesSource valuesSource, boolean rehash, int d, int lgW, int lgMaxFreq, @Nullable ValueFormatter formatter,
                                 AggregationContext context, Aggregator parent, Map<String, Object> metaData) throws IOException {
        super(name, context, parent, metaData);
        this.valuesSource = valuesSource;
        this.rehash = rehash;
        this.d = d;
        this.lgW = lgW;
        this.lgMaxFreq = lgMaxFreq;
        this.frequencies = valuesSource == null ? null : new CountMinSketch(d, lgW, lgMaxFreq, context.bigArrays());
        this.formatter = formatter;
    }

    @Override
    public boolean needsScores() {
        return valuesSource != null && valuesSource.needsScores();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        // if rehash is false then the value source is either already hashed, or the user explicitly
        // requested not to hash the values (perhaps they already hashed the values themselves before indexing the doc)
        // so we can just work with the original value source as is
        if (!rehash) {
            MurmurHash3Values hashValues = MurmurHash3Values.cast(((ValuesSource.Numeric) valuesSource).longValues(ctx));
            return new DirectCollector(frequencies, hashValues);
        }

        if (valuesSource instanceof ValuesSource.Numeric) {
            ValuesSource.Numeric source = (ValuesSource.Numeric) valuesSource;
            MurmurHash3Values hashValues = source.isFloatingPoint() ? MurmurHash3Values.hash(source.doubleValues(ctx)) : MurmurHash3Values.hash(source.longValues(ctx));
            return new DirectCollector(frequencies, hashValues);
        }

        return new DirectCollector(frequencies, MurmurHash3Values.hash(valuesSource.bytesValues(ctx)));
    }

    @Override
    public boolean hasMetric(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double metric(String name, long owningBucketOrd) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (counts == null || owningBucketOrdinal >= counts.maxBucket() || counts.cardinality(owningBucketOrdinal) == 0) {
            return buildEmptyAggregation();
        }
        // We need to build a copy because the returned Aggregation needs remain usable after
        // this Aggregator (and its HLL++ counters) is released.
        HyperLogLogPlusPlus copy = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
        copy.merge(0, counts, owningBucketOrdinal);
        return new InternalFrequencies(name, copy, formatter, metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalFrequencies(name, null, formatter, metaData());
    }

    @Override
    protected void doClose() {
        Releasables.close(counts, collector);
    }

    private static class DirectCollector extends LeafBucketCollector {

        private final MurmurHash3Values hashes;
        private final CountMinSketch frequencies;

        DirectCollector(CountMinSketch counts, MurmurHash3Values values) {
            this.frequencies = counts;
            this.hashes = values;
        }

        @Override
        public void collect(int doc, long bucketOrd) {
            hashes.setDocument(doc);
            final int valueCount = hashes.count();
            for (int i = 0; i < valueCount; ++i) {
                frequencies.collect(bucketOrd, hashes.valueAt(i));
            }
        }

    }

    /**
     * Representation of a list of hash values. There might be dups and there is no guarantee on the order.
     */
    static abstract class MurmurHash3Values {

        public abstract void setDocument(int docId);

        public abstract int count();

        public abstract long valueAt(int index);

        /**
         * Return a {@link MurmurHash3Values} instance that returns each value as its hash.
         */
        public static MurmurHash3Values cast(final SortedNumericDocValues values) {
            return new MurmurHash3Values() {
                @Override
                public void setDocument(int docId) {
                    values.setDocument(docId);
                }
                @Override
                public int count() {
                    return values.count();
                }
                @Override
                public long valueAt(int index) {
                    return values.valueAt(index);
                }
            };
        }

        /**
         * Return a {@link MurmurHash3Values} instance that computes hashes on the fly for each double value.
         */
        public static MurmurHash3Values hash(SortedNumericDoubleValues values) {
            return new Double(values);
        }

        /**
         * Return a {@link MurmurHash3Values} instance that computes hashes on the fly for each long value.
         */
        public static MurmurHash3Values hash(SortedNumericDocValues values) {
            return new Long(values);
        }

        /**
         * Return a {@link MurmurHash3Values} instance that computes hashes on the fly for each binary value.
         */
        public static MurmurHash3Values hash(SortedBinaryDocValues values) {
            return new Bytes(values);
        }

        private static class Long extends MurmurHash3Values {

            private final SortedNumericDocValues values;

            public Long(SortedNumericDocValues values) {
                this.values = values;
            }

            @Override
            public void setDocument(int docId) {
                values.setDocument(docId);
            }

            @Override
            public int count() {
                return values.count();
            }

            @Override
            public long valueAt(int index) {
                return MurmurHash3.hash(values.valueAt(index));
            }
        }

        private static class Double extends MurmurHash3Values {

            private final SortedNumericDoubleValues values;

            public Double(SortedNumericDoubleValues values) {
                this.values = values;
            }

            @Override
            public void setDocument(int docId) {
                values.setDocument(docId);
            }

            @Override
            public int count() {
                return values.count();
            }

            @Override
            public long valueAt(int index) {
                return MurmurHash3.hash(java.lang.Double.doubleToLongBits(values.valueAt(index)));
            }
        }

        private static class Bytes extends MurmurHash3Values {

            private final org.elasticsearch.common.hash.MurmurHash3.Hash128 hash = new org.elasticsearch.common.hash.MurmurHash3.Hash128();

            private final SortedBinaryDocValues values;

            public Bytes(SortedBinaryDocValues values) {
                this.values = values;
            }

            @Override
            public void setDocument(int docId) {
                values.setDocument(docId);
            }

            @Override
            public int count() {
                return values.count();
            }

            @Override
            public long valueAt(int index) {
                final BytesRef bytes = values.valueAt(index);
                org.elasticsearch.common.hash.MurmurHash3.hash128(bytes.bytes, bytes.offset, bytes.length, 0, hash);
                return hash.h1;
            }
        }
    }
}
