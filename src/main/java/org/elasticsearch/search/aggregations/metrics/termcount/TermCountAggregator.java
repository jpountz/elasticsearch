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

package org.elasticsearch.search.aggregations.metrics.termcount;

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.base.Preconditions;
import com.carrotsearch.hppc.hash.MurmurHash3;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.AtomicFieldData.Order;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.numeric.NumericValuesSource;

import java.io.IOException;

/**
 * An aggregator that computes approximate counts of unique values.
 */
public class TermCountAggregator extends MetricsAggregator.SingleValue {

    private final int precision;
    private final boolean rehash;
    private final ValuesSource valuesSource;
    private HyperLogLogPlusPlus counts;
    private Collector collector;

    public TermCountAggregator(String name, long estimatedBucketsCount, ValuesSource valuesSource, boolean rehash, int precision, AggregationContext context, Aggregator parent) {
        super(name, estimatedBucketsCount, context, parent);
        this.valuesSource = valuesSource;
        this.rehash = rehash;
        this.precision = precision;
        counts = new HyperLogLogPlusPlus(precision, bigArrays, estimatedBucketsCount);
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        if (collector != null) {
            collector.postCollect();
            collector.release();
            collector = null;
        }

        LongValues hashValues = null;
        BytesValues.WithOrdinals values = null;
        if (!rehash) {
            hashValues = ((NumericValuesSource) valuesSource).longValues();
        } else {
            if (valuesSource instanceof NumericValuesSource) {
                NumericValuesSource source = (NumericValuesSource) valuesSource;
                if (source.isFloatingPoint()) {
                    final DoubleValues doubleValues = source.doubleValues();
                    hashValues = new LongValues(values.isMultiValued()) {

                        @Override
                        public int setDocument(int docId) {
                            return doubleValues.setDocument(docId);
                        }

                        @Override
                        public long nextValue() {
                            return MurmurHash3.hash(Double.doubleToLongBits(doubleValues.nextValue()));
                        }

                        @Override
                        public Order getOrder() {
                            return Order.NONE;
                        }
                    };
                } else {
                    final LongValues longValues = source.longValues();
                    hashValues = new LongValues(longValues.isMultiValued()) {

                        @Override
                        public int setDocument(int docId) {
                            return longValues.setDocument(docId);
                        }

                        @Override
                        public long nextValue() {
                            return MurmurHash3.hash(longValues.nextValue());
                        }

                        @Override
                        public Order getOrder() {
                            return Order.NONE;
                        }
                    };
                }
            } else {
                final BytesValues bytesValues = valuesSource.bytesValues();
                if (bytesValues instanceof BytesValues.WithOrdinals) {
                    values = (BytesValues.WithOrdinals) bytesValues;
                    final long maxOrd = values.ordinals().getMaxOrd();
                    if (values.ordinals().getMaxOrd() > Integer.MAX_VALUE) {
                        // don't use ordinals
                        values = null;
                    }
                }
                if (values == null) {
                    final org.elasticsearch.common.hash.MurmurHash3.Hash128 hash = new org.elasticsearch.common.hash.MurmurHash3.Hash128();
                    hashValues = new LongValues(bytesValues.isMultiValued()) {

                        @Override
                        public int setDocument(int docId) {
                            return bytesValues.setDocument(docId);
                        }

                        @Override
                        public long nextValue() {
                            final BytesRef next = bytesValues.nextValue();
                            org.elasticsearch.common.hash.MurmurHash3.hash128(next.bytes, next.offset, next.length, 0, hash);
                            return hash.h1;
                        }

                        @Override
                        public Order getOrder() {
                            return Order.NONE;
                        }
                    };
                }
            }
        }

        if (hashValues != null) {
            assert values == null;
            collector = new DirectCollector(counts, hashValues);
        } else {
            collector = new OrdinalsCollector(counts, values, bigArrays);
        }
    }

    @Override
    public boolean shouldCollect() {
        return valuesSource != null;
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        collector.collect(doc, owningBucketOrdinal);
    }

    @Override
    protected void doPostCollection() {
        if (collector != null) {
            collector.postCollect();
            collector.release();
            collector = null;
        }
    }

    @Override
    public double metric(long owningBucketOrd) {
        return counts.cardinality(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (owningBucketOrdinal >= counts.maxBucket()) {
            return buildEmptyAggregation();
        }
        HyperLogLogPlusPlus copy = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
        copy.merge(counts, owningBucketOrdinal, 0);
        return new InternalTermCount(name, copy);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalTermCount(name, new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1));
    }

    @Override
    protected void doRelease() {
        Releasables.release(counts, collector);
    }

    private static interface Collector extends Releasable {

        void collect(int doc, long bucketOrd);

        void postCollect();

    }

    private static class DirectCollector implements Collector {

        private final LongValues hashes;
        private final HyperLogLogPlusPlus counts;

        DirectCollector(HyperLogLogPlusPlus counts, LongValues values) {
            this.counts = counts;
            this.hashes = values;
        }

        @Override
        public void collect(int doc, long bucketOrd) {
            final int valueCount = hashes.setDocument(doc);
            for (int i = 0; i < valueCount; ++i) {
                counts.collect(bucketOrd, hashes.nextValue());
            }
        }

        @Override
        public void postCollect() {
            // no-op
        }

        @Override
        public boolean release() throws ElasticsearchException {
            return true;
        }

    }

    private static class OrdinalsCollector implements Collector {

        private final BigArrays bigArrays;
        private final BytesValues.WithOrdinals values;
        private final Ordinals.Docs ordinals;
        private final int maxOrd;
        private final HyperLogLogPlusPlus counts;
        private ObjectArray<FixedBitSet> visitedOrds;

        OrdinalsCollector(HyperLogLogPlusPlus counts, BytesValues.WithOrdinals values, BigArrays bigArrays) {
            ordinals = values.ordinals();
            Preconditions.checkArgument(ordinals.getMaxOrd() <= Integer.MAX_VALUE);
            maxOrd = (int) ordinals.getMaxOrd();
            this.bigArrays = bigArrays;
            this.counts = counts;
            this.values = values;
            visitedOrds = bigArrays.newObjectArray(1);
        }

        @Override
        public void collect(int doc, long bucketOrd) {
            visitedOrds = bigArrays.grow(visitedOrds, bucketOrd + 1);
            FixedBitSet bits = visitedOrds.get(bucketOrd);
            if (bits == null) {
                bits = new FixedBitSet(maxOrd);
                visitedOrds.set(bucketOrd, bits);
            }
            final int valueCount = ordinals.setDocument(doc);
            for (int i = 0; i < valueCount; ++i) {
                bits.set((int) ordinals.nextOrd());
            }
        }

        @Override
        public void postCollect() {
            final FixedBitSet allVisitedOrds = new FixedBitSet(maxOrd);
            for (long bucket = visitedOrds.size() - 1; bucket >= 0; --bucket) {
                final FixedBitSet bits = visitedOrds.get(bucket);
                if (bits != null) {
                    allVisitedOrds.or(bits);
                }
            }

            final org.elasticsearch.common.hash.MurmurHash3.Hash128 hash = new org.elasticsearch.common.hash.MurmurHash3.Hash128();
            final LongArray hashes = bigArrays.newLongArray(maxOrd, false);
            boolean success = false;
            try {
                for (int ord = allVisitedOrds.nextSetBit(0); ord != -1; ord = ord + 1 < maxOrd ? allVisitedOrds.nextSetBit(ord + 1) : -1) {
                    final BytesRef value = values.getValueByOrd(ord);
                    org.elasticsearch.common.hash.MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, hash);
                    hashes.set(ord, hash.h1);
                }

                for (long bucket = visitedOrds.size() - 1; bucket >= 0; --bucket) {
                    final FixedBitSet bits = visitedOrds.get(bucket);
                    if (bits != null) {
                        for (int ord = bits.nextSetBit(0); ord != -1; ord = ord + 1 < maxOrd ? bits.nextSetBit(ord + 1) : -1) {
                            counts.collect(bucket, hashes.get(ord));
                        }
                    }
                }
                success = true;
            } finally {
                Releasables.release(success, hashes);
            }
        }

        @Override
        public boolean release() throws ElasticsearchException {
            Releasables.release(visitedOrds);
            return true;
        }

    }

}
