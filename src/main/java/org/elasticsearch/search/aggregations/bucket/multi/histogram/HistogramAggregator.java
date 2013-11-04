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

package org.elasticsearch.search.aggregations.bucket.multi.histogram;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.collect.ReusableGrowableArray;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.context.numeric.NumericValuesSource;
import org.elasticsearch.search.aggregations.context.numeric.ValueFormatter;
import org.elasticsearch.search.aggregations.factory.AggregatorFactories;
import org.elasticsearch.search.aggregations.factory.ValueSourceAggregatorFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class HistogramAggregator extends Aggregator {

    private final NumericValuesSource valuesSource;
    private final Rounding rounding;
    private final InternalOrder order;
    private final boolean keyed;
    private final boolean computeEmptyBuckets;
    private final AbstractHistogramBase.Factory histogramFactory;
    private final Collector collector;

    private final Recycler.V<LongObjectOpenHashMap<BucketCollector>> bucketCollectors;

    public HistogramAggregator(String name,
                               AggregatorFactories factories,
                               Rounding rounding,
                               InternalOrder order,
                               boolean keyed,
                               boolean computeEmptyBuckets,
                               @Nullable NumericValuesSource valuesSource,
                               AbstractHistogramBase.Factory<?> histogramFactory,
                               AggregationContext aggregationContext,
                               Aggregator parent) {

        super(name, BucketAggregationMode.PER_BUCKET, factories, 50, aggregationContext, parent);
        this.valuesSource = valuesSource;
        this.rounding = rounding;
        this.order = order;
        this.keyed = keyed;
        this.computeEmptyBuckets = computeEmptyBuckets;
        this.histogramFactory = histogramFactory;
        this.bucketCollectors = aggregationContext.cacheRecycler().longObjectMap(-1);
        this.collector = valuesSource == null ? null : new Collector();
    }

    @Override
    public boolean shouldCollect() {
        return collector != null;
    }

    @Override
    public void collect(int doc, int owningBucketOrdinal) throws IOException {
        collector.collect(doc);
    }

    @Override
    protected void doPostCollection() {
        collector.postCollection();
    }

    @Override
    public InternalAggregation buildAggregation(int owningBucketOrdinal) {
        List<HistogramBase.Bucket> buckets = new ArrayList<HistogramBase.Bucket>(bucketCollectors.v().size());
        boolean[] allocated = bucketCollectors.v().allocated;
        Object[] collectors = this.bucketCollectors.v().values;
        for (int i = 0; i < allocated.length; i++) {
            if (!allocated[i]) {
                continue;
            }
            BucketCollector collector = (BucketCollector) collectors[i];
            buckets.add(histogramFactory.createBucket(collector.key, collector.docCount(), collector.buildAggregations()));
        }
        CollectionUtil.introSort(buckets, order.comparator());

        // value source will be null for unmapped fields
        ValueFormatter formatter = valuesSource != null ? valuesSource.formatter() : null;

        return histogramFactory.create(name, buckets, order, computeEmptyBuckets ? rounding : null, formatter, keyed);
    }

    class Collector {

        int ordCounter;
        LongObjectOpenHashMap<BucketCollector> bucketCollectors;

        Collector() {
             bucketCollectors = HistogramAggregator.this.bucketCollectors.v();
        }

        // a reusable list of matched buckets which is used when dealing with multi-valued fields. see #populateMatchedBuckets method
        private final ReusableGrowableArray<BucketCollector> matchedBuckets = new ReusableGrowableArray<BucketCollector>(BucketCollector.class);

        public void postCollection() {
            Object[] values = bucketCollectors.values;
            for (int i = 0; i < bucketCollectors.allocated.length; i++) {
                if (bucketCollectors.allocated[i]) {
                    ((BucketCollector) values[i]).postCollection();
                }
            }
        }

        public void collect(int doc) throws IOException {

            LongValues values = valuesSource.longValues();

            int valuesCount = values.setDocument(doc);

            // nocommit the matched logic could be more efficient if we knew the values are in order
            matchedBuckets.reset();
            for (int i = 0; i < valuesCount; ++i) {
                long value = values.nextValue();
                long key = rounding.round(value);
                BucketCollector bucketCollector = bucketCollectors.get(key);
                if (bucketCollector == null) {
                    bucketCollector = new BucketCollector(ordCounter++, key, rounding, factories.createBucketAggregators(HistogramAggregator.this, multiBucketAggregators, Math.max(50, bucketCollectors.size())));
                    bucketCollectors.put(key, bucketCollector);
                } else if (bucketCollector.matched) {
                    continue;
                }
                bucketCollector.collect(doc);
                bucketCollector.matched = true;
                matchedBuckets.add(bucketCollector);
            }
            for (int i = 0; i < matchedBuckets.size(); ++i) {
                matchedBuckets.innerValues()[i].matched = false;
            }
        }


    }

    /**
     * A collector for a histogram bucket. This collector counts the number of documents that fall into it,
     * but also serves as the get context for all the sub addAggregation it contains.
     */
    static class BucketCollector extends org.elasticsearch.search.aggregations.bucket.BucketCollector {

        // hacky, but needed for performance. We use this in the #findMatchedBuckets method, to keep track of the buckets
        // we already matched (we don't want to pick up the same bucket twice). An alternative for this hack would have
        // been to use a set in that method instead of a list, but that comes with performance costs (every time
        // a bucket is added to the set it's being hashed and compared to other buckets)
        boolean matched = false;

        final long key;
        final Rounding rounding;

        BucketCollector(int ord, long key, Rounding rounding, Aggregator[] subAggregators) {
            super(ord, subAggregators);
            this.key = key;
            this.rounding = rounding;
        }

        @Override
        protected boolean onDoc(int doc) throws IOException {
            return true;
        }
    }

    public static class Factory extends ValueSourceAggregatorFactory<NumericValuesSource> {

        private final Rounding rounding;
        private final InternalOrder order;
        private final boolean keyed;
        private final boolean computeEmptyBuckets;
        private final AbstractHistogramBase.Factory<?> histogramFactory;

        public Factory(String name, ValuesSourceConfig<NumericValuesSource> valueSourceConfig,
                       Rounding rounding, InternalOrder order, boolean keyed, boolean computeEmptyBuckets, AbstractHistogramBase.Factory<?> histogramFactory) {
            super(name, histogramFactory.type(), valueSourceConfig);
            this.rounding = rounding;
            this.order = order;
            this.keyed = keyed;
            this.computeEmptyBuckets = computeEmptyBuckets;
            this.histogramFactory = histogramFactory;
        }

        @Override
        public BucketAggregationMode bucketMode() {
            return BucketAggregationMode.PER_BUCKET;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new HistogramAggregator(name, factories, rounding, order, keyed, computeEmptyBuckets, null, histogramFactory, aggregationContext, parent);
        }

        @Override
        protected Aggregator create(NumericValuesSource valuesSource, int expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            return new HistogramAggregator(name, factories, rounding, order, keyed, computeEmptyBuckets, valuesSource, histogramFactory, aggregationContext, parent);
        }

    }
}
