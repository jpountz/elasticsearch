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
import org.elasticsearch.common.collect.ReusableGrowableArray;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.bucket.LongBucketsAggregator;
import org.elasticsearch.search.aggregations.context.ValueSpace;
import org.elasticsearch.search.aggregations.context.numeric.NumericValuesSource;

import java.io.IOException;
import java.util.List;

/**
 * A collector that is used by the histogram aggregator which aggregates the histogram buckets
 */
class HistogramCollector implements Aggregator.Collector {

    final List<Aggregator.Factory> factories;
    final LongBucketsAggregator aggregator;
    final LongObjectOpenHashMap<BucketCollector> bucketCollectors;
    final Rounding rounding;

    NumericValuesSource valuesSource;

    // a reusable list of matched buckets which is used when dealing with multi-valued fields. see #populateMatchedBuckets method
    private final ReusableGrowableArray<BucketCollector> matchedBuckets = new ReusableGrowableArray<BucketCollector>(BucketCollector.class);

    /**
     * Constructs a new histogram collector.
     *
     * @param aggregator        The histogram aggregator this collector is associated with (will serve as the parent aggregator
     *                          all bucket-level sub-aggregators
     * @param valuesSource      The values source on which this aggregator works
     * @param rounding          The rounding strategy by which the get will bucket documents
     */
    HistogramCollector(LongBucketsAggregator aggregator,
                       List<Aggregator.Factory> factories,
                       NumericValuesSource valuesSource,
                       Rounding rounding,
                       LongObjectOpenHashMap<BucketCollector> bucketCollectors) {

        this.factories = factories;
        this.aggregator = aggregator;
        this.valuesSource = valuesSource;
        this.rounding = rounding;
        this.bucketCollectors = bucketCollectors;
    }

    @Override
    public void postCollection() {
        Object[] collectors = bucketCollectors.values;
        for (int i = 0; i < bucketCollectors.allocated.length; i++) {
            if (bucketCollectors.allocated[i]) {
                ((BucketCollector) collectors[i]).postCollection();
            }
        }
    }

    @Override
    public void collect(int doc, ValueSpace valueSpace) throws IOException {

        LongValues values = valuesSource.longValues();

        if (!values.hasValue(doc)) {
            return;
        }

        if (!values.isMultiValued()) {

            // optimization for a single valued field when aggregating a single field. In this case,
            // there's no need to mark buckets as a match on a bucket will always be a single match per doc
            // so we can just collect it

            long value = values.getValue(doc);
            if (!valueSpace.accept(valuesSource.key(), value)) {
                return;
            }
            long key = rounding.round(value);
            BucketCollector bucketCollector = bucketCollectors.get(key);
            if (bucketCollector == null) {
                bucketCollector = new BucketCollector(key, rounding, valuesSource, factories, aggregator);
                bucketCollectors.put(key, bucketCollector);
            }
            bucketCollector.collect(doc, valueSpace);
            return;

        }

        // it's a multi-valued field, meaning, some values of the field may fit the bucket, while other
        // won't. thus, we need to iterate on all values and mark the bucket that they fall in (or
        // create new buckets if needed). Only after this "mark" phase ends, we can iterate over all the buckets
        // and aggregate only those that are marked (and while at it, clear the mark, making it ready for
        // the next aggregation).

        populateMatchedBuckets(doc, valuesSource.key(), values, valueSpace);
        BucketCollector[] mBukcets = matchedBuckets.innerValues();
        for (int i = 0; i < matchedBuckets.size(); i++) {
            mBukcets[i].matched = false;
            mBukcets[i].collect(doc, valueSpace);
        }
    }

    // collecting all the buckets (and creating new buckets if needed) that match the given doc
    // based on the values of the given field. after this method is called we'll go over the buckets and only
    // collect the matched ones. We need to do this to avoid situations where multiple values in a single field
    // or multiple values across the aggregated fields match the bucket and then the bucket will collect the same
    // document multiple times.
    private void populateMatchedBuckets(int doc, Object valuesSourceKey, LongValues values, ValueSpace valueSpace) {
        matchedBuckets.reset();
        for (LongValues.Iter iter = values.getIter(doc); iter.hasNext();) {
            long value = iter.next();
            if (!valueSpace.accept(valuesSourceKey, value)) {
                continue;
            }
            long key = rounding.round(value);
            BucketCollector bucket = bucketCollectors.get(key);
            if (bucket == null) {
                bucket = new BucketCollector(key, rounding, valuesSource, factories, aggregator);
                bucketCollectors.put(key, bucket);
            }
            if (!bucket.matched) {
                matchedBuckets.add(bucket);
                bucket.matched = true;
            }
        }
    }



    /**
     * A collector for a histogram bucket. This collector counts the number of documents that fall into it,
     * but also serves as the get context for all the sub addAggregation it contains.
     */
    static class BucketCollector extends LongBucketsAggregator.BucketCollector {

        // hacky, but needed for performance. We use this in the #findMatchedBuckets method, to keep track of the buckets
        // we already matched (we don't want to pick up the same bucket twice). An alternative for this hack would have
        // been to use a set in that method instead of a list, but that comes with performance costs (every time
        // a bucket is added to the set it's being hashed and compared to other buckets)
        boolean matched = false;

        final long key;
        final Rounding rounding;

        long docCount;

        BucketCollector(long key, Rounding rounding, NumericValuesSource valuesSource, List<Aggregator.Factory> factories, Aggregator parent) {
            super(valuesSource, factories, parent);
            this.key = key;
            this.rounding = rounding;
        }

        @Override
        protected boolean onDoc(int doc, LongValues values, ValueSpace valueSpace) throws IOException {
            docCount++;
            return true;
        }

        @Override
        public boolean accept(long value) {
            return this.key == rounding.round(value);
        }

    }

}
