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

package org.elasticsearch.search.aggregations.bucket.multi.terms;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.collect.ReusableGrowableArray;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.bucket.LongBucketsAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValueSpace;
import org.elasticsearch.search.aggregations.context.numeric.NumericValuesSource;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.search.aggregations.bucket.BucketsAggregator.buildAggregations;

/**
 *
 */
public class LongTermsAggregator extends LongBucketsAggregator {

    private final List<Aggregator.Factory> factories;
    private final InternalOrder order;
    private final int requiredSize;

    final Recycler.V<LongObjectOpenHashMap<BucketCollector>> bucketCollectors;

    public LongTermsAggregator(String name, List<Aggregator.Factory> factories, NumericValuesSource valuesSource,
                               InternalOrder order, int requiredSize, AggregationContext aggregationContext, Aggregator parent) {
        super(name, valuesSource, aggregationContext, parent);
        this.factories = factories;
        this.order = order;
        this.requiredSize = requiredSize;
        this.bucketCollectors = aggregationContext.cacheRecycler().longObjectMap(-1);
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public LongTerms buildAggregation() {
        if (bucketCollectors.v().isEmpty()) {
            return new LongTerms(name, order, valuesSource.formatter(), requiredSize, ImmutableList.<InternalTerms.Bucket>of());
        }

        if (requiredSize < EntryPriorityQueue.LIMIT) {
            BucketPriorityQueue ordered = new BucketPriorityQueue(requiredSize, order.comparator());
            boolean[] states = bucketCollectors.v().allocated;
            Object[] collectors = bucketCollectors.v().values;
            for (int i = 0; i < states.length; i++) {
                if (states[i]) {
                    ordered.insertWithOverflow(((LongTermsAggregator.BucketCollector) collectors[i]).buildBucket());
                }
            }
            bucketCollectors.release();
            InternalTerms.Bucket[] list = new InternalTerms.Bucket[ordered.size()];
            for (int i = ordered.size() - 1; i >= 0; i--) {
                list[i] = (LongTerms.Bucket) ordered.pop();
            }
            return new LongTerms(name, order, valuesSource.formatter(), requiredSize, Arrays.asList(list));
        } else {
            BoundedTreeSet<InternalTerms.Bucket> ordered = new BoundedTreeSet<InternalTerms.Bucket>(order.comparator(), requiredSize);
            boolean[] states = bucketCollectors.v().allocated;
            Object[] collectors = bucketCollectors.v().values;
            for (int i = 0; i < states.length; i++) {
                ordered.add(((LongTermsAggregator.BucketCollector) collectors[i]).buildBucket());
            }
            bucketCollectors.release();
            return new LongTerms(name, order, valuesSource.formatter(), requiredSize, ordered);
        }
    }

    class Collector implements Aggregator.Collector {

        private ReusableGrowableArray<BucketCollector> matchedBuckets;

        @Override
        public void collect(int doc, ValueSpace valueSpace) throws IOException {

            LongValues values = valuesSource.longValues();

            if (!values.hasValue(doc)) {
                return;
            }

            Object valuesSourceKey = valuesSource.key();
            if (!values.isMultiValued()) {
                long term = values.getValue(doc);
                if (!valueSpace.accept(valuesSourceKey, term)) {
                    return;
                }
                BucketCollector bucket = bucketCollectors.v().get(term);
                if (bucket == null) {
                    bucket = new BucketCollector(valuesSource, term, LongTermsAggregator.this);
                    bucketCollectors.v().put(term, bucket);
                }
                bucket.collect(doc, valueSpace);
                return;
            }

            if (matchedBuckets == null) {
                matchedBuckets = new ReusableGrowableArray<BucketCollector>(BucketCollector.class);
            }
            populateMatchingBuckets(doc, valuesSourceKey, values, valueSpace);
            BucketCollector[] mBuckets = matchedBuckets.innerValues();
            for (int i = 0; i < matchedBuckets.size(); i++) {
                mBuckets[i].collect(doc, valueSpace);
            }
        }

        private void populateMatchingBuckets(int doc, Object valuesSourceKey, LongValues values, ValueSpace valueSpace) {
            matchedBuckets.reset();
            for (LongValues.Iter iter = values.getIter(doc); iter.hasNext();) {
                long term = iter.next();
                if (!valueSpace.accept(valuesSourceKey, term)) {
                    continue;
                }
                BucketCollector bucket = bucketCollectors.v().get(term);
                if (bucket == null) {
                    bucket = new BucketCollector(valuesSource, term, LongTermsAggregator.this);
                    bucketCollectors.v().put(term, bucket);
                }
                matchedBuckets.add(bucket);
            }
        }

        @Override
        public void postCollection() {
            boolean[] states = bucketCollectors.v().allocated;
            Object[] collectors = bucketCollectors.v().values;
            for (int i = 0; i < states.length; i++) {
                if (states[i]) {
                    ((LongTermsAggregator.BucketCollector) collectors[i]).postCollection();
                }
            }
        }

    }

    static class BucketCollector extends LongBucketsAggregator.BucketCollector {

        final long term;
        long docCount;

        BucketCollector(NumericValuesSource valuesSource, long term, LongTermsAggregator parent) {
            super(valuesSource, parent.factories, parent);
            this.term = term;
        }

        @Override
        protected boolean onDoc(int doc, LongValues values, ValueSpace valueSpace) throws IOException {
            docCount++;
            return true;
        }

        @Override
        public boolean accept(long value) {
            return term == value;
        }

        LongTerms.Bucket buildBucket() {
            return new LongTerms.Bucket(term, docCount, buildAggregations(subAggregators));
        }
    }

}
