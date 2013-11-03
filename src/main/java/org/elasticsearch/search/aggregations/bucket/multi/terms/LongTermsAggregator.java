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
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.OrdsAggregator;
import org.elasticsearch.search.aggregations.bucket.multi.MultiBucketAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.numeric.NumericValuesSource;
import org.elasticsearch.search.aggregations.factory.AggregatorFactories;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;

import java.io.IOException;
import java.util.Arrays;

/**
 *
 */
public class LongTermsAggregator extends MultiBucketAggregator {

    private final InternalOrder order;
    private final int requiredSize;
    private final NumericValuesSource valuesSource;

    final Recycler.V<LongObjectOpenHashMap<BucketCollector>> bucketCollectors;

    int ordCounter;

    public LongTermsAggregator(String name, AggregatorFactories factories, NumericValuesSource valuesSource,
                               InternalOrder order, int requiredSize, AggregationContext aggregationContext, Aggregator parent) {
        super(name, factories, 50, aggregationContext, parent);
        this.valuesSource = valuesSource;
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
                    LongTermsAggregator.BucketCollector collector = (LongTermsAggregator.BucketCollector) collectors[i];
                    LongTerms.Bucket bucket = new LongTerms.Bucket(collector.term, collector.docCount(), collector.buildAggregations(ordsAggregators));
                    ordered.insertWithOverflow(bucket);
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
                LongTermsAggregator.BucketCollector collector = (LongTermsAggregator.BucketCollector) collectors[i];
                LongTerms.Bucket bucket = new LongTerms.Bucket(collector.term, collector.docCount(), collector.buildAggregations(ordsAggregators));
                ordered.add(bucket);
            }
            bucketCollectors.release();
            return new LongTerms(name, order, valuesSource.formatter(), requiredSize, ordered);
        }
    }

    class Collector implements Aggregator.Collector {

        LongObjectOpenHashMap<BucketCollector> bucketCollectors;

        Collector() {
            bucketCollectors = LongTermsAggregator.this.bucketCollectors.v();
        }

        @Override
        public void collect(int doc) throws IOException {
            LongValues values = valuesSource.longValues();
            int valuesCount = values.setDocument(doc);
            for (int i = 0; i < valuesCount; ++i) {
                long term = values.nextValue();
                BucketCollector bucket = bucketCollectors.get(term);
                if (bucket == null) {
                    bucket = new BucketCollector(ordCounter++, term, factories.createAggregators(LongTermsAggregator.this), ordsCollectors);
                    bucketCollectors.put(term, bucket);
                }
                bucket.collect(doc);
            }
        }

        @Override
        public void postCollection() {
            boolean[] states = bucketCollectors.allocated;
            Object[] collectors = bucketCollectors.values;
            for (int i = 0; i < states.length; i++) {
                if (states[i]) {
                    ((LongTermsAggregator.BucketCollector) collectors[i]).postCollection();
                }
            }
        }

    }

    static class BucketCollector extends MultiBucketAggregator.BucketCollector {

        final long term;

        BucketCollector(int ord, long term, Aggregator[] aggregators, OrdsAggregator.Collector[] ordsCollectors) {
            super(ord, aggregators, ordsCollectors);
            this.term = term;
        }
    }

}
