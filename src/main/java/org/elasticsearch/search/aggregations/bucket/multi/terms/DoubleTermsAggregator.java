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

import com.carrotsearch.hppc.DoubleObjectOpenHashMap;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.index.fielddata.DoubleValues;
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
public class DoubleTermsAggregator extends MultiBucketAggregator {

    private final InternalOrder order;
    private final int requiredSize;
    private final NumericValuesSource valuesSource;

    Recycler.V<DoubleObjectOpenHashMap<BucketCollector>> bucketCollectors;

    private int ordCounter;

    public DoubleTermsAggregator(String name, AggregatorFactories factories, NumericValuesSource valuesSource,
                                 InternalOrder order, int requiredSize, AggregationContext aggregationContext, Aggregator parent) {
        super(name, factories, 50, aggregationContext, parent);
        this.valuesSource = valuesSource;
        this.order = order;
        this.requiredSize = requiredSize;
        this.bucketCollectors = aggregationContext.cacheRecycler().doubleObjectMap(-1);
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public DoubleTerms buildAggregation() {

        if (bucketCollectors.v().isEmpty()) {
            return new DoubleTerms(name, order, requiredSize, ImmutableList.<InternalTerms.Bucket>of());
        }

        if (requiredSize < EntryPriorityQueue.LIMIT) {
            BucketPriorityQueue ordered = new BucketPriorityQueue(requiredSize, order.comparator());
            Object[] collectors = bucketCollectors.v().values;
            boolean[] states = bucketCollectors.v().allocated;
            for (int i = 0; i < states.length; i++) {
                if (states[i]) {
                    BucketCollector collector = (BucketCollector) collectors[i];
                    DoubleTerms.Bucket bucket = new DoubleTerms.Bucket(collector.term, collector.docCount(), collector.buildAggregations(ordsAggregators));
                    ordered.insertWithOverflow(bucket);
                }
            }
            bucketCollectors.release();
            InternalTerms.Bucket[] list = new InternalTerms.Bucket[ordered.size()];
            for (int i = ordered.size() - 1; i >= 0; i--) {
                list[i] = (DoubleTerms.Bucket) ordered.pop();
            }
            return new DoubleTerms(name, order, requiredSize, Arrays.asList(list));
        } else {
            BoundedTreeSet<InternalTerms.Bucket> ordered = new BoundedTreeSet<InternalTerms.Bucket>(order.comparator(), requiredSize);
            Object[] collectors = bucketCollectors.v().values;
            boolean[] states = bucketCollectors.v().allocated;
            for (int i = 0; i < states.length; i++) {
                if (states[i]) {
                    BucketCollector collector = (BucketCollector) collectors[i];
                    DoubleTerms.Bucket bucket = new DoubleTerms.Bucket(collector.term, collector.docCount(), collector.buildAggregations(ordsAggregators));
                    ordered.add(bucket);
                }
            }
            bucketCollectors.release();
            return new DoubleTerms(name, order, requiredSize, ordered);
        }
    }

    class Collector implements Aggregator.Collector {

        DoubleObjectOpenHashMap<BucketCollector> bucketCollectors;

        Collector() {
            this.bucketCollectors = DoubleTermsAggregator.this.bucketCollectors.v();
        }

        @Override
        public void collect(int doc) throws IOException {

            DoubleValues values = valuesSource.doubleValues();
            int valuesCount = values.setDocument(doc);

            for (int i = 0; i < valuesCount; ++i) {
                double term = values.nextValue();
                BucketCollector bucket = bucketCollectors.get(term);
                if (bucket == null) {
                    bucket = new BucketCollector(ordCounter++, term, factories.createAggregators(DoubleTermsAggregator.this), ordsCollectors);
                    bucketCollectors.put(term, bucket);
                }
                bucket.collect(doc);
            }

        }

        @Override
        public void postCollection() {
            Object[] collectors = bucketCollectors.values;
            boolean[] states = bucketCollectors.allocated;
            for (int i = 0; i < states.length; i++) {
                if (states[i]) {
                    ((BucketCollector) collectors[i]).postCollection();
                }
            }
        }
    }

    static class BucketCollector extends MultiBucketAggregator.BucketCollector {

        final double term;

        BucketCollector(int ord, double term, Aggregator[] aggregators, OrdsAggregator.Collector[] ordsCollectors) {
            super(ord, aggregators, ordsCollectors);
            this.term = term;
        }
    }

}
