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
import org.elasticsearch.search.aggregations.bucket.DoubleBucketsAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.numeric.NumericValuesSource;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.search.aggregations.bucket.BucketsAggregator.buildAggregations;

/**
 *
 */
public class DoubleTermsAggregator extends DoubleBucketsAggregator {

    private final List<Aggregator.Factory> factories;
    private final InternalOrder order;
    private final int requiredSize;

    Recycler.V<DoubleObjectOpenHashMap<BucketCollector>> bucketCollectors;

    public DoubleTermsAggregator(String name, List<Aggregator.Factory> factories, NumericValuesSource valuesSource,
                                 InternalOrder order, int requiredSize, AggregationContext aggregationContext, Aggregator parent) {
        super(name, valuesSource, aggregationContext, parent);
        this.factories = factories;
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
                    ordered.insertWithOverflow(((BucketCollector) collectors[i]).buildBucket());
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
                    ordered.add(((BucketCollector) collectors[i]).buildBucket());
                }
            }
            bucketCollectors.release();
            return new DoubleTerms(name, order, requiredSize, ordered);
        }
    }

    class Collector implements Aggregator.Collector {

        @Override
        public void collect(int doc) throws IOException {

            DoubleValues values = valuesSource.doubleValues();
            int valuesCount = values.setDocument(doc);

            for (int i = 0; i < valuesCount; ++i) {
                double term = values.nextValue();
                BucketCollector bucket = bucketCollectors.v().get(term);
                if (bucket == null) {
                    bucket = new BucketCollector(valuesSource, term, DoubleTermsAggregator.this);
                    bucketCollectors.v().put(term, bucket);
                }
                bucket.collect(doc);
            }

        }

        @Override
        public void postCollection() {
            Object[] collectors = bucketCollectors.v().values;
            boolean[] states = bucketCollectors.v().allocated;
            for (int i = 0; i < states.length; i++) {
                if (states[i]) {
                    ((BucketCollector) collectors[i]).postCollection();
                }
            }
        }
    }

    static class BucketCollector extends DoubleBucketsAggregator.BucketCollector {

        final double term;

        long docCount;

        BucketCollector(NumericValuesSource valuesSource, double term, DoubleTermsAggregator parent) {
            super(valuesSource, parent.factories, parent);
            this.term = term;
        }

        @Override
        protected boolean onDoc(int doc, DoubleValues values) throws IOException {
            docCount++;
            return true;
        }

        DoubleTerms.Bucket buildBucket() {
            return new DoubleTerms.Bucket(term, docCount, buildAggregations(subAggregators));
        }
    }

}
