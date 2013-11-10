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

package org.elasticsearch.search.aggregations.factory;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.Aggregator.BucketAggregationMode;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.context.AggregationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class AggregatorFactories {

    public static final AggregatorFactories EMPTY = new Empty();

    private final AggregatorFactory[] perBucket;
    private final AggregatorFactory[] ordinals;

    public static Builder builder() {
        return new Builder();
    }

    private AggregatorFactories(AggregatorFactory[] perBucket, AggregatorFactory[] ordinals) {
        this.perBucket = perBucket;
        this.ordinals = ordinals;
    }

    /** Create all aggregators so that they can be consumed with multiple buckets. */
    public Aggregator[] createSubAggregators(Aggregator parent, final long estimatedBucketsCount) {
        Aggregator[] aggregators = new Aggregator[count()];
        for (int i = 0; i < perBucket.length; i++) {
            final AggregatorFactory factory = perBucket[i];
            final Aggregator first = factory.create(parent.context(), parent, estimatedBucketsCount);
            aggregators[i] = new Aggregator(first.name(), BucketAggregationMode.MULTI_BUCKETS, AggregatorFactories.EMPTY, 1, first.context(), first.parent()) {

                ObjectArray<Aggregator> aggregators;

                {
                    aggregators = BigArrays.newObjectArray(estimatedBucketsCount);
                    aggregators.set(0, first);
                }

                @Override
                public boolean shouldCollect() {
                    return first.shouldCollect();
                }

                @Override
                protected void doPostCollection() {
                    for (long i = 0; i < aggregators.size(); ++i) {
                        final Aggregator aggregator = aggregators.get(i);
                        if (aggregator != null) {
                            aggregator.postCollection();
                        }
                    }
                }

                @Override
                public void collect(int doc, long owningBucketOrdinal) throws IOException {
                    aggregators = BigArrays.grow(aggregators, owningBucketOrdinal + 1);
                    Aggregator aggregator = aggregators.get(owningBucketOrdinal);
                    if (aggregator == null) {
                        aggregator = factory.create(parent.context(), parent, estimatedBucketsCount);
                        aggregators.set(owningBucketOrdinal, aggregator);
                    }
                    aggregator.collect(doc, 0);
                }

                @Override
                public InternalAggregation buildAggregation(long owningBucketOrdinal) {
                    if (owningBucketOrdinal >= aggregators.size() || aggregators.get(owningBucketOrdinal) == null) {
                        // nocommit: should we have an Aggregator.buildEmptyAggregation instead? or maybe return null and expect callers to deal with it?
                        return first.buildAggregation(1); // we know 1 is unused since we used 0
                    } else {
                        return aggregators.get(owningBucketOrdinal).buildAggregation(owningBucketOrdinal);
                    }
                }
            };
        }
        for (int i = 0; i < ordinals.length; i++) {
            aggregators[i+perBucket.length] = ordinals[i].create(parent.context(), parent, estimatedBucketsCount);
        }
        return aggregators;
    }

    public Aggregator[] createTopLevelAggregators(AggregationContext ctx) {
        Aggregator[] aggregators = new Aggregator[perBucket.length + ordinals.length];
        for (int i = 0; i < perBucket.length; i++) {
            aggregators[i] = perBucket[i].create(ctx, null, 0);
        }
        for (int i = 0; i < ordinals.length; i++) {
            aggregators[i+perBucket.length] = ordinals[i].create(ctx, null, 0);
        }
        return aggregators;
    }

    public int count() {
        return perBucket.length + ordinals.length;
    }

    void setParent(AggregatorFactory parent) {
        for (int i = 0; i < perBucket.length; i++) {
            perBucket[i].parent = parent;
        }
        for (int i = 0; i < ordinals.length; i++) {
            ordinals[i].parent = parent;
        }
    }

    public void validate() {
        for (int i = 0; i < perBucket.length; i++) {
            perBucket[i].validate();
        }
        for (int i = 0; i < ordinals.length; i++) {
            ordinals[i].validate();
        }
    }

    private final static class Empty extends AggregatorFactories {

        private static final AggregatorFactory[] EMPTY_FACTORIES = new AggregatorFactory[0];
        private static final Aggregator[] EMPTY_AGGREGATORS = new Aggregator[0];

        private Empty() {
            super(EMPTY_FACTORIES, EMPTY_FACTORIES);
        }

        @Override
        public int count() {
            return 0;
        }

        @Override
        public Aggregator[] createSubAggregators(Aggregator parent, long estimatedBucketsCount) {
            return EMPTY_AGGREGATORS;
        }

        @Override
        public Aggregator[] createTopLevelAggregators(AggregationContext ctx) {
            return EMPTY_AGGREGATORS;
        }

        @Override
        public void validate() {
        }

        @Override
        void setParent(AggregatorFactory parent) {
        }
    }

    public static class Builder {

        private List<AggregatorFactory> perBucket = new ArrayList<AggregatorFactory>();
        private List<AggregatorFactory> ordinals = new ArrayList<AggregatorFactory>();

        public Builder add(AggregatorFactory factory) {
            switch (factory.bucketMode()) {
                case PER_BUCKET:
                    perBucket.add(factory);
                    break;
                case MULTI_BUCKETS:
                    ordinals.add(factory);
                    break;
                default:
                    assert false : "there can only be two bucket modes [ PER_BUCKET, ORDINALS ]";
            }
            return this;
        }

        public AggregatorFactories build() {
            return new AggregatorFactories(perBucket.toArray(new AggregatorFactory[perBucket.size()]), ordinals.toArray(new AggregatorFactory[ordinals.size()]));
        }
    }
}
