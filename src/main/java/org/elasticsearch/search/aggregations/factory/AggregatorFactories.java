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

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;

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

    public Aggregator[] createBucketAggregators(Aggregator parent, Aggregator[] multiBucketAggregators, int estimatedBucketsCount) {
        Aggregator[] aggregators = new Aggregator[perBucket.length + multiBucketAggregators.length];
        for (int i = 0; i < perBucket.length; i++) {
            aggregators[i] = perBucket[i].create(parent.context(), parent, estimatedBucketsCount);
        }
        for (int i = 0; i < multiBucketAggregators.length; i++) {
            aggregators[i+perBucket.length] = multiBucketAggregators[i];
        }
        return aggregators;
    }

    public Aggregator[] createMultiBucketAggregators(Aggregator parent, int estimatedBucketsCount) {
        Aggregator[] aggregators = new Aggregator[ordinals.length];
        for (int i = 0; i < ordinals.length; i++) {
            aggregators[i] = ordinals[i].create(parent.context(), parent, estimatedBucketsCount);
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

    public int perBucketCount() {
        return perBucket.length;
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
        public Aggregator[] createMultiBucketAggregators(Aggregator parent, int estimatedBucketsCount) {
            return EMPTY_AGGREGATORS;
        }

        @Override
        public int count() {
            return 0;
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
