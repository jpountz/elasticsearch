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

package org.elasticsearch.search.aggregations.bucket.single;

import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.BucketCollector;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.factory.AggregatorFactories;

import java.util.ArrayList;
import java.util.List;

/**
 * A bucket aggregator that creates a single bucket
 */
public abstract class SingleBucketAggregator extends Aggregator {

    private final Aggregator[] aggregators;

    /**
     * Constructs a new single bucket aggregator.
     *
     * @param name                  The aggregation name.
     * @param factories             The aggregator factories of all sub-aggregations associated with the bucket of this aggregator.
     * @param aggregationContext    The aggregation context.
     * @param parent                The parent aggregator of this aggregator.
     */
    protected SingleBucketAggregator(String name, AggregatorFactories factories,
                                     AggregationContext aggregationContext, Aggregator parent) {
        super(name, factories, aggregationContext, parent);
        aggregators = factories.createAggregators(this);
    }

    @Override
    public final Aggregator.Collector collector() {
        return collector(aggregators, ordsAggregators);
    }

    protected abstract Collector collector(Aggregator[] aggregators, OrdsAggregator[] ordsAggregators);

    @Override
    public final InternalAggregation buildAggregation() {
        List<InternalAggregation> aggregations = new ArrayList<InternalAggregation>(aggregators.length + ordsAggregators.length);
        for (int i = 0; i < aggregators.length; i++) {
            aggregations.add(aggregators[i].buildAggregation());
        }
        for (int i = 0; i < ordsAggregators.length; i++) {
            aggregations.add(ordsAggregators[i].buildAggregation(0));
        }
        return buildAggregation(new InternalAggregations(aggregations));
    }

    protected abstract InternalAggregation buildAggregation(InternalAggregations aggregations);

    protected static abstract class Collector extends BucketCollector {

        private final OrdsAggregator[] ordsAggregators;

        public Collector(Aggregator[] aggregators, OrdsAggregator[] ordsAggregators) {
            super(aggregators, ordsCollectors(ordsAggregators));
            this.ordsAggregators = ordsAggregators;
        }

        @Override
        protected void postCollection(Aggregator[] aggregators, long docCount) {
            for (int i = 0; i < ordsCollectors.length; i++) {
                ordsCollectors[i].postCollection();
            }
            postCollection(aggregators, ordsAggregators, docCount);
        }

        protected abstract void postCollection(Aggregator[] aggregators, OrdsAggregator[] ordsAggregators, long docCount);


        private static OrdsAggregator.Collector[] ordsCollectors(OrdsAggregator[] aggregators) {
            OrdsAggregator.Collector[] collectors = new OrdsAggregator.Collector[aggregators.length];
            for (int i = 0; i < aggregators.length; i++) {
                collectors[i] = aggregators[i].collector(0);
            }
            return collectors;
        }
    }

}
