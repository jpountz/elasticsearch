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

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.OrdsAggregator;
import org.elasticsearch.search.aggregations.bucket.BucketCollector;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.factory.AggregatorFactories;

import java.io.IOException;

/**
 * A bucket aggregator that creates a single bucket
 */
public abstract class SingleBucketAggregator extends Aggregator {

    private final Aggregator[] aggregators;
    protected BucketCollector collector;

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
        super(name, factories, 1, aggregationContext, parent);
        aggregators = factories.createAggregators(this);
        collector = collector(aggregators, ordsAggregators);
        assert collector != null : "A single bucket collector must have a non-null collector (created by the #collector(Aggregator[], OrdsAggregator[]) callback method";
    }

    /**
     * Constructs and returns a non-null bucket collector that will be responsible for the aggregation of the single bucket this aggregator
     * represents.
     *
     * This method is called during initialization, meaning, sub-classes cannot rely on class internal class state for deciding which
     * collector should be returned (it could be that this callback is called before other internal state is initialized)
     *
     * As mentioned above. The returned value <strong>must not be {@code null}</strong>
     */
    protected abstract BucketCollector collector(Aggregator[] aggregators, OrdsAggregator[] ordsAggregators);

    @Override
    public boolean shouldCollect() {
        return true;
    }

    @Override
    public void collect(int doc) throws IOException {
        collector.collect(doc);
    }

    @Override
    public void postCollection() {
        collector.postCollection();
    }

    @Override
    public final InternalAggregation buildAggregation() {
        return buildAggregation(collector.buildAggregations(), collector.docCount());
    }

    protected abstract InternalAggregation buildAggregation(InternalAggregations aggregations, long docCount);

}
