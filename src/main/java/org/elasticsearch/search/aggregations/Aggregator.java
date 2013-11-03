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

package org.elasticsearch.search.aggregations;

import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.factory.AggregatorFactories;

import java.io.IOException;

/**
 * Instantiated per named get in the request (every get type has a dedicated aggregator). The aggregator
 * handles the aggregation by providing the appropriate collector (see {@link #collector()}), and when the aggregation finishes, it is also used
 * for generating the result aggregation (see {@link #buildAggregation()}).
 */
public abstract class Aggregator extends AbstractAggregator {

    protected final AggregatorFactories factories;
    protected final OrdsAggregator[] ordsAggregators;

    protected Aggregator(String name, AggregatorFactories factories, int initialBucketsCount, AggregationContext context, Aggregator parent) {
        super(name, context, parent);
        this.factories = factories;
        assert factories != null : "sub-factories provided to BucketAggregator must not be null, use AggragatorFactories.EMPTY instead";
        this.ordsAggregators = factories.createOrdsAggregators(this, initialBucketsCount);
    }

    public abstract boolean shouldCollect();

    public abstract void collect(int doc) throws IOException;

    public abstract void postCollection();

    /**
     * @return  The aggregated & built get.
     */
    public abstract InternalAggregation buildAggregation();


}
