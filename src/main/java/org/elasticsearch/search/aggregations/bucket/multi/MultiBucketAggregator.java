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

package org.elasticsearch.search.aggregations.bucket.multi;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.OrdsAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.factory.AggregatorFactories;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public abstract class MultiBucketAggregator extends Aggregator {

    protected final OrdsAggregator[] ordsAggregators;
    protected final OrdsAggregator.Collector[] ordsCollectors;

    protected MultiBucketAggregator(String name, AggregatorFactories factories, int initialNumberOfBuckets, AggregationContext context, Aggregator parent) {
        super(name, factories, context, parent);
        this.ordsAggregators = factories.createOrdsAggregators(this);
        this.ordsCollectors = new OrdsAggregator.Collector[ordsAggregators.length];
        for (int i = 0; i < ordsAggregators.length; i++) {
            ordsCollectors[i] = ordsAggregators[i].collector(initialNumberOfBuckets);
        }
    }


    public static abstract class BucketCollector extends org.elasticsearch.search.aggregations.bucket.BucketCollector {

        public BucketCollector(int ord, Aggregator[] aggregators, OrdsAggregator.Collector[] ordsCollectors) {
            super(ord, aggregators, ordsCollectors);
        }

        public InternalAggregations buildAggregations(OrdsAggregator[] ordsAggregators) {
            List<InternalAggregation> aggregations = new ArrayList<InternalAggregation>(aggregators.length + ordsAggregators.length);
            for (int i = 0; i < aggregators.length; i++) {
                aggregations.add(aggregators[i].buildAggregation());
            }
            for (int i = 0; i < ordsAggregators.length; i++) {
                aggregations.add(ordsAggregators[i].buildAggregation(ord));
            }
            return new InternalAggregations(aggregations);
        }

        @Override
        protected boolean onDoc(int doc) throws IOException {
            return true;
        }

        @Override
        protected void postCollection(Aggregator[] aggregators, long docCount) {
        }
    }

}
