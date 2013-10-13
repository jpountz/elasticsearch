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

package org.elasticsearch.search.aggregations.bucket.single.global;

import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.single.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValueSpace;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class GlobalAggregator extends SingleBucketAggregator {

    long docCount;

    public GlobalAggregator(String name, List<Aggregator.Factory> factories, AggregationContext aggregationContext) {
        super(name, factories, aggregationContext, null);
    }

    @Override
    public Collector collector(Aggregator[] aggregators) {
        return new Collector(aggregators);
    }

    @Override
    public InternalGlobal buildAggregation(InternalAggregations aggregations) {
        return new InternalGlobal(name, docCount, aggregations);
    }

    class Collector extends BucketCollector {

        long docCount;

        Collector(Aggregator[] subAggregators) {
            super(subAggregators, GlobalAggregator.this);
        }

        @Override
        protected ValueSpace onDoc(int doc, ValueSpace context) throws IOException {
            docCount++;
            return context;
        }

        @Override
        protected void postCollection(Aggregator[] aggregators) {
            GlobalAggregator.this.docCount = docCount;
        }
    }

    public static class Factory extends Aggregator.CompoundFactory {

        public Factory(String name) {
            super(name);
        }

        @Override
        public GlobalAggregator create(AggregationContext context, Aggregator parent) {
            if (parent != null) {
                throw new AggregationExecutionException("Aggregation [" + parent.name() + "] cannot have a global " +
                        "sub-aggregation [" + name + "]. Global aggregations can only be defined as top level aggregations");
            }
            return new GlobalAggregator(name, factories, context);
        }

    }
}
