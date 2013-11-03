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

public abstract class OrdsAggregator extends AbstractAggregator {

    protected OrdsAggregator(String name, AggregationContext context, Aggregator parent) {
        super(name, context, parent);
    }

    public Aggregator asAggregator() {
        return new AsAggregatorWrapper(this);
    }

    public abstract boolean shouldCollect();

    public abstract void collect(int doc, int ord) throws IOException;

    public abstract void postCollection();


    /**
     * @return  The aggregated & built get.
     */
    public abstract InternalAggregation buildAggregation(int ord);


    static class AsAggregatorWrapper extends Aggregator {

        private final OrdsAggregator inner;

        AsAggregatorWrapper(OrdsAggregator inner) {
            super(inner.name, AggregatorFactories.EMPTY, 1, inner.context, inner.parent);
            this.inner = inner;
        }

        @Override
        public boolean shouldCollect() {
            return inner.shouldCollect();
        }

        @Override
        public void collect(int doc) throws IOException {
            inner.collect(doc, 0);
        }

        @Override
        public void postCollection() {
            inner.postCollection();
        }

        @Override
        public InternalAggregation buildAggregation() {
            return inner.buildAggregation(0);
        }
    }


}
