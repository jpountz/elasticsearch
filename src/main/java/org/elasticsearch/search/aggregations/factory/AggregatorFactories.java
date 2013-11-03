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
import org.elasticsearch.search.aggregations.OrdsAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class AggregatorFactories {

    public static final AggregatorFactories EMPTY = new Empty();

    private List<AggregatorFactory> aggregatorFactories;
    private List<OrdsAggregatorFactory> ordsAggregatorFactories;

    public AggregatorFactories() {
        this(new ArrayList<AggregatorFactory>(), new ArrayList<OrdsAggregatorFactory>());
    }

    private AggregatorFactories(List<AggregatorFactory> aggregatorFactories, List<OrdsAggregatorFactory> ordsAggregatorFactories) {
        this.aggregatorFactories = aggregatorFactories;
        this.ordsAggregatorFactories = ordsAggregatorFactories;
    }

    public AggregatorFactories add(AggregatorFactory factory) {
        if (factory instanceof OrdsAggregatorFactory) {
            ordsAggregatorFactories.add((OrdsAggregatorFactory) factory);
            return this;
        }
        aggregatorFactories.add(factory);
        return this;
    }

    public Aggregator[] createAggregators(Aggregator parent) {
        int i = 0;
        Aggregator[] aggregators = new Aggregator[aggregatorFactories.size()];
        for (AggregatorFactory factory : aggregatorFactories) {
            aggregators[i++] = (Aggregator) factory.create(parent.context(), parent);
        }
        return aggregators;
    }

    public OrdsAggregator[] createOrdsAggregators(Aggregator parent, int initialOrdCount) {
        int i = 0;
        OrdsAggregator[] aggregators = new OrdsAggregator[ordsAggregatorFactories.size()];
        for (OrdsAggregatorFactory factory : ordsAggregatorFactories) {
            aggregators[i++] = factory.create(parent.context(), parent, initialOrdCount);
        }
        return aggregators;
    }

    public Aggregator[] createTopLevelAggregators(AggregationContext ctx) {
        int i = 0;
        Aggregator[] aggregators = new Aggregator[ordsAggregatorFactories.size() + aggregatorFactories.size()];
        for (OrdsAggregatorFactory factory : ordsAggregatorFactories) {
            aggregators[i++] = factory.create(ctx, null).asAggregator();
        }
        for (AggregatorFactory factory : aggregatorFactories) {
            aggregators[i++] = (Aggregator) factory.create(ctx, null);
        }
        return aggregators;
    }

    public int count() {
        return aggregatorFactories.size() + ordsAggregatorFactories.size();
    }

    void setParent(AggregatorFactory parent) {
        for (AggregatorFactory factory : aggregatorFactories) {
            factory.parent = parent;
        }
        for (AggregatorFactory factory : ordsAggregatorFactories) {
            factory.parent = parent;
        }
    }

    public void validate() {
        for (AggregatorFactory factory : aggregatorFactories) {
            factory.validate();
        }
        for (AggregatorFactory factory : ordsAggregatorFactories) {
            factory.validate();
        }
    }

    private final static class Empty extends AggregatorFactories {

        private static final Aggregator[] EMPTY_BUCKETS = new Aggregator[0];
        private static final OrdsAggregator[] EMPTY_LEAVES = new OrdsAggregator[0];

        private Empty() {
            super((List<AggregatorFactory>) Collections.EMPTY_LIST, (List<OrdsAggregatorFactory>) Collections.EMPTY_LIST);
        }

        @Override
        public Aggregator[] createAggregators(Aggregator parent) {
            return EMPTY_BUCKETS;
        }

        @Override
        public OrdsAggregator[] createOrdsAggregators(Aggregator parent, int initialOrdCount) {
            return EMPTY_LEAVES;
        }

        @Override
        public Aggregator[] createTopLevelAggregators(AggregationContext ctx) {
            return EMPTY_BUCKETS;
        }

        @Override
        public void validate() {
        }

        @Override
        void setParent(AggregatorFactory parent) {
        }
    }
}
