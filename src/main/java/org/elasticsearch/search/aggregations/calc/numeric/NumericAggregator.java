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

package org.elasticsearch.search.aggregations.calc.numeric;

import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.context.numeric.NumericValuesSource;
import org.elasticsearch.search.aggregations.factory.AggregatorFactories;
import org.elasticsearch.search.aggregations.factory.ValueSourceAggregatorFactory;

import java.io.IOException;

/**
 *
 */
public class NumericAggregator<A extends NumericAggregation> extends Aggregator {

    protected final NumericAggregation.Factory<A> aggregationFactory;
    protected final NumericValuesSource valuesSource;

    protected final A stats;

    public NumericAggregator(String name,
                             NumericValuesSource valuesSource,
                             NumericAggregation.Factory<A> aggregationFactory,
                             AggregationContext aggregationContext,
                             Aggregator parent) {

        super(name, AggregatorFactories.EMPTY, aggregationContext, parent);
        this.aggregationFactory = aggregationFactory;
        this.valuesSource = valuesSource;
        this.stats = valuesSource == null ? aggregationFactory.createUnmapped(name) : aggregationFactory.create(name);
    }

    @Override
    public Collector collector() {
        return valuesSource != null ? new Collector() : null;
    }

    @Override
    public NumericAggregation buildAggregation() {
        return stats;
    }

    //========================================= Collector ===============================================//

    class Collector implements Aggregator.Collector {

        @Override
        public void collect(int doc) throws IOException {

            DoubleValues values = valuesSource.doubleValues();
            if (values == null) {
                return;
            }

            int valuesCount = values.setDocument(doc);
            for (int i = 0; i < valuesCount; i++) {
                stats.collect(doc, values.nextValue());
            }
        }

        @Override
        public void postCollection() {
        }
    }

    //============================================== Factory ===============================================//

    public static class Factory<A extends NumericAggregation> extends ValueSourceAggregatorFactory.Normal<NumericValuesSource> {

        private final NumericAggregation.Factory<A> aggregationFactory;

        public Factory(String name, ValuesSourceConfig<NumericValuesSource> valuesSourceConfig, NumericAggregation.Factory<A> aggregationFactory) {
            super(name, aggregationFactory.type(), valuesSourceConfig);
            this.aggregationFactory = aggregationFactory;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new NumericAggregator<A>(name, null, aggregationFactory, aggregationContext, parent);
        }

        @Override
        protected Aggregator create(NumericValuesSource valuesSource, AggregationContext aggregationContext, Aggregator parent) {
            return new NumericAggregator<A>(name, valuesSource, aggregationFactory, aggregationContext, parent);
        }

    }

}
