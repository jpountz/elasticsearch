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
import org.elasticsearch.search.aggregations.calc.ValuesSourceCalcAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValueSpace;
import org.elasticsearch.search.aggregations.context.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.context.numeric.NumericValuesSource;

import java.io.IOException;

/**
 *
 */
public class NumericAggregator<A extends NumericAggregation> extends ValuesSourceCalcAggregator<NumericValuesSource> {

    protected final NumericAggregation.Factory<A> aggregationFactory;

    A stats;

    public NumericAggregator(String name,
                             NumericValuesSource valuesSource,
                             NumericAggregation.Factory<A> aggregationFactory,
                             AggregationContext aggregationContext,
                             Aggregator parent) {

        super(name, valuesSource, aggregationContext, parent);
        this.aggregationFactory = aggregationFactory;
    }

    @Override
    public Collector collector() {
        if (valuesSource == null) {
            return null;
        }
        A stats = aggregationFactory.create(name);
        return new Collector(valuesSource, stats);
    }

    @Override
    public NumericAggregation buildAggregation() {
        return stats != null ? stats : aggregationFactory.createUnmapped(name);
    }

    //========================================= Collector ===============================================//

    class Collector extends ValuesSourceCalcAggregator.Collector<NumericValuesSource> {

        private A stats;

        Collector(NumericValuesSource valuesSource, A stats) {
            super(valuesSource, NumericAggregator.this);
            this.stats = stats;
        }

        @Override
        public void collect(int doc, ValueSpace valueSpace) throws IOException {

            DoubleValues values = valuesSource.doubleValues();
            if (values == null) {
                return;
            }

            if (!values.hasValue(doc)) {
                return;
            }

            if (!values.isMultiValued()) {
                double value = values.getValue(doc);
                if (valueSpace.accept(valuesSource.key(), value)) {
                    stats.collect(doc, value);
                }
                return;
            }

            for (DoubleValues.Iter iter = values.getIter(doc); iter.hasNext();) {
                double value = iter.next();
                if (valueSpace.accept(valuesSource.key(), value)) {
                    stats.collect(doc, value);
                }
            }
        }

        @Override
        public void postCollection() {
            NumericAggregator.this.stats = stats;
        }
    }

    //============================================== Factory ===============================================//

    public static class Factory<A extends NumericAggregation> extends ValuesSourceCalcAggregator.Factory<NumericValuesSource> {

        private final NumericAggregation.Factory<A> aggregationFactory;

        public Factory(String name, ValuesSourceConfig<NumericValuesSource> valuesSourceConfig, NumericAggregation.Factory<A> aggregationFactory) {
            super(name, valuesSourceConfig);
            this.aggregationFactory = aggregationFactory;
        }

        @Override
        protected NumericAggregator<A> createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new NumericAggregator<A>(name, null, aggregationFactory, aggregationContext, parent);
        }

        @Override
        protected NumericAggregator<A> create(NumericValuesSource vs, AggregationContext aggregationContext, Aggregator parent) {
            return new NumericAggregator<A>(name, vs, aggregationFactory, aggregationContext, parent);
        }
    }

}
