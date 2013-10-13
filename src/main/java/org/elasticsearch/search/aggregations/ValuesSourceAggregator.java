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
import org.elasticsearch.search.aggregations.context.ValuesSource;
import org.elasticsearch.search.aggregations.context.ValuesSourceConfig;

/**
 * An aggregator that aggregates based on values that are provided by a {@link ValuesSource}.
 */
public abstract class ValuesSourceAggregator<VS extends ValuesSource> extends Aggregator {

    protected final VS valuesSource;

    public ValuesSourceAggregator(String name, VS valuesSource, AggregationContext aggregationContext, Aggregator parent) {
        super(name, aggregationContext, parent);
        this.valuesSource = valuesSource;
    }

    private static interface ValuesSourceConfigurable {

        ValuesSourceConfig valuesSourceConfig();

    }

    public static abstract class Factory<VS extends ValuesSource> extends Aggregator.Factory implements ValuesSourceConfigurable {

        protected ValuesSourceConfig<VS> valuesSourceConfig;

        protected Factory(String name, ValuesSourceConfig<VS> valuesSourceConfig) {
            super(name);
            this.valuesSourceConfig = valuesSourceConfig;
        }

        @Override
        public ValuesSourceConfig valuesSourceConfig() {
            return valuesSourceConfig;
        }

        @Override
        public final Aggregator create(AggregationContext context, Aggregator parentAggregator) {
            if (valuesSourceConfig.unmapped()) {
                return createUnmapped(context, parentAggregator);
            }
            VS vs = context.valuesSource(valuesSourceConfig);
            return create(vs, context, parentAggregator);
        }

        @Override
        public void validate() {
            if (!valuesSourceConfig.valid()) {
                valuesSourceConfig = resolveValuesSourceConfigFromAncestors(name, parent, valuesSourceConfig.valueSourceType());
            }
        }

        protected abstract Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent);

        protected abstract Aggregator create(VS valuesSource, AggregationContext aggregationContext, Aggregator parent);
    }

    public static abstract class CompoundFactory<VS extends ValuesSource> extends Aggregator.CompoundFactory implements ValuesSourceConfigurable {

        protected ValuesSourceConfig<VS> valueSourceConfig;

        protected CompoundFactory(String name, ValuesSourceConfig<VS> valueSourceConfig) {
            super(name);
            this.valueSourceConfig = valueSourceConfig;
        }

        @Override
        public ValuesSourceConfig valuesSourceConfig() {
            return valueSourceConfig;
        }

        @Override
        public final Aggregator create(AggregationContext context, Aggregator parentAggregator) {
            if (valueSourceConfig.unmapped()) {
                return createUnmapped(context, parentAggregator);
            }
            VS vs = context.valuesSource(valueSourceConfig);
            return create(vs, context, parentAggregator);
        }

        @Override
        public void validate() {
            if (!valueSourceConfig.valid()) {
                valueSourceConfig = resolveValuesSourceConfigFromAncestors(name, parent, valueSourceConfig.valueSourceType());
            }
            for (Aggregator.Factory factory : factories) {
                factory.validate();
            }
        }

        protected abstract Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent);

        protected abstract Aggregator create(VS valuesSource, AggregationContext aggregationContext, Aggregator parent);
    }

    private static <VS extends ValuesSource> ValuesSourceConfig<VS> resolveValuesSourceConfigFromAncestors(String aggName, Aggregator.Factory parent, Class<VS> requiredValuesSourceType) {
        ValuesSourceConfig config;
        while (parent != null) {
            if (parent instanceof ValuesSourceConfigurable) {
                config = ((ValuesSourceConfigurable) parent).valuesSourceConfig();
                if (config != null && config.valid()) {
                    if (requiredValuesSourceType == null || requiredValuesSourceType.isAssignableFrom(config.valueSourceType())) {
                        return (ValuesSourceConfig<VS>) config;
                    }
                }
            }
            parent = parent.parent;
        }
        throw new AggregationExecutionException("could not find the appropriate value context to perform aggregation [" + aggName + "]");
    }

}
