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

package org.elasticsearch.search.aggregations.calc.bytes.count;

import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.context.bytes.BytesValuesSource;
import org.elasticsearch.search.aggregations.factory.AggregatorFactories;
import org.elasticsearch.search.aggregations.factory.ValueSourceAggregatorFactory;

import java.io.IOException;

/**
 * A field data based aggregator that counts the number of values a specific field has within the aggregation context.
 */
public class CountAggregator extends Aggregator {

    private final BytesValuesSource valuesSource;

    long count;

    public CountAggregator(String name, BytesValuesSource valuesSource, AggregationContext aggregationContext, Aggregator parent) {
        super(name, AggregatorFactories.EMPTY, aggregationContext, parent);
        this.valuesSource = valuesSource;
    }

    @Override
    public Aggregator.Collector collector() {
        return valuesSource != null ? new Collector() : null;
    }

    @Override
    public InternalAggregation buildAggregation() {
        return new InternalCount(name, count);
    }

    class Collector implements Aggregator.Collector {

        long count;

        @Override
        public void collect(int doc) throws IOException {
            BytesValues values = valuesSource.bytesValues();
            if (values == null) {
                return;
            }
            int valuesCount = values.setDocument(doc);
            count += valuesCount;
        }

        @Override
        public void postCollection() {
            CountAggregator.this.count = count;
        }
    }

    public static class Factory extends ValueSourceAggregatorFactory.Normal<BytesValuesSource> {

        public Factory(String name, ValuesSourceConfig<BytesValuesSource> valuesSourceBuilder) {
            super(name, InternalCount.TYPE.name(), valuesSourceBuilder);
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new CountAggregator(name, null, aggregationContext, parent);
        }

        @Override
        protected Aggregator create(BytesValuesSource valuesSource, AggregationContext aggregationContext, Aggregator parent) {
            return new CountAggregator(name, valuesSource, aggregationContext, parent);
        }
    }

}
