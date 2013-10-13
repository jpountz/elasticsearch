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

package org.elasticsearch.search.aggregations.calc.bytes;

import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.calc.ValuesSourceCalcAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValueSpace;
import org.elasticsearch.search.aggregations.context.ValuesSource;
import org.elasticsearch.search.aggregations.context.bytes.BytesValuesSource;

import java.io.IOException;

/**
 *
 */
public abstract class BytesCalcAggregator extends ValuesSourceCalcAggregator<BytesValuesSource> {

    public BytesCalcAggregator(String name, BytesValuesSource valuesSource, AggregationContext aggregationContext, Aggregator parent) {
        super(name, valuesSource, aggregationContext, parent);
    }

    protected static abstract class Collector extends ValuesSourceCalcAggregator.Collector<ValuesSource> {

        protected Collector(ValuesSource valuesSource, Aggregator aggregator) {
            super(valuesSource, aggregator);
        }

        @Override
        public void collect(int doc, ValueSpace valueSpace) throws IOException {
            collect(doc, valuesSource.bytesValues(), valueSpace);
        }

        protected abstract void collect(int doc, BytesValues values, ValueSpace context) throws IOException;
    }

}
