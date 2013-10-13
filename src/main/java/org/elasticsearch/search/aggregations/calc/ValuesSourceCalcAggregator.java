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

package org.elasticsearch.search.aggregations.calc;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.ValuesSourceAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValuesSource;

/**
 *
 */
public abstract class ValuesSourceCalcAggregator<VS extends ValuesSource> extends ValuesSourceAggregator<VS> {

    public ValuesSourceCalcAggregator(String name, VS valuesSource, AggregationContext aggregationContext, Aggregator parent) {
        super(name, valuesSource, aggregationContext, parent);
    }

    protected static abstract class Collector<VS extends ValuesSource> implements Aggregator.Collector {

        protected Aggregator aggregator;
        protected VS valuesSource;

        protected Collector(VS valuesSource, Aggregator aggregator) {
            this.valuesSource = valuesSource;
            this.aggregator = aggregator;
        }
    }

}
