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

package org.elasticsearch.search.aggregations.bucket.single.missing;

import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.OrdsAggregator;
import org.elasticsearch.search.aggregations.bucket.BucketCollector;
import org.elasticsearch.search.aggregations.bucket.single.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValuesSource;
import org.elasticsearch.search.aggregations.context.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.factory.AggregatorFactories;
import org.elasticsearch.search.aggregations.factory.ValueSourceAggregatorFactory;

import java.io.IOException;

/**
 *
 */
public class MissingAggregator extends SingleBucketAggregator {

    private ValuesSource valuesSource;

    public MissingAggregator(String name, AggregatorFactories factories, ValuesSource valuesSource,
                             AggregationContext aggregationContext, Aggregator parent) {
        super(name, factories, aggregationContext, parent);
        this.valuesSource = valuesSource;
    }

    @Override
    protected BucketCollector collector(Aggregator[] aggregators, OrdsAggregator[] ordsAggregators) {
        return new Collector(aggregators, ordsAggregators);
    }

    @Override
    protected InternalAggregation buildAggregation(InternalAggregations aggregations, long docCount) {
        return new InternalMissing(name, docCount, aggregations);
    }

    class Collector extends BucketCollector {

        Collector(Aggregator[] aggregators, OrdsAggregator[] ordsAggregators) {
            super(aggregators, ordsAggregators);
        }

        @Override
        protected boolean onDoc(int doc) throws IOException {
            if (valuesSource == null) {
                return true;
            }
            BytesValues values = valuesSource.bytesValues();
            if (values.setDocument(doc) == 0) {
                return true;
            }
            return false;
        }
    }

    public static class Factory extends ValueSourceAggregatorFactory.Normal {

        public Factory(String name, ValuesSourceConfig valueSourceConfig) {
            super(name, InternalMissing.TYPE.name(), valueSourceConfig);
        }

        @Override
        protected MissingAggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new MissingAggregator(name, factories, null, aggregationContext, parent);
        }

        @Override
        protected MissingAggregator create(ValuesSource valuesSource, AggregationContext aggregationContext, Aggregator parent) {
            return new MissingAggregator(name, factories, valuesSource, aggregationContext, parent);
        }
    }

}


