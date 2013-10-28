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
import org.elasticsearch.search.aggregations.ValuesSourceAggregator;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.BytesBucketsAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValuesSource;
import org.elasticsearch.search.aggregations.context.ValuesSourceConfig;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class MissingAggregator extends BytesBucketsAggregator {

    private final Aggregator[] subAggregators;

    long docCount;

    public MissingAggregator(String name, List<Aggregator.Factory> factories, ValuesSource valuesSource,
                             AggregationContext aggregationContext, Aggregator parent) {
        super(name, valuesSource, aggregationContext, parent);
        this.subAggregators = BucketsAggregator.createSubAggregators(factories, this);
    }

    @Override
    public Aggregator.Collector collector() {
        return valuesSource != null ? new Collector(valuesSource, subAggregators) : new AllMissingCollector(subAggregators, this);
    }

    @Override
    public InternalAggregation buildAggregation() {
        return new InternalMissing(name, docCount, BucketsAggregator.buildAggregations(subAggregators));
    }

    class Collector extends BytesBucketsAggregator.BucketCollector {

        private long docCount;

        Collector(ValuesSource valuesSource, Aggregator[] subAggregators) {
            super(valuesSource, subAggregators, MissingAggregator.this);
        }

        @Override
        protected boolean onDoc(int doc, BytesValues values) throws IOException {
            if (!values.hasValue(doc)) {
                docCount++;
                return true;
            }
            return false;
        }

        @Override
        protected void doPostCollection() {
            MissingAggregator.this.docCount = docCount;
        }
    }

    public class AllMissingCollector extends BucketsAggregator.BucketCollector {

        private long docCount;

        public AllMissingCollector(Aggregator[] subAggregators, Aggregator aggregator) {
            super(subAggregators, aggregator);
        }

        @Override
        protected boolean onDoc(int doc) throws IOException {
            docCount++;
            return true;
        }

        @Override
        protected void postCollection(Aggregator[] aggregators) {
            MissingAggregator.this.docCount = docCount;
        }

    }

    public static class Factory extends ValuesSourceAggregator.CompoundFactory<ValuesSource> {

        public Factory(String name, ValuesSourceConfig valueSourceConfig) {
            super(name, valueSourceConfig);
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


