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

        super(name, BucketAggregationMode.PER_BUCKET, AggregatorFactories.EMPTY, 0, aggregationContext, parent);
        this.aggregationFactory = aggregationFactory;
        this.valuesSource = valuesSource;
        this.stats = valuesSource == null ? aggregationFactory.createUnmapped(name) : aggregationFactory.create(name);
    }

    @Override
    public boolean shouldCollect() {
        return valuesSource != null;
    }

    @Override
    public void collect(int doc, int owningBucketOrdinal) throws IOException {
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
    protected void doPostCollection() {
    }

    @Override
    public NumericAggregation buildAggregation(int owningBucketOrdinal) {
        return stats;
    }


    //============================================== Factory ===============================================//

    public static class Factory<A extends NumericAggregation> extends ValueSourceAggregatorFactory.LeafOnly<NumericValuesSource> {

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
        public BucketAggregationMode bucketMode() {
            return BucketAggregationMode.PER_BUCKET;
        }

        @Override
        protected Aggregator create(NumericValuesSource valuesSource, int expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            return new NumericAggregator<A>(name, valuesSource, aggregationFactory, aggregationContext, parent);
        }

    }

}
