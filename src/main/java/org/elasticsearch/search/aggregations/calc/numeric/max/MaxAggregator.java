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

package org.elasticsearch.search.aggregations.calc.numeric.max;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.context.numeric.NumericValuesSource;
import org.elasticsearch.search.aggregations.factory.AggregatorFactories;
import org.elasticsearch.search.aggregations.factory.ValueSourceAggregatorFactory;

import java.io.IOException;
import java.util.Arrays;

/**
 *
 */
public class MaxAggregator extends Aggregator {

    private final NumericValuesSource valuesSource;

    private double[] maxes;

    public MaxAggregator(String name, int estimatedBucketsCount, NumericValuesSource valuesSource, AggregationContext context, Aggregator parent) {
        super(name, BucketAggregationMode.MULTI_BUCKETS, AggregatorFactories.EMPTY, estimatedBucketsCount, context, parent);
        this.valuesSource = valuesSource;
        if (valuesSource != null) {
            if (estimatedBucketsCount < 2) {
                maxes =  new double[1];
                maxes[0] = Double.NEGATIVE_INFINITY;
            } else {
                maxes = new double[estimatedBucketsCount];
                Arrays.fill(maxes, Double.NEGATIVE_INFINITY);
            }
        }
    }

    @Override
    public boolean shouldCollect() {
        return valuesSource != null;
    }

    @Override
    public void collect(int doc, int owningBucketOrdinal) throws IOException {
        assert valuesSource != null : "collect should only be called when value source is not null";

        DoubleValues values = valuesSource.doubleValues();
        if (values == null) {
            return;
        }

        if (owningBucketOrdinal >= maxes.length) {
            int from = maxes.length;
            maxes = ArrayUtil.grow(maxes, owningBucketOrdinal + 1);
            Arrays.fill(maxes, from, maxes.length, Double.NEGATIVE_INFINITY);
        }

        int valueCount = values.setDocument(doc);
        for (int i = 0; i < valueCount; i++) {
            maxes[owningBucketOrdinal] = Math.max(maxes[owningBucketOrdinal], values.nextValue());
        }
    }

    @Override
    protected void doPostCollection() {
    }

    @Override
    public InternalAggregation buildAggregation(int owningBucketOrdinal) {
        if (valuesSource == null || owningBucketOrdinal >= maxes.length) {
            return new InternalMax(name, Double.NEGATIVE_INFINITY);
        }
        return new InternalMax(name, maxes[owningBucketOrdinal]);
    }


    public static class Factory extends ValueSourceAggregatorFactory.LeafOnly<NumericValuesSource> {

        public Factory(String name, ValuesSourceConfig<NumericValuesSource> valuesSourceConfig) {
            super(name, InternalMax.TYPE.name(), valuesSourceConfig);
        }

        @Override
        public BucketAggregationMode bucketMode() {
            return BucketAggregationMode.MULTI_BUCKETS;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new MaxAggregator(name, 0, null, aggregationContext, parent);
        }

        @Override
        protected Aggregator create(NumericValuesSource valuesSource, int expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            return new MaxAggregator(name, expectedBucketsCount, valuesSource, aggregationContext, parent);
        }
    }
}
