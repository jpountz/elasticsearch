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

package org.elasticsearch.search.aggregations.calc.numeric.avg;

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

/**
 *
 */
public class AvgAggregator extends Aggregator {

    private final NumericValuesSource valuesSource;

    private long[] counts;
    private double[] sums;


    public AvgAggregator(String name, int estimatedBucketsCount, NumericValuesSource valuesSource, AggregationContext context, Aggregator parent) {
        super(name, BucketAggregationMode.MULTI_BUCKETS, AggregatorFactories.EMPTY, estimatedBucketsCount, context, parent);
        this.valuesSource = valuesSource;
        if (valuesSource != null) {
            if (estimatedBucketsCount < 2) {
                counts = new long[1];
                sums = new double[1];
            } else {
                counts = new long[estimatedBucketsCount];
                sums = new double[estimatedBucketsCount];
            }
        }
    }

    @Override
    public boolean shouldCollect() {
        return valuesSource != null;
    }

    @Override
    public void collect(int doc, int owningBucketOrdinal) throws IOException {
        assert valuesSource != null : "if value source is null, collect should never be called";

        DoubleValues values = valuesSource.doubleValues();
        if (values == null) {
            return;
        }

        if (owningBucketOrdinal >= counts.length) {
            counts = ArrayUtil.grow(counts, owningBucketOrdinal + 1);
            sums = ArrayUtil.grow(sums, owningBucketOrdinal + 1);
        }

        int valueCount = values.setDocument(doc);
        counts[owningBucketOrdinal] += valueCount;
        for (int i = 0; i < valueCount; i++) {
            sums[owningBucketOrdinal] += values.nextValue();
        }

    }

    @Override
    public InternalAggregation buildAggregation(int owningBucketOrdinal) {
        if (valuesSource == null || owningBucketOrdinal >= counts.length) {
            return new InternalAvg(name, 0l, 0);
        }
        return new InternalAvg(name, sums[owningBucketOrdinal], counts[owningBucketOrdinal]);
    }

    public static class Factory extends ValueSourceAggregatorFactory.LeafOnly<NumericValuesSource> {

        public Factory(String name, String type, ValuesSourceConfig<NumericValuesSource> valuesSourceConfig) {
            super(name, type, valuesSourceConfig);
        }

        @Override
        public BucketAggregationMode bucketMode() {
            return BucketAggregationMode.MULTI_BUCKETS;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new AvgAggregator(name, 0, null, aggregationContext, parent);
        }

        @Override
        protected Aggregator create(NumericValuesSource valuesSource, int expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            return new AvgAggregator(name, expectedBucketsCount, valuesSource, aggregationContext, parent);
        }
    }
}
