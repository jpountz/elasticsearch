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

package org.elasticsearch.search.aggregations.calc.numeric.stats.extended;

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
public class ExtendedStatsAggregator extends Aggregator {

    private final NumericValuesSource valuesSource;

    private long[] counts;
    private double[] sums;
    private double[] mins;
    private double[] maxes;
    private double[] sumOfSqrs;

    public ExtendedStatsAggregator(String name, int estimatedBucketsCount, NumericValuesSource valuesSource, AggregationContext context, Aggregator parent) {
        super(name, BucketAggregationMode.MULTI_BUCKETS, AggregatorFactories.EMPTY, estimatedBucketsCount, context, parent);
        this.valuesSource = valuesSource;
        if (valuesSource != null) {
            if (estimatedBucketsCount < 2) {
                counts = new long[1];
                sums = new double[1];
                mins = new double[1];
                mins[0] = Double.POSITIVE_INFINITY;
                maxes = new double[1];
                maxes[0] = Double.NEGATIVE_INFINITY;
                sumOfSqrs = new double[1];
            } else {
                counts = new long[estimatedBucketsCount];
                sums = new double[estimatedBucketsCount];
                mins = new double[estimatedBucketsCount];
                Arrays.fill(mins, Double.POSITIVE_INFINITY);
                maxes = new double[estimatedBucketsCount];
                Arrays.fill(maxes, Double.NEGATIVE_INFINITY);
                sumOfSqrs = new double[estimatedBucketsCount];
            }
        }
    }

    @Override
    public boolean shouldCollect() {
        return valuesSource != null;
    }

    @Override
    public void collect(int doc, int owningBucketOrdinal) throws IOException {
        assert valuesSource != null : "collect must only be called if #shouldCollect returns true";

        DoubleValues values = valuesSource.doubleValues();
        if (values == null) {
            return;
        }

        if (owningBucketOrdinal >= counts.length) {
            counts = ArrayUtil.grow(counts, owningBucketOrdinal + 1);
            sums = ArrayUtil.grow(sums, owningBucketOrdinal + 1);
            sumOfSqrs = ArrayUtil.grow(sumOfSqrs, owningBucketOrdinal + 1);
            int from = mins.length;
            mins = ArrayUtil.grow(mins, owningBucketOrdinal + 1);
            Arrays.fill(mins, from, mins.length, Double.POSITIVE_INFINITY);
            from = maxes.length;
            maxes = ArrayUtil.grow(maxes, owningBucketOrdinal + 1);
            Arrays.fill(maxes, from, maxes.length, Double.NEGATIVE_INFINITY);
        }

        int valuesCount = values.setDocument(doc);
        counts[owningBucketOrdinal] += valuesCount;
        for (int i = 0; i < valuesCount; i++) {
            double value = values.nextValue();
            sums[owningBucketOrdinal] += value;
            sumOfSqrs[owningBucketOrdinal] += value * value;
            mins[owningBucketOrdinal] = Math.min(mins[owningBucketOrdinal], value);
            maxes[owningBucketOrdinal] = Math.max(maxes[owningBucketOrdinal], value);
        }
    }

    @Override
    protected void doPostCollection() {
    }

    @Override
    public InternalAggregation buildAggregation(int owningBucketOrdinal) {
        if (valuesSource == null || owningBucketOrdinal >= counts.length) {
            return new InternalExtendedStats(name, 0, 0d, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0d);
        }
        return new InternalExtendedStats(name, counts[owningBucketOrdinal], sums[owningBucketOrdinal], mins[owningBucketOrdinal],
                maxes[owningBucketOrdinal], sumOfSqrs[owningBucketOrdinal]);
    }

    public static class Factory extends ValueSourceAggregatorFactory.LeafOnly<NumericValuesSource> {

        public Factory(String name, ValuesSourceConfig<NumericValuesSource> valuesSourceConfig) {
            super(name, InternalExtendedStats.TYPE.name(), valuesSourceConfig);
        }

        @Override
        public BucketAggregationMode bucketMode() {
            return BucketAggregationMode.MULTI_BUCKETS;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new ExtendedStatsAggregator(name, 0, null, aggregationContext, parent);
        }

        @Override
        protected Aggregator create(NumericValuesSource valuesSource, int expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            return new ExtendedStatsAggregator(name, expectedBucketsCount, valuesSource, aggregationContext, parent);
        }
    }
}
