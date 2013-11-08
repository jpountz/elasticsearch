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

import org.apache.lucene.util.ArrayUtil;
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
 *
 * This aggregator works in a multi-bucket mode, that is, when serves as a sub-aggregator, a single aggregator instance aggregates the
 * counts for all buckets owned by the parent aggregator)
 */
public class CountAggregator extends Aggregator {

    private final BytesValuesSource valuesSource;

    // a count per bucket
    long[] counts;

    public CountAggregator(String name, int expectedBucketsCount, BytesValuesSource valuesSource, AggregationContext aggregationContext, Aggregator parent) {
        super(name, BucketAggregationMode.MULTI_BUCKETS, AggregatorFactories.EMPTY, 0, aggregationContext, parent);
        this.valuesSource = valuesSource;
        if (valuesSource != null) {
            // expectedBucketsCount == 0 means it's a top level bucket
            this.counts = expectedBucketsCount < 2 ? new long[1] : new long[expectedBucketsCount];
        }
    }

    @Override
    public boolean shouldCollect() {
        return valuesSource != null;
    }

    @Override
    public void collect(int doc, int owningBucketOrdinal) throws IOException {
        BytesValues values = valuesSource.bytesValues();
        if (values == null) {
            return;
        }
        if (owningBucketOrdinal >= counts.length) {
            counts = ArrayUtil.grow(counts, owningBucketOrdinal+1);
        }
        counts[owningBucketOrdinal] += values.setDocument(doc);
    }

    @Override
    public InternalAggregation buildAggregation(int owningBucketOrdinal) {
        if (valuesSource == null) {
            return new InternalCount(name, 0);
        }
        if (owningBucketOrdinal >= counts.length) {
            return new InternalCount(name, 0);
        }
        return new InternalCount(name, counts[owningBucketOrdinal]);
    }

    public static class Factory extends ValueSourceAggregatorFactory.LeafOnly<BytesValuesSource> {

        public Factory(String name, ValuesSourceConfig<BytesValuesSource> valuesSourceBuilder) {
            super(name, InternalCount.TYPE.name(), valuesSourceBuilder);
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new CountAggregator(name, 0, null, aggregationContext, parent);
        }

        @Override
        protected Aggregator create(BytesValuesSource valuesSource, int expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            return new CountAggregator(name, expectedBucketsCount, valuesSource, aggregationContext, parent);
        }

        @Override
        public BucketAggregationMode bucketMode() {
            return BucketAggregationMode.MULTI_BUCKETS;
        }

    }

}
