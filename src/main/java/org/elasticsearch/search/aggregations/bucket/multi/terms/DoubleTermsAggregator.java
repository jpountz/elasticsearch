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

package org.elasticsearch.search.aggregations.bucket.multi.terms;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.numeric.NumericValuesSource;
import org.elasticsearch.search.aggregations.factory.AggregatorFactories;

import java.io.IOException;
import java.util.Arrays;

/**
 *
 */
public class DoubleTermsAggregator extends Aggregator {

    private static final int INITIAL_CAPACITY = 50; // TODO sizing

    private final InternalOrder order;
    private final int requiredSize;
    private final NumericValuesSource valuesSource;
    private final LongHash bucketOrds;
    private LongArray counts;

    public DoubleTermsAggregator(String name, AggregatorFactories factories, NumericValuesSource valuesSource,
                               InternalOrder order, int requiredSize, AggregationContext aggregationContext, Aggregator parent) {
        super(name, BucketAggregationMode.PER_BUCKET, factories, INITIAL_CAPACITY, aggregationContext, parent);
        this.valuesSource = valuesSource;
        this.order = order;
        this.requiredSize = requiredSize;
        bucketOrds = new LongHash(INITIAL_CAPACITY);
        counts = BigArrays.newLongArray(INITIAL_CAPACITY);
    }

    @Override
    public boolean shouldCollect() {
        return true;
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;
        final DoubleValues values = valuesSource.doubleValues();
        final int valuesCount = values.setDocument(doc);

        for (int i = 0; i < valuesCount; ++i) {
            final double val = values.nextValue();
            final long bits = Double.doubleToRawLongBits(val);
            long bucketOrdinal = bucketOrds.add(bits);
            if (bucketOrdinal < 0) { // already seen
                bucketOrdinal = - 1 - bucketOrdinal;
            } else if (bucketOrdinal >= counts.size()) { // new bucket, maybe grow
                counts = BigArrays.grow(counts, bucketOrdinal + 1);
            }
            counts.increment(bucketOrdinal, 1);
            for (Aggregator subAggregator : subAggregators) {
                subAggregator.collect(doc, bucketOrdinal);
            }
        }
    }

    // private impl that stores a bucket ord. This allows for computing the aggregations lazily.
    static class OrdinalBucket extends DoubleTerms.Bucket {

        long bucketOrd;

        public OrdinalBucket() {
            super(0, 0, (InternalAggregations) null);
        }

    }

    @Override
    public DoubleTerms buildAggregation(long owningBucketOrdinal) {
        final int size = (int) Math.min(bucketOrds.size(), requiredSize);

        BucketPriorityQueue ordered = new BucketPriorityQueue(size, order.comparator());
        OrdinalBucket spare = null;
        for (long i = 0; i < bucketOrds.capacity(); ++i) {
            final long id = bucketOrds.id(i);
            if (id < 0) {
                // slot is not allocated
                continue;
            }

            if (spare == null) {
                spare = new OrdinalBucket();
            }
            spare.term = Double.longBitsToDouble(bucketOrds.key(i));
            spare.docCount = counts.get(id);
            spare.bucketOrd = id;
            spare = (OrdinalBucket) ordered.insertWithOverflow(spare);
        }

        final InternalTerms.Bucket[] list = new InternalTerms.Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; --i) {
            final OrdinalBucket bucket = (OrdinalBucket) ordered.pop();
            bucket.aggregations = buildSubAggregations(bucket.bucketOrd);
            list[i] = bucket;
        }
        return new DoubleTerms(name, order, valuesSource.formatter(), requiredSize, Arrays.asList(list));
    }

}
