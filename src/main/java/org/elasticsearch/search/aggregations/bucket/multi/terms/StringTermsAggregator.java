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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValuesSource;
import org.elasticsearch.search.aggregations.factory.AggregatorFactories;

import java.io.IOException;
import java.util.Arrays;

/**
 * nocommit we need to change this aggregator to be based on ordinals (see {@link org.elasticsearch.search.facet.terms.strings.TermsStringOrdinalsFacetExecutor})
 */
public class StringTermsAggregator extends Aggregator {

    private static final int INITIAL_CAPACITY = 50; // TODO sizing

    private final ValuesSource valuesSource;
    private final InternalOrder order;
    private final int requiredSize;
    private final Collector collector;
    private final Aggregator[] subAggregators;

    public StringTermsAggregator(String name, AggregatorFactories factories, ValuesSource valuesSource,
                                 InternalOrder order, int requiredSize, AggregationContext aggregationContext, Aggregator parent) {

        super(name, BucketAggregationMode.PER_BUCKET, factories, 50, aggregationContext, parent);
        this.valuesSource = valuesSource;
        this.order = order;
        this.requiredSize = requiredSize;
        this.collector = new Collector();
        subAggregators = factories.createBucketAggregatorsAsMulti(this, INITIAL_CAPACITY);
    }

    @Override
    public boolean shouldCollect() {
        return true;
    }

    @Override
    public void collect(int doc, int owningBucketOrdinal) throws IOException {
        collector.collect(doc);
    }

    @Override
    protected void doPostCollection() {
        collector.postCollection();
    }

    // private impl that stores a bucket ord. This allows for computing the aggregations lazily.
    static class OrdinalBucket extends StringTerms.Bucket {

        int bucketOrd;

        public OrdinalBucket() {
            super(new BytesRef(), 0, null);
        }

    }

    @Override
    public StringTerms buildAggregation(int owningBucketOrdinal) {
        final BytesRefHash bytes = collector.bucketOrds;
        final LongArray counts = collector.counts;
        final int size = Math.min(bytes.size(), requiredSize);

        BucketPriorityQueue ordered = new BucketPriorityQueue(size, order.comparator());
        OrdinalBucket spare = null;
        for (int i = 0; i < bytes.size(); ++i) {
            if (spare == null) {
                spare = new OrdinalBucket();
            }
            bytes.get(i, spare.termBytes);
            spare.docCount = counts.get(i);
            spare.bucketOrd = i;
            spare = (OrdinalBucket) ordered.insertWithOverflow(spare);
        }

        final InternalTerms.Bucket[] list = new InternalTerms.Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; --i) {
            final OrdinalBucket bucket = (OrdinalBucket) ordered.pop();
            final InternalAggregation[] aggregations = new InternalAggregation[subAggregators.length];
            for (int j = 0; j < subAggregators.length; ++j) {
                aggregations[j] = subAggregators[j].buildAggregation(bucket.bucketOrd);
            }
            bucket.aggregations = new InternalAggregations(Arrays.asList(aggregations));
            list[i] = bucket;
        }
        return new StringTerms(name, order, requiredSize, Arrays.asList(list));
    }

    class Collector {

        private final BytesRefHash bucketOrds;
        private LongArray counts;

        Collector() {
            bucketOrds = new BytesRefHash();
            counts = BigArrays.newLongArray(INITIAL_CAPACITY);
        }

        public void collect(int doc) throws IOException {
            final BytesValues values = valuesSource.bytesValues();
            final int valuesCount = values.setDocument(doc);

            for (int i = 0; i < valuesCount; ++i) {
                final BytesRef bytes = values.nextValue();
                final int hash = values.currentValueHash();
                int bucketOrdinal = bucketOrds.add(bytes, hash);
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

        public void postCollection() {
            for (Aggregator subAggregator : subAggregators) {
                subAggregator.postCollection();
            }
        }
    }

}
