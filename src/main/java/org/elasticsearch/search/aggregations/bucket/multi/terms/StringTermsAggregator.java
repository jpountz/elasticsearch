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

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.google.common.collect.ImmutableList;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.collect.ReusableGrowableArray;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.bucket.BytesBucketsAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValueSpace;
import org.elasticsearch.search.aggregations.context.ValuesSource;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.search.aggregations.bucket.BucketsAggregator.buildAggregations;

/**
 *
 */
public class StringTermsAggregator extends BytesBucketsAggregator {

    private final List<Aggregator.Factory> factories;
    private final InternalOrder order;
    private final int requiredSize;

    Recycler.V<ObjectObjectOpenHashMap<HashedBytesRef, BucketCollector>> buckets;

    public StringTermsAggregator(String name, List<Aggregator.Factory> factories, ValuesSource valuesSource,
                                 InternalOrder order, int requiredSize, AggregationContext aggregationContext, Aggregator parent) {

        super(name, valuesSource, aggregationContext, parent);
        this.factories = factories;
        this.order = order;
        this.requiredSize = requiredSize;
        buckets = aggregationContext.cacheRecycler().hashMap(-1);
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public StringTerms buildAggregation() {

        if (buckets.v().isEmpty()) {
            return new StringTerms(name, order, requiredSize, ImmutableList.<InternalTerms.Bucket>of());
        }

        if (requiredSize < EntryPriorityQueue.LIMIT) {
            BucketPriorityQueue ordered = new BucketPriorityQueue(requiredSize, order.comparator());
            boolean[] allocated = buckets.v().allocated;
            Object[] collectors = buckets.v().values;
            for (int i = 0; i < allocated.length; i++) {
                if (allocated[i]) {
                    ordered.insertWithOverflow(((BucketCollector) collectors[i]).buildBucket());
                }
            }
            buckets.release();
            InternalTerms.Bucket[] list = new InternalTerms.Bucket[ordered.size()];
            for (int i = ordered.size() - 1; i >= 0; i--) {
                list[i] = (StringTerms.Bucket) ordered.pop();
            }
            return new StringTerms(name, order, requiredSize, Arrays.asList(list));
        } else {
            BoundedTreeSet<InternalTerms.Bucket> ordered = new BoundedTreeSet<InternalTerms.Bucket>(order.comparator(), requiredSize);
            boolean[] allocated = buckets.v().allocated;
            Object[] collectors = buckets.v().values;
            for (int i = 0; i < allocated.length; i++) {
                if (allocated[i]) {
                    ordered.add(((BucketCollector) collectors[i]).buildBucket());
                }
            }
            buckets.release();
            return new StringTerms(name, order, requiredSize, ordered);
        }
    }

    class Collector implements Aggregator.Collector {

        private ReusableGrowableArray<BucketCollector> matchedBuckets;
        private HashedBytesRef scratch = new HashedBytesRef(new BytesRef());

        @Override
        public void collect(int doc, ValueSpace valueSpace) throws IOException {
            BytesValues values = valuesSource.bytesValues();

            if (!values.hasValue(doc)) {
                return;
            }

            Object valuesSourceKey = valuesSource.key();
            if (!values.isMultiValued()) {
                scratch.hash = values.getValueHashed(doc, scratch.bytes);
                if (!valueSpace.accept(valuesSourceKey, scratch.bytes)) {
                    return;
                }
                BucketCollector bucket = buckets.v().get(scratch);
                if (bucket == null) {
                    HashedBytesRef put = new HashedBytesRef(values.makeSafe(scratch.bytes), scratch.hash);
                    bucket = new BucketCollector(valuesSource, put.bytes, factories, StringTermsAggregator.this);
                    buckets.v().put(put, bucket);
                }
                bucket.collect(doc, valueSpace);
                return;
            }

            if (matchedBuckets == null) {
                matchedBuckets = new ReusableGrowableArray<BucketCollector>(BucketCollector.class);
            }

            // we'll first find all the buckets that match the values, and then propagate the document through them
            // we need to do that to avoid counting the same document more than once.
            populateMatchingBuckets(doc, valuesSourceKey, values, valueSpace);
            BucketCollector[] mBuckets = matchedBuckets.innerValues();
            for (int i = 0; i < matchedBuckets.size(); i++) {
                mBuckets[i].collect(doc, valueSpace);
            }
        }

        private void populateMatchingBuckets(int doc, Object valuesSourceKey, BytesValues values, ValueSpace context) throws IOException {
            matchedBuckets.reset();
            for (BytesValues.Iter iter = values.getIter(doc); iter.hasNext();) {
                scratch.bytes = iter.next();
                scratch.hash = iter.hash();
                if (!context.accept(valuesSourceKey, scratch.bytes)) {
                    continue;
                }
                BucketCollector bucket = buckets.v().get(scratch);
                if (bucket == null) {
                    HashedBytesRef put = new HashedBytesRef(values.makeSafe(scratch.bytes), scratch.hash);
                    bucket = new BucketCollector(valuesSource, put.bytes, factories, StringTermsAggregator.this);
                    buckets.v().put(put, bucket);
                }
                matchedBuckets.add(bucket);
            }
        }


        @Override
        public void postCollection() {
            boolean[] states = buckets.v().allocated;
            Object[] collectors = buckets.v().values;
            for (int i = 0; i < states.length; i++) {
                if (states[i]) {
                    ((BucketCollector) collectors[i]).postCollection();
                }
            }
            StringTermsAggregator.this.buckets = buckets;
        }
    }

    static class BucketCollector extends BytesBucketsAggregator.BucketCollector {

        final BytesRef term;

        long docCount;

        BucketCollector(ValuesSource valuesSource, BytesRef term, List<Aggregator.Factory> factories, Aggregator aggregator) {
            super(valuesSource, factories, aggregator);
            this.term = term;
        }

        @Override
        protected boolean onDoc(int doc, BytesValues values, ValueSpace valueSpace) throws IOException {
            docCount++;
            return true;
        }

        @Override
        public boolean accept(BytesRef value) {
            // we can optimize here and instead of checking the value space, just return 'true'. The reason for this is that the bucket only
            // represents a single field value anyway, and currently checking the value space will only be required if we put a cout calc
            // agg under the bucket or another term agg on the same field (which makes no sense). The problem is, once we do return 'true'
            // we "break" the contract of the aggs as putting "count" agg under the bucket (as insensible it may be) will return wrong counts.
            // so for now we keep the contract and still check the value space.
            return value.equals(term);
        }

        StringTerms.Bucket buildBucket() {
            return new StringTerms.Bucket(term, docCount, buildAggregations(subAggregators));
        }
    }

}
