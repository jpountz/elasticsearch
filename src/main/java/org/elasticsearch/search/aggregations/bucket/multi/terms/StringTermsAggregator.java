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
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValuesSource;
import org.elasticsearch.search.aggregations.factory.AggregatorFactories;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;

import java.io.IOException;
import java.util.Arrays;

/**
 * nocommit we need to change this aggregator to be based on ordinals (see {@link org.elasticsearch.search.facet.terms.strings.TermsStringOrdinalsFacetExecutor})
 */
public class StringTermsAggregator extends Aggregator {

    private final ValuesSource valuesSource;
    private final InternalOrder order;
    private final int requiredSize;
    private final Collector collector;
    private final Recycler.V<ObjectObjectOpenHashMap<HashedBytesRef, BucketCollector>> bucketCollectors;

    public StringTermsAggregator(String name, AggregatorFactories factories, ValuesSource valuesSource,
                                 InternalOrder order, int requiredSize, AggregationContext aggregationContext, Aggregator parent) {

        super(name, BucketAggregationMode.PER_BUCKET, factories, 50, aggregationContext, parent);
        this.valuesSource = valuesSource;
        this.order = order;
        this.requiredSize = requiredSize;
        this.bucketCollectors = aggregationContext.cacheRecycler().hashMap(-1);
        this.collector = new Collector(bucketCollectors.v());
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

    @Override
    public StringTerms buildAggregation(int owningBucketOrdinal) {

        if (bucketCollectors.v().isEmpty()) {
            return new StringTerms(name, order, requiredSize, ImmutableList.<InternalTerms.Bucket>of());
        }

        if (requiredSize < EntryPriorityQueue.LIMIT) {
            BucketPriorityQueue ordered = new BucketPriorityQueue(requiredSize, order.comparator());
            boolean[] allocated = bucketCollectors.v().allocated;
            Object[] collectors = bucketCollectors.v().values;
            for (int i = 0; i < allocated.length; i++) {
                if (allocated[i]) {
                    BucketCollector collector = (BucketCollector) collectors[i];
                    StringTerms.Bucket bucket = new StringTerms.Bucket(collector.term, collector.docCount(), collector.buildAggregations());
                    ordered.insertWithOverflow(bucket);
                }
            }
            bucketCollectors.release();
            InternalTerms.Bucket[] list = new InternalTerms.Bucket[ordered.size()];
            for (int i = ordered.size() - 1; i >= 0; i--) {
                list[i] = (StringTerms.Bucket) ordered.pop();
            }
            return new StringTerms(name, order, requiredSize, Arrays.asList(list));
        } else {
            BoundedTreeSet<InternalTerms.Bucket> ordered = new BoundedTreeSet<InternalTerms.Bucket>(order.comparator(), requiredSize);
            boolean[] allocated = bucketCollectors.v().allocated;
            Object[] collectors = bucketCollectors.v().values;
            for (int i = 0; i < allocated.length; i++) {
                if (allocated[i]) {
                    BucketCollector collector = (BucketCollector) collectors[i];
                    StringTerms.Bucket bucket = new StringTerms.Bucket(collector.term, collector.docCount(), collector.buildAggregations());
                    ordered.add(bucket);
                }
            }
            bucketCollectors.release();
            return new StringTerms(name, order, requiredSize, ordered);
        }
    }

    class Collector {

        private final ObjectObjectOpenHashMap<HashedBytesRef, BucketCollector> bucketCollectors;

        Collector(ObjectObjectOpenHashMap<HashedBytesRef, BucketCollector> bucketCollectors) {
            this.bucketCollectors = bucketCollectors;
        }

        private HashedBytesRef scratch = new HashedBytesRef(new BytesRef());

        int ordCounter;

        public void collect(int doc) throws IOException {
            BytesValues values = valuesSource.bytesValues();
            int valuesCount = values.setDocument(doc);

            for (int i = 0; i < valuesCount; ++i) {
                scratch.bytes = values.nextValue();
                scratch.hash = values.currentValueHash();
                BucketCollector bucket = bucketCollectors.get(scratch);
                if (bucket == null) {
                    HashedBytesRef put = scratch.deepCopy();
                    bucket = new BucketCollector(ordCounter++, put.bytes, factories.createBucketAggregators(StringTermsAggregator.this, multiBucketAggregators, Math.max(50, bucketCollectors.size())));
                    bucketCollectors.put(put, bucket);
                }
                bucket.collect(doc);
            }
        }

        public void postCollection() {
            boolean[] states = bucketCollectors.allocated;
            Object[] collectors = bucketCollectors.values;
            for (int i = 0; i < states.length; i++) {
                if (states[i]) {
                    ((BucketCollector) collectors[i]).postCollection();
                }
            }
        }
    }

    static class BucketCollector extends org.elasticsearch.search.aggregations.bucket.BucketCollector {

        final BytesRef term;

        BucketCollector(int ord, BytesRef term, Aggregator[] aggregators) {
            super(ord, aggregators);
            this.term = term;
        }

        @Override
        protected boolean onDoc(int doc) throws IOException {
            return true;
        }
    }

}
