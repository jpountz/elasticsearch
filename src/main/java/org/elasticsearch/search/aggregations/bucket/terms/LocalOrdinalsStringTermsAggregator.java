/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.bucket.terms;


import com.google.common.collect.Lists;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.mahout.math.list.LongArrayList;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.bucket.terms.support.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.bytes.BytesValuesSource;

import java.io.IOException;
import java.util.Collections;
import java.util.List;


/**
 * An aggregator of string values that uses ordinals. Each segment is collected separately and results are merged in the final
 * {@link #buildAggregation(long)} phase.
 */
public class LocalOrdinalsStringTermsAggregator extends AbstractStringTermsAggregator {

    private final BytesValuesSource.WithOrdinals valuesSource;
    private Ordinals.Docs ordinals;
    private final List<AtomicCounts> termCounts;
    private LongArray currentTermCounts;
    private long ordBase;

    public LocalOrdinalsStringTermsAggregator(String name, AggregatorFactories factories, BytesValuesSource.WithOrdinals valuesSource,
            long estimatedBucketCount, InternalOrder order, int requiredSize, int shardSize, long minDocCount, AggregationContext aggregationContext, Aggregator parent) {
        super(name, factories, estimatedBucketCount, aggregationContext, parent, order, requiredSize, shardSize, minDocCount);
        this.valuesSource = valuesSource;
        termCounts = Lists.newArrayList();
        ordBase = 0;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        final BytesValues.WithOrdinals values = valuesSource.bytesValues();
        ordinals = values.ordinals();
        ordBase += currentTermCounts == null ? 0 : currentTermCounts.size();
        currentTermCounts = context().bigArrays().newLongArray(ordinals.getMaxOrd());
        termCounts.add(new AtomicCounts(values, currentTermCounts, ordBase));
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;
        final int valuesCount = ordinals.setDocument(doc);

        for (int i = 0; i < valuesCount; ++i) {
            final long termOrd = ordinals.nextOrd();
            currentTermCounts.increment(termOrd, 1);
            collectBucket(doc, ordBase + termOrd);
        }
    }

    private static void copy(BytesRef from, BytesRef to) {
        if (to.bytes.length < from.length) {
            to.bytes = new byte[ArrayUtil.oversize(from.length, RamUsageEstimator.NUM_BYTES_BYTE)];
        }
        to.offset = 0;
        to.length = from.length;
        System.arraycopy(from.bytes, from.offset, to.bytes, 0, from.length);
    }

    private static class Bucket extends StringTerms.Bucket {

        final LongArrayList bucketOrds;

        public Bucket(BytesRef term, long docCount, InternalAggregations aggregations) {
            super(term, docCount, aggregations);
            bucketOrds = new LongArrayList();
        }

    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        final PriorityQueue<AtomicCountsIterator> pq = new PriorityQueue<AtomicCountsIterator>(termCounts.size()) {
            @Override
            protected boolean lessThan(AtomicCountsIterator a, AtomicCountsIterator b) {
                return a.term.compareTo(b.term) < 0;
            }
        };
        long maxSize = 0;
        for (AtomicCounts counts : termCounts) {
            AtomicCountsIterator iterator = counts.iterator();
            if (iterator.next()) {
                maxSize += iterator.values.ordinals().getNumOrds();
                pq.add(iterator);
            }
        }

        final int size = (int) Math.min(maxSize, shardSize);
        final BucketPriorityQueue ordered = new BucketPriorityQueue(size, order.comparator(this));
        Bucket spare = null;
        for (AtomicCountsIterator top = pq.top(); top != null; ) {
            if (spare == null) {
                spare = new Bucket(new BytesRef(), 0, null);
            }
            copy(top.term, spare.termBytes);
            spare.docCount = 0;
            spare.bucketOrds.clear();
            spare.bucketOrd = top.ord;
            do {
                spare.docCount += top.count;
                spare.bucketOrds.add(top.bucketOrd());
                if (!top.next()) {
                    final AtomicCountsIterator i = pq.pop();
                    assert i == top;
                    top = pq.top();
                    if (top == null) {
                        break;
                    }
                } else {
                    top = pq.updateTop();
                }
            } while (top.term.equals(spare.termBytes));

            if (spare.docCount >= minDocCount) {
                spare = (Bucket) ordered.insertWithOverflow(spare);
            }
        }

        List<InternalAggregation> aggregations = Lists.newArrayList();
        for (int i = ordered.size() - 1; i >= 0; --i) {
            final Bucket metaBucket = (Bucket) ordered.pop();
            if (metaBucket.bucketOrds.isEmpty()) {
                final StringTerms.Bucket bucket = new StringTerms.Bucket(metaBucket.termBytes, 0, bucketEmptyAggregations());
                aggregations.add(new StringTerms(name, order, requiredSize, minDocCount, Collections.<InternalTerms.Bucket>singletonList(bucket)));
            } else {
                for (int j = 0; j < metaBucket.bucketOrds.size(); ++j) {
                    final StringTerms.Bucket bucket = new StringTerms.Bucket(metaBucket.termBytes, metaBucket.docCount, bucketAggregations(metaBucket.bucketOrds.get(j)));
                    aggregations.add(new StringTerms(name, order, requiredSize, minDocCount, Collections.<InternalTerms.Bucket>singletonList(bucket)));
                }
            }
        }
        if (aggregations.isEmpty()) {
            return buildEmptyAggregation();
        } else {
            return aggregations.get(0).reduce(new ReduceContext(aggregations, context.bigArrays()));
        }
    }

    @Override
    public void doRelease() {
        Releasables.release(termCounts);
    }

    private static class AtomicCounts implements Releasable {

        private final BytesValues.WithOrdinals values;
        private final LongArray counts;
        private final long ordBase;

        AtomicCounts(BytesValues.WithOrdinals values, LongArray counts, long ordBase) {
            this.values = values;
            this.counts = counts;
            this.ordBase = ordBase;
        }

        AtomicCountsIterator iterator() {
            return new AtomicCountsIterator(values, counts, ordBase);
        }

        @Override
        public boolean release() throws ElasticsearchException {
            counts.release();
            return true;
        }
    }

    private static class AtomicCountsIterator {

        private final BytesValues.WithOrdinals values;
        private final LongArray counts;
        private final long ordBase;
        private final long maxOrd;
        private long ord;
        private BytesRef term;
        private long count;

        AtomicCountsIterator(BytesValues.WithOrdinals values, LongArray counts, long ordBase) {
            this.values = values;
            this.counts = counts;
            this.ordBase = ordBase;
            maxOrd = values.ordinals().getMaxOrd();
            ord = Ordinals.MISSING_ORDINAL;
            term = null;
        }

        boolean next() {
            if (++ord >= maxOrd) {
                term = null;
                count = 0;
                return false;
            } else {
                term = values.getValueByOrd(ord);
                count = counts.get(ord);
                return true;
            }
        }

        long bucketOrd() {
            return ordBase + ord;
        }
    }

}
