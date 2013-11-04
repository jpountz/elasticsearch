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

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A base class for a bucket collector. The bucket collector will be called for every document in the current context
 * and its job is:
 *
 * <ol>
 *     <li>to determine whether the doc belongs to the bucket</li>
 *     <li>perform any aggregation that is associated with this bucket</li>
 * </ol>
 */
public abstract class BucketCollector {

    // the ordinal of the bucket
    protected int ord;

    protected final Aggregator[] aggregators;

    protected long docCount;

    /**
     * Creates a new bucket collector. By default, the ordinal of this bucket will be considered to be 0.
     *
     * @param aggregators   The aggregators of this bucket
     */
    public BucketCollector(Aggregator[] aggregators) {
        this(0, aggregators);
    }

    /**
     * Creates a new bucket.
     *
     * @param ord           The ordinal of this bucket (compared to other sibling buckets in the same aggregator).
     * @param aggregators   The aggregators of this bucket.
     */
    public BucketCollector(int ord, Aggregator[] aggregators) {
        this.ord = ord;
        this.aggregators = aggregators;
    }

    /**
     * Collects the given document. This method will first determine whether the given doc "falls in" this bucket. If so, it will
     * propagate the doc to all the aggregators in the bucket.
     *
     * @param doc   The doc to collect
     */
    public void collect(int doc) throws IOException {
        if (onDoc(doc)) {
            docCount++;
            for (int i = 0; i < aggregators.length; i++) {
                aggregators[i].collect(doc, ord);
            }
        }
    }

    /**
     * Called after all documents where collected.
     */
    public void postCollection() {
        // we only need to call postCollection on the per_bucket aggregators. The ordinals aggregators are
        // shared among all buckets and therefore they are "post collected" at the aggregator level (see Aggregator#postCollection())
        for (int i = 0; i < aggregators.length; i++) {
            if (aggregators[i].bucketAggregationMode() == Aggregator.BucketAggregationMode.PER_BUCKET && aggregators[i].shouldCollect()) {
                aggregators[i].postCollection();
            }
        }

    }

    /**
     * Builds and returns the aggregations of this bucket.
     */
    public InternalAggregations buildAggregations() {
        List<InternalAggregation> aggregations = new ArrayList<InternalAggregation>(aggregators.length);
        for (int i = 0; i < aggregators.length; i++) {
            aggregations.add(aggregators[i].buildAggregation(ord));
        }
        return new InternalAggregations(aggregations);
    }

    /**
     * @return The ordinal of this bucket
     */
    public int ord() {
        return ord;
    }

    /**
     * @return The number of documents in the bucket - should only be called after {@link #postCollection()} is called.
     */
    public long docCount() {
        return docCount;
    }

    /**
     * Called to aggregate the data in the given doc and returns whether the document falls in this bucket or not.
     *
     * @param doc   The doc to aggregate
     * @return      {@code true} if the give doc falls in the bucket, {@code false} otherwise.
     */
    protected abstract boolean onDoc(int doc) throws IOException;

}
