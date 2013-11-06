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

package org.elasticsearch.search.aggregations;

import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.factory.AggregatorFactories;

import java.io.IOException;

public abstract class Aggregator {

    /**
     * Defines the nature of the aggregator's aggregation execution when nested in other aggregators and the buckets they create.
     */
    public static enum BucketAggregationMode {

        /**
         * In this mode, a new aggregator instance will be created per bucket (created by the parent aggregator)
         */
        PER_BUCKET,

        /**
         * In this mode, a single aggregator instance will be created per parent aggregator, that will handle the aggregations of all its buckets.
         */
        MULTI_BUCKETS
    }

    protected final String name;
    protected final Aggregator parent;
    protected final AggregationContext context;
    protected final int depth;

    protected final BucketAggregationMode bucketAggregationMode;
    protected final AggregatorFactories factories;
    protected final Aggregator[] multiBucketAggregators;

    /**
     * Constructs a new Aggregator.
     *
     * @param name                  The name of the aggregation
     * @param bucketAggregationMode The nature of execution as a sub-aggregator (see {@link BucketAggregationMode})
     * @param factories             The factories for all the sub-aggregators under this aggregator
     * @param estimatedBucketsCount When served as a sub-aggregator, indicate how many buckets the parent aggregator will generate.
     * @param context               The aggregation context
     * @param parent                The parent aggregator (may be {@code null} for top level aggregators)
     */
    protected Aggregator(String name, BucketAggregationMode bucketAggregationMode, AggregatorFactories factories, int estimatedBucketsCount, AggregationContext context, Aggregator parent) {
        this.name = name;
        this.parent = parent;
        this.context = context;
        this.depth = parent == null ? 0 : 1 + parent.depth();
        this.bucketAggregationMode = bucketAggregationMode;
        this.factories = factories;
        assert factories != null : "sub-factories provided to BucketAggregator must not be null, use AggragatorFactories.EMPTY instead";
        this.multiBucketAggregators = factories.createMultiBucketAggregators(this, estimatedBucketsCount);
    }

    /**
     * @return  The name of the aggregation.
     */
    public String name() {
        return name;
    }

    /** Return the depth of this aggregator in the aggregation tree. */
    public final int depth() {
        return depth;
    }

    /**
     * @return  The parent aggregator of this aggregator. The addAggregation are hierarchical in the sense that some can
     *          be composed out of others (more specifically, bucket addAggregation can define other addAggregation that will
     *          be aggregated per bucket). This method returns the direct parent aggregator that contains this aggregator, or
     *          {@code null} if there is none (meaning, this aggregator is a top level one)
     */
    public Aggregator parent() {
        return parent;
    }

    /**
     * @return  The current aggregation context.
     */
    public AggregationContext context() {
        return context;
    }

    /**
     * @return  The bucket aggregation mode of this aggregator. This mode defines the nature in which the aggregation is executed
     * @see     BucketAggregationMode
     */
    public BucketAggregationMode bucketAggregationMode() {
        return bucketAggregationMode;
    }

    /**
     * @return  Whether this aggregator is in the state where it can collect documents. Some aggregators can do their aggregations without
     *          actually collecting documents, for example, an aggregator that computes stats over unmapped fields doesn't need to collect
     *          anything as it knows to just return "empty" stats as the aggregation result.
     */
    public abstract boolean shouldCollect();

    /**
     * Called during the query phase, to collect & aggregate the given document.
     *
     * @param doc                   The document to be collected/aggregated
     * @param owningBucketOrdinal   The ordinal of the bucket this aggregator belongs to, assuming this aggregator is not a top level aggregator.
     *                              Typically, aggregators with {@code #bucketAggregationMode} set to {@link BucketAggregationMode#MULTI_BUCKETS}
     *                              will heavily depend on this ordinal. Other aggregators may or may not use it and can see this ordinal as just
     *                              an extra information for the aggregation context. For top level aggregators, the ordinal will always be
     *                              equal to 0.
     * @throws IOException
     */
    public abstract void collect(int doc, int owningBucketOrdinal) throws IOException;

    /**
     * Called after collection of all document is done.
     */
    public void postCollection() {
        // we "post collect" the multi bucket aggregators once here... then we delegate to doPostCollect to enable the different
        // aggregators to "post collect" all the buckets.
        for (int i = 0; i < multiBucketAggregators.length; i++) {
            multiBucketAggregators[i].postCollection();
        }
        doPostCollection();
    }

    /**
     * Should be implemented by aggregators to "finalize" the collection process. "Bucketing" aggregators will typically use this callback
     * to call {@link #postCollection()} on the "per_bucket" aggregators in each of their buckets.
     */
    protected abstract void doPostCollection();

    /**
     * @return  The aggregated & built get.
     */
    public abstract InternalAggregation buildAggregation(int owningBucketOrdinal);


}
