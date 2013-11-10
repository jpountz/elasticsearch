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

package org.elasticsearch.search.aggregations.bucket.single;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.BucketsCollector;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.factory.AggregatorFactories;

import java.io.IOException;

/**
 * A bucket aggregator that creates a single bucket
 */
public abstract class SingleBucketAggregator extends Aggregator {

    protected BucketsCollector collector;

    protected SingleBucketAggregator(String name, AggregatorFactories factories,
                                     AggregationContext aggregationContext, Aggregator parent) {
        super(name, BucketAggregationMode.PER_BUCKET, factories, 1, aggregationContext, parent);
        collector = new BucketsCollector(subAggregators, 1) {
            @Override
            protected boolean onDoc(int doc, int bucketOrd) throws IOException {
                return SingleBucketAggregator.this.onDoc(doc);
            }
        };
    }

    @Override
    public boolean shouldCollect() {
        return true;
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        collector.collect(doc, 0);
    }

    @Override
    public final InternalAggregation buildAggregation(long owningBucketOrdinal) {
        return buildAggregation(collector.buildAggregations(0), collector.docCount(0));
    }

    protected abstract boolean onDoc(int doc) throws IOException;

    /**
     * Convenient method to implement... given the aggregations of the single bucket and the number of documents that "fell in" it
     * during the collection time, this method builds the aggregation of this aggregator.
     */
    protected abstract InternalAggregation buildAggregation(InternalAggregations aggregations, long docCount);


}
