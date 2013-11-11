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

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.factory.AggregatorFactories;

/**
 * A bucket aggregator that doesn't create new buckets.
 */
public abstract class SingleBucketAggregator extends Aggregator {

    protected LongArray counts;

    protected SingleBucketAggregator(String name, AggregatorFactories factories,
                                     AggregationContext aggregationContext, Aggregator parent) {
        super(name, BucketAggregationMode.MULTI_BUCKETS, factories, parent == null ? 1 : parent.estimatedBucketCount(), aggregationContext, parent);
        counts = BigArrays.newLongArray(parent == null ? 1 : parent.estimatedBucketCount());
    }

    @Override
    public boolean shouldCollect() {
        return true;
    }

    protected final long docCount(long bucketOrdinal) {
        if (bucketOrdinal >= counts.size()) {
            return 0;
        } else {
            return counts.get(bucketOrdinal);
        }
    }

}
