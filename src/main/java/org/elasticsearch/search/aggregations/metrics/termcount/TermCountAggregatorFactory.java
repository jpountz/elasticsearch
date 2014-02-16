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

package org.elasticsearch.search.aggregations.metrics.termcount;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.Aggregator.BucketAggregationMode;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

final class TermCountAggregatorFactory extends ValueSourceAggregatorFactory<ValuesSource> {

    private final int precision;
    private final boolean rehash;

    TermCountAggregatorFactory(String name, ValuesSourceConfig valuesSourceConfig, int precision, boolean rehash) {
        super(name, InternalTermCount.TYPE.name(), valuesSourceConfig);
        this.precision = precision;
        this.rehash = rehash;
    }

    /**
     * If one of the parent aggregators is a MULTI_BUCKET one, we might want to lower the precision
     * because otherwise it might be memory-intensive. On the other hand, for top-level aggregators
     * we try to focus on accuracy.
     */
    private int defaultPrecision(Aggregator parent) {
        boolean hasParentMultiBucketAggregator = false;
        while (parent != null) {
            if (parent.bucketAggregationMode() == BucketAggregationMode.MULTI_BUCKETS) {
                hasParentMultiBucketAggregator = true;
                break;
            }
            parent = parent.parent();
        }

        return hasParentMultiBucketAggregator ? 9 : 14;
    }

    private int precision(Aggregator parent) {
        return precision < 0 ? defaultPrecision(parent) : precision;
    }

    @Override
    protected Aggregator createUnmapped(AggregationContext context, Aggregator parent) {
        return new TermCountAggregator(name, parent == null ? 1 : parent.estimatedBucketCount(), null, true, precision(parent), context, parent);
    }

    @Override
    protected Aggregator create(ValuesSource valuesSource, long expectedBucketsCount, AggregationContext context,
            Aggregator parent) {
        return new TermCountAggregator(name, parent == null ? 1 : parent.estimatedBucketCount(), valuesSource, rehash, precision(parent), context, parent);
    }

}
