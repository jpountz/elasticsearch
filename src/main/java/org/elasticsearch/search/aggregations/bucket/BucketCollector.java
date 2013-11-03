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
import org.elasticsearch.search.aggregations.OrdsAggregator;

import java.io.IOException;

/**
 * A base class for a bucket collector. The bucket collector will be called for every document in the current context
 * and its job is:
 *
 * <ol>
 *     <li>to determine whether the doc belongs to the bucket</li>
 *     <li>perform any aggregation that is associated with this bucket</li>
 * </ol>
 */
public abstract class BucketCollector implements Aggregator.Collector {

    // the ordinal of the bucket
    protected int ord;

    protected final Aggregator[] aggregators;
    protected final Aggregator.Collector[] collectors;
    protected final OrdsAggregator.Collector[] ordsCollectors;

    protected long docCount;

    public BucketCollector(Aggregator[] aggregators, OrdsAggregator.Collector[] ordsCollectors) {
        this(0, aggregators, ordsCollectors);
    }

    public BucketCollector(int ord, Aggregator[] aggregators, OrdsAggregator.Collector[] ordsCollectors) {
        this.ord = ord;
        this.ordsCollectors = ordsCollectors;
        this.aggregators = aggregators;
        this.collectors = new Aggregator.Collector[aggregators.length];
        for (int i = 0; i < aggregators.length; i++) {
            collectors[i] = aggregators[i].collector();
        }
    }

    @Override
    public void collect(int doc) throws IOException {
        if (onDoc(doc)) {
            docCount++;
            for (int i = 0; i < ordsCollectors.length; i++) {
                ordsCollectors[i].collect(doc, ord);
            }
            for (int i = 0; i < collectors.length; i++) {
                if (collectors[i] != null) {
                    collectors[i].collect(doc);
                }
            }
        }
    }

    @Override
    public final void postCollection() {
        for (int i = 0; i < collectors.length; i++) {
            if (collectors[i] != null) {
                collectors[i].postCollection();
            }
        }
        for (int i = 0; i < ordsCollectors.length; i++) {
            ordsCollectors[i].postCollection();
        }
        postCollection(aggregators, docCount);
    }

    public int ord() {
        return ord;
    }

    public long docCount() {
        return docCount;
    }

    public Aggregator[] aggregators() {
        return aggregators;
    }


    /**
     * Called to aggregate the data in the given doc and returns whether the value space that should be used for all sub-aggregators
     * of this bucket.
     *
     * @param doc   The doc to aggregate
     * @return      {@code true} if the give doc falls in the bucket, {@code false} otherwise.
     * @throws java.io.IOException
     */
    protected abstract boolean onDoc(int doc) throws IOException;

    /**
     * Called when collection is finished
     *
     * @param aggregators   The sub aggregators of this bucket
     */
    protected abstract void postCollection(Aggregator[] aggregators, long docCount);

}
