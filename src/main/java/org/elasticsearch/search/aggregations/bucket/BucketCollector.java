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
import org.elasticsearch.search.aggregations.OrdsAggregator;

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
    protected final OrdsAggregator[] ordsAggregators;

    protected long docCount;

    public BucketCollector(Aggregator[] aggregators, OrdsAggregator[] ordsAggregators) {
        this(0, aggregators, ordsAggregators);
    }

    public BucketCollector(int ord, Aggregator[] aggregators, OrdsAggregator[] ordsAggregators) {
        this.ord = ord;
        this.aggregators = aggregators;
        this.ordsAggregators = ordsAggregators;
    }

    public void collect(int doc) throws IOException {
        if (onDoc(doc)) {
            docCount++;
            for (int i = 0; i < ordsAggregators.length; i++) {
                if (ordsAggregators[i].shouldCollect()) {
                    ordsAggregators[i].collect(doc, ord);
                }
            }
            for (int i = 0; i < aggregators.length; i++) {
                if (aggregators[i].shouldCollect()) {
                    aggregators[i].collect(doc);
                }
            }
        }
    }

    public void postCollection() {
        for (int i = 0; i < aggregators.length; i++) {
            if (aggregators[i].shouldCollect()) {
                aggregators[i].postCollection();
            }
        }
        for (int i = 0; i < ordsAggregators.length; i++) {
            if (ordsAggregators[i].shouldCollect()) {
                ordsAggregators[i].postCollection();
            }
        }
    }

    public InternalAggregations buildAggregations() {
        List<InternalAggregation> aggregations = new ArrayList<InternalAggregation>(aggregators.length + ordsAggregators.length);
        for (int i = 0; i < aggregators.length; i++) {
            aggregations.add(aggregators[i].buildAggregation());
        }
        for (int i = 0; i < ordsAggregators.length; i++) {
            aggregations.add(ordsAggregators[i].buildAggregation(ord));
        }
        return new InternalAggregations(aggregations);
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
     * Called to aggregate the data in the given doc and returns whether the document falls in this bucket or not.
     *
     * @param doc   The doc to aggregate
     * @return      {@code true} if the give doc falls in the bucket, {@code false} otherwise.
     */
    protected abstract boolean onDoc(int doc) throws IOException;

}
