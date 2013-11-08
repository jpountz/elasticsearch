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

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class BucketsCollector {

    protected final Aggregator[] aggregators;
    protected long[] docCounts;

    public BucketsCollector(Aggregator[] aggregators, int expectedBucketsCount) {
        this.aggregators = aggregators;
        this.docCounts = new long[expectedBucketsCount];
    }

    public boolean collect(int doc, int bucketOrd) throws IOException {
        if (onDoc(doc, bucketOrd)) {
            if (bucketOrd >= docCounts.length) {
                docCounts = ArrayUtil.grow(docCounts, bucketOrd + 1);
            }
            docCounts[bucketOrd]++;
            for (int i = 0; i < aggregators.length; i++) {
                aggregators[i].collect(doc, bucketOrd);
            }
            return true;
        }
        return false;
    }

    public int bucketsCount() {
        return docCounts.length;
    }

    public InternalAggregations buildAggregations(int bucketOrd) {
        List<InternalAggregation> aggregations = new ArrayList<InternalAggregation>(aggregators.length);
        for (int i = 0; i < aggregators.length; i++) {
            aggregations.add(aggregators[i].buildAggregation(bucketOrd));
        }
        return new InternalAggregations(aggregations);
    }

    /**
     * @return The number of documents in the bucket.
     */
    public long docCount(int bucketOrd) {
        if (bucketOrd >= docCounts.length) {
            return 0;
        }
        return docCounts[bucketOrd];
    }

    protected abstract boolean onDoc(int doc, int bucketOrd) throws IOException;

}
