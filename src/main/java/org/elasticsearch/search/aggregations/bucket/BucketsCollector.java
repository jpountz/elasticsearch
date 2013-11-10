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

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.search.aggregations.Aggregator;

import java.io.IOException;

public abstract class BucketsCollector {

    protected final Aggregator[] aggregators;
    protected LongArray docCounts;

    public BucketsCollector(Aggregator[] aggregators, long expectedBucketsCount) {
        this.aggregators = aggregators;
        this.docCounts = BigArrays.newLongArray(expectedBucketsCount);
    }

    public boolean collect(int doc, long bucketOrd) throws IOException {
        if (onDoc(doc, bucketOrd)) {
            docCounts = BigArrays.grow(docCounts, bucketOrd + 1);
            docCounts.increment(bucketOrd, 1);
            for (int i = 0; i < aggregators.length; i++) {
                aggregators[i].collect(doc, bucketOrd);
            }
            return true;
        }
        return false;
    }

    public long bucketsCount() {
        return docCounts.size();
    }

    /**
     * @return The number of documents in the bucket.
     */
    public long docCount(long bucketOrd) {
        if (bucketOrd >= docCounts.size()) {
            return 0;
        }
        return docCounts.get(bucketOrd);
    }

    protected abstract boolean onDoc(int doc, long bucketOrd) throws IOException;

}
