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

import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.geopoints.GeoPointValuesSource;

import java.io.IOException;
import java.util.List;

/**
 * A value source based aggregator which can aggregate buckets based on {@code geo_point} values.
 */
public abstract class GeoPointBucketsAggregator extends ValuesSourceBucketsAggregator<GeoPointValuesSource> {

    protected GeoPointBucketsAggregator(String name,
                                        GeoPointValuesSource valuesSource,
                                        AggregationContext aggregationContext,
                                        Aggregator parent) {

        super(name, valuesSource, aggregationContext, parent);
    }

    /**
     * A runtime representation of a bucket. This bucket also serves as value space, which is effectively a criteria that decides whether
     * a geo_point value matches this bucket or not. When the aggregator encounters a document, the geo_point value/s will be extracted from the
     * the document (based on the configured {@link GeoPointValuesSource}) and will be checked against this criteria. If one of the checked
     * values matches, the document will be considered as "falling in" this bucket and it will be aggregated. Aggregating the document
     * in this bucket means:
     * <ol>
     *     <li>the document will be counted as part of the {@code doc_count} of this bucket</li>
     *     <li>the document will be propagated to all the sub-aggregators that are associated with this bucket</li>
     * </ol>
     */
    public static abstract class BucketCollector extends ValuesSourceBucketsAggregator.BucketCollector<GeoPointValuesSource> {

        protected BucketCollector(GeoPointValuesSource valuesSource, Aggregator[] subAggregators, Aggregator aggregator) {
            super(valuesSource, subAggregators, aggregator);
        }

        protected BucketCollector(GeoPointValuesSource valuesSource, List<Aggregator.Factory> factories, Aggregator parent) {
            super(valuesSource, factories, parent);
        }


        @Override
        protected final boolean onDoc(int doc) throws IOException {
            return onDoc(doc, valuesSource.values());
        }

        /**
         * Called for every doc that the aggregator encounters. If the doc falls in this bucket, it is aggregated and this method returns
         * {@code true}, otherwise it won't be aggregated in this bucket and this method will return {@code false}.
         *
         * @param doc           The doc id.
         * @param values        The values in the current segment.
         *
         * @return              {@code true} iff the give doc falls in this bucket, {@code false} otherwise.
         * @throws IOException
         */
        protected abstract boolean onDoc(int doc, GeoPointValues values) throws IOException;

    }

}
