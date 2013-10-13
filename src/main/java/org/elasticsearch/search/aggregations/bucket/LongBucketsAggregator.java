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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValueSpace;
import org.elasticsearch.search.aggregations.context.numeric.NumericValuesSource;

import java.io.IOException;
import java.util.List;

/**
 * A value source based aggregator which can aggregate buckets based on {@code long} values.
 */
public abstract class LongBucketsAggregator extends ValuesSourceBucketsAggregator<NumericValuesSource> {

    public LongBucketsAggregator(String name,
                                 NumericValuesSource valuesSource,
                                 AggregationContext aggregationContext,
                                 Aggregator parent) {

        super(name, valuesSource, aggregationContext, parent);
    }

    /**
     * A runtime representation of a bucket. This bucket also serves as value space, which is effectively a criteria that decides whether
     * a long value matches this bucket or not. When the aggregator encounters a document, the long value/s will be extracted from the
     * the document (based on the configured {@link NumericValuesSource}) and will be checked against this criteria. If one of the checked
     * values matches, the document will be considered as "falling in" this bucket and it will be aggregated. Aggregating the document
     * in this bucket means:
     * <ol>
     *     <li>the document will be counted as part of the {@code doc_count} of this bucket</li>
     *     <li>
     *         the document will be propagated to all the sub-aggregators that are associated with this bucket. In this case, this
     *         bucket will also serve as the {@link ValueSpace} for all all the sub-aggregators, as they can only aggregate values that
     *         match the criteria of this bucket.
     *     </li>
     * </ol>
     */
    public static abstract class BucketCollector extends ValuesSourceBucketsAggregator.BucketCollector<NumericValuesSource> implements ValueSpace {

        private ValueSpace parentContext;

        protected BucketCollector(NumericValuesSource valuesSource, Aggregator[] subAggregators, Aggregator aggregator) {
            super(valuesSource, subAggregators, aggregator);
        }

        protected BucketCollector(NumericValuesSource valuesSource, List<Aggregator.Factory> factories, Aggregator parent) {
            super(valuesSource, factories, parent);
        }

        @Override
        protected final ValueSpace onDoc(int doc, ValueSpace valueSpace) throws IOException {
            LongValues values = valuesSource.longValues();
            if (!onDoc(doc, values, valueSpace)) {
                return null;
            }
            if (values.isMultiValued()) {
                parentContext = valueSpace;
                return this;
            }
            return valueSpace;
        }

        /**
         * Called for every doc that the aggregator encounters. If the doc falls in this bucket, it is aggregated and this method returns
         * {@code true}, otherwise it won't be aggregated in this bucket and this method will return {@code false}.
         *
         * @param doc           The doc id.
         * @param values        The values in the current segment.
         * @param valueSpace    The value space of the aggregator.
         *
         * @return              {@code true} iff the give doc falls in this bucket, {@code false} otherwise.
         * @throws IOException
         */
        protected abstract boolean onDoc(int doc, LongValues values, ValueSpace valueSpace) throws IOException;

        @Override
        public boolean accept(Object valueSourceKey, double value) {
            if (!parentContext.accept(valueSourceKey, value)) {
                return false;
            }
            if (valuesSource.key().equals(valueSourceKey)) {
                return accept((long) value);
            }
            return true;
        }

        @Override
        public boolean accept(Object valueSourceKey, long value) {
            if (!parentContext.accept(valueSourceKey, value)) {
                return false;
            }
            if (valuesSource.key().equals(valueSourceKey)) {
                return accept(value);
            }
            return true;
        }

        @Override
        public boolean accept(Object valueSourceKey, GeoPoint value) {
            return parentContext.accept(valueSourceKey, value);
        }

        @Override
        public boolean accept(Object valueSourceKey, BytesRef value) {
            return parentContext.accept(valueSourceKey, value);
        }

        /**
         * Indicates whether this bucket can accept the given value. Typically, each bucket defines a criteria which decides what values
         * fit it and what don't (based on this criteria, the {@link #onDoc(int, LongValues, ValueSpace)} decides whether a document falls
         * in this bucket or not).
         *
         * @param value The checked value.
         * @return      {@code true} if this value matches the criteria associated with this bucket, {@code false} otherwise.
         */
        public abstract boolean accept(long value);
    }

}
