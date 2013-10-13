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

package org.elasticsearch.search.aggregations.context;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;

/**
 * The get context is passed down the collector hierarchy during the get process. The context may
 * determine whether a specific value for a specific field should be counted for get. This only applies for
 * calc aggregators - as bucketing aggregators get (bucket) based on doc ids, while calc aggregators aggregate
 * based on field values.
 */
public interface ValueSpace {

    Default DEFAULT = new Default();

    /**
     * Determines whether the given double value for the given field should be aggregated.
     *
     * @param valueSourceKey    The key of the value source
     * @param value             The value
     * @return {@code true} if the value should be aggregated, {@code false} otherwise.
     */
    boolean accept(Object valueSourceKey, double value);

    /**
     * Determines whether the given long value for the given field should be aggregated.
     *
     * @param valueSourceKey    The key of the value source
     * @param value             The value
     * @return {@code true} if the value should be aggregated, {@code false} otherwise.
     */
    boolean accept(Object valueSourceKey, long value);

    /**
     * Determines whether the given bytesref value for the given field should be aggregated.
     *
     * @param valueSourceKey    The key of the value source
     * @param value             The value
     * @return {@code true} if the value should be aggregated, {@code false} otherwise.
     */
    boolean accept(Object valueSourceKey, BytesRef value);

    /**
     * Determines whether the given geo point value for the given field should be aggregated.
     *
     * @param valueSourceKey    The key of the value source
     * @param value             The value
     * @return {@code true} if the value should be aggregated, {@code false} otherwise.
     */
    boolean accept(Object valueSourceKey, GeoPoint value);

    /**
     * The default get context (a fast one) which determines that all values for all fields should be aggregated.
     */
    class Default implements ValueSpace {

        private Default() {
        }

        @Override
        public boolean accept(Object valuesSourceKey, double value) {
            return true;
        }

        @Override
        public boolean accept(Object valuesSourceKey, long value) {
            return true;
        }

        @Override
        public boolean accept(Object valuesSourceKey, BytesRef value) {
            return true;
        }

        @Override
        public boolean accept(Object valuesSourceKey, GeoPoint value) {
            return true;
        }
    }
}
