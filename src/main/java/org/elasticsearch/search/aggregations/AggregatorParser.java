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

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * Parses the aggregation request and creates the appropriate aggregator factory for it.
 *
 * @see {@link Aggregator.Factory}
*/
public interface AggregatorParser {

    /**
     * @return The aggregation type this parser is associated with.
     */
    String type();

    /**
     * Returns the aggregator factory with which this parser is associated, may return {@code null} indicating the
     * aggregation should be skipped (e.g. when trying to aggregate on unmapped fields).
     *
     * @param aggregationName   The name of the aggregation
     * @param parser            The xcontent parser
     * @param context           The search context
     * @return                  The resolved aggregator factory or {@code null} in case the aggregation should be skipped
     * @throws IOException      When parsing fails
     */
    Aggregator.Factory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException;

}
