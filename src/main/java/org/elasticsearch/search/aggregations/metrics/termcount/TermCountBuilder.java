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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.metrics.ValuesSourceMetricsAggregationBuilder;

import java.io.IOException;

public class TermCountBuilder extends ValuesSourceMetricsAggregationBuilder<TermCountBuilder> {

    private Integer precision;
    private Boolean rehash;

    public TermCountBuilder(String name) {
        super(name, InternalTermCount.TYPE.name());
    }

    public TermCountBuilder precision(int precision) {
        this.precision = precision;
        return this;
    }

    public TermCountBuilder rehash(boolean rehash) {
        this.rehash = rehash;
        return this;
    }

    @Override
    protected void internalXContent(XContentBuilder builder, Params params) throws IOException {
        super.internalXContent(builder, params);
        if (precision != null) {
            builder.field("precision", precision);
        }
        if (rehash != null) {
            builder.field("rehash", rehash);
        }
    }

}
