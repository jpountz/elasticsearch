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

package org.elasticsearch.search.aggregations.bucket.single;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.aggregations.bucket.single.missing.Missing;
import org.elasticsearch.search.aggregations.calc.numeric.avg.Avg;
import org.elasticsearch.test.AbstractIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.missing;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 *
 */
public class MissingTests extends AbstractIntegrationTest {

    @Override
    public Settings getSettings() {
        return randomSettingsBuilder()
                .put("index.number_of_shards", numberOfShards())
                .put("index.number_of_replicas", 0)
                .build();
    }

    protected int numberOfShards() {
        return 5;
    }

    @Before
    public void init() throws Exception {
        createIndex("idx");
        for (int i = 0; i < 5; i++) {
            client().prepareIndex("idx", "type", ""+i).setSource(jsonBuilder()
                    .startObject()
                    .field("tag", "tag1")
                    .endObject())
                    .execute().actionGet();
        }
        for (int i = 0; i < 5; i++) {
            client().prepareIndex("idx", "type", ""+i+6).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i)
                    .endObject())
                    .execute().actionGet();
        }

        createIndex("unmapped_idx");
        for (int i = 0; i < 3; i++) {
            client().prepareIndex("unmapped_idx", "type", ""+i).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i+5)
                    .endObject())
                    .execute().actionGet();
        }

        client().admin().indices().prepareFlush().execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();
    }

    @Test
    public void unmapped() throws Exception {
        SearchResponse response = client().prepareSearch("unmapped_idx")
                .addAggregation(missing("missing_tag").field("tag"))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Missing missing = response.getAggregations().get("missing_tag");
        assertThat(missing, notNullValue());
        assertThat(missing.getName(), equalTo("missing_tag"));
        assertThat(missing.getDocCount(), equalTo(3l));
    }

    @Test
    public void partiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx", "unmapped_idx")
                .addAggregation(missing("missing_tag").field("tag"))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Missing missing = response.getAggregations().get("missing_tag");
        assertThat(missing, notNullValue());
        assertThat(missing.getName(), equalTo("missing_tag"));
        assertThat(missing.getDocCount(), equalTo(8l));
    }

    @Test
    public void simple() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(missing("missing_tag").field("tag"))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Missing missing = response.getAggregations().get("missing_tag");
        assertThat(missing, notNullValue());
        assertThat(missing.getName(), equalTo("missing_tag"));
        assertThat(missing.getDocCount(), equalTo(5l));
    }

    @Test
    public void withSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx", "unmapped_idx")
                .addAggregation(missing("missing_tag").field("tag")
                        .subAggregation(avg("avg_value").field("value")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Missing missing = response.getAggregations().get("missing_tag");
        assertThat(missing, notNullValue());
        assertThat(missing.getName(), equalTo("missing_tag"));
        assertThat(missing.getDocCount(), equalTo(8l));
        assertThat(missing.getAggregations().asList().isEmpty(), is(false));

        Avg avgValue = missing.getAggregations().get("avg_value");
        assertThat(avgValue, notNullValue());
        assertThat(avgValue.getName(), equalTo("avg_value"));
        assertThat(avgValue.getValue(), equalTo((double) (0+1+2+3+4+5+6+7) / 8));
    }

    @Test
    public void filter_WithInheritedSubAggregation() throws Exception {

        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(missing("top_missing").field("tag")
                        .subAggregation(missing("sub_missing")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Missing topMissing = response.getAggregations().get("top_missing");
        assertThat(topMissing, notNullValue());
        assertThat(topMissing.getName(), equalTo("top_missing"));
        assertThat(topMissing.getDocCount(), equalTo(5l));
        assertThat(topMissing.getAggregations().asList().isEmpty(), is(false));

        Missing subMissing = topMissing.getAggregations().get("sub_missing");
        assertThat(subMissing, notNullValue());
        assertThat(subMissing.getName(), equalTo("sub_missing"));
        assertThat(subMissing.getDocCount(), equalTo(5l));
    }

    @Test
    public void missing_WithInheritedSubMissing() throws Exception {

        SearchResponse response = client().prepareSearch()
                .addAggregation(missing("top_missing").field("tag")
                        .subAggregation(missing("sub_missing")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Missing topMissing = response.getAggregations().get("top_missing");
        assertThat(topMissing, notNullValue());
        assertThat(topMissing.getName(), equalTo("top_missing"));
        assertThat(topMissing.getDocCount(), equalTo(8l));
        assertThat(topMissing.getAggregations().asList().isEmpty(), is(false));

        Missing subMissing = topMissing.getAggregations().get("sub_missing");
        assertThat(subMissing, notNullValue());
        assertThat(subMissing.getName(), equalTo("sub_missing"));
        assertThat(subMissing.getDocCount(), equalTo(8l));
    }


}
