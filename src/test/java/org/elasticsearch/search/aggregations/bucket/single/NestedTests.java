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

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.aggregations.bucket.single.nested.Nested;
import org.elasticsearch.search.aggregations.calc.numeric.stats.Stats;
import org.elasticsearch.test.AbstractIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.nested;
import static org.elasticsearch.search.aggregations.AggregationBuilders.stats;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 *
 */
public class NestedTests extends AbstractIntegrationTest {

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

        client().admin().indices().prepareCreate("idx")
                .addMapping("type", "nested", "type=nested")
                .execute().actionGet();

        for (int i = 0; i < 5; i++) {
            client().prepareIndex("idx", "type", ""+i+1).setSource(jsonBuilder()
                    .startObject()
                        .field("value", i + 1)
                        .startArray("nested")
                            .startObject().field("value", i + 1).endObject()
                            .startObject().field("value", i + 2).endObject()
                            .startObject().field("value", i + 3).endObject()
                            .startObject().field("value", i + 4).endObject()
                            .startObject().field("value", i + 5).endObject()
                        .endArray()
                    .endObject())
                    .execute().actionGet();
        }
        client().admin().indices().prepareFlush().execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();
    }

    @Test
    public void simple() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(nested("nested").path("nested")
                        .subAggregation(stats("nested_value_stats").field("nested.value")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Nested nested = response.getAggregations().get("nested");
        assertThat(nested, notNullValue());
        assertThat(nested.getName(), equalTo("nested"));
        assertThat(nested.getDocCount(), equalTo(25l));
        assertThat(nested.getAggregations().asList().isEmpty(), is(false));

        Stats stats = nested.getAggregations().get("nested_value_stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo(9.0));
        assertThat(stats.getCount(), equalTo(25l));
        assertThat(stats.getSum(), equalTo((double) 1+2+3+4+5+2+3+4+5+6+3+4+5+6+7+4+5+6+7+8+5+6+7+8+9));
        assertThat(stats.getAvg(), equalTo((double) (1+2+3+4+5+2+3+4+5+6+3+4+5+6+7+4+5+6+7+8+5+6+7+8+9) / 25 ));
    }

    @Test
    public void OnNonNestedField() throws Exception {

        try {
            client().prepareSearch("idx")
                    .addAggregation(nested("nested").path("value")
                            .subAggregation(stats("nested_value_stats").field("nested.value")))
                    .execute().actionGet();

            fail("expected execution to fail - an attempt to nested facet on non-nested field/path");

        } catch (ElasticSearchException ese) {
        }
    }
}
