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
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.aggregations.bucket.single.filter.Filter;
import org.elasticsearch.search.aggregations.calc.numeric.avg.Avg;
import org.elasticsearch.test.AbstractIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filter;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 *
 */
public class FilterTests extends AbstractIntegrationTest {

    @Override
    public Settings getSettings() {
        return randomSettingsBuilder()
                .put("index.number_of_shards", between(1, 5))
                .put("index.number_of_replicas", between(0, 1))
                .build();
    }

    @Before
    public void init() throws Exception {
        createIndex("idx");
        createIndex("idx2");
        List<IndexRequestBuilder> builders = new ArrayList<IndexRequestBuilder>();
        for (int i = 0; i < 5; i++) { // NOCOMMIT randomize the size
            builders.add(client().prepareIndex("idx", "type", ""+i+1).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i + 1)
                    .field("tag", "tag1")
                    .endObject()));
        }
        for (int i = 0; i < 5; i++) { // NOCOMMIT randomize the size
            builders.add(client().prepareIndex("idx", "type", ""+i+6).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i + 6)
                    .field("tag", "tag2")
                    .field("name", "name" + i+6)
                    .endObject()));
        }
        indexRandom(true, builders);
    }

    @Test
    public void simple() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(filter("tag1").filter(termFilter("tag", "tag1")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Filter filter = response.getAggregations().get("tag1");
        assertThat(filter, notNullValue());
        assertThat(filter.getName(), equalTo("tag1"));
        assertThat(filter.getDocCount(), equalTo(5l));
    }

    @Test
    public void withSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(filter("tag1")
                        .filter(termFilter("tag", "tag1"))
                        .subAggregation(avg("avg_value").field("value")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Filter filter = response.getAggregations().get("tag1");
        assertThat(filter, notNullValue());
        assertThat(filter.getName(), equalTo("tag1"));
        assertThat(filter.getDocCount(), equalTo(5l));

        assertThat(filter.getAggregations().asList().isEmpty(), is(false));
        Avg avgValue = filter.getAggregations().get("avg_value");
        assertThat(avgValue, notNullValue());
        assertThat(avgValue.getName(), equalTo("avg_value"));
        assertThat(avgValue.getValue(), equalTo((double) (1+2+3+4+5) / 5));
    }

    @Test
    public void withContextBasedSubAggregation() throws Exception {

        try {
            client().prepareSearch("idx")
                    .addAggregation(filter("tag1")
                            .filter(termFilter("tag", "tag1"))
                            .subAggregation(avg("avg_value")))
                    .execute().actionGet();

            fail("expected execution to fail - an attempt to have a context based numeric sub-aggregation, but there is not value source" +
                    "context which the sub-aggregation can inherit");

        } catch (ElasticSearchException ese) {
        }
    }
}
