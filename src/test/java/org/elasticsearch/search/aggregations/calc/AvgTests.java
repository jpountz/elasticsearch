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

package org.elasticsearch.search.aggregations.calc;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.calc.numeric.avg.Avg;
import org.junit.Test;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class AvgTests extends AbstractNumericTests {

    @Test
    public void testUnmapped() throws Exception {
        client().admin().cluster().prepareHealth("idx2").setWaitForGreenStatus().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch("idx2")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").field("value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(0l));

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo(Double.NaN));
    }

    @Test
    public void testSingleValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").field("value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (1+2+3+4+5+6+7+8+9+10) / 10));
    }

    @Override
    public void testSingleValuedField_PartiallyUnmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx", "idx2")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").field("value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (1+2+3+4+5+6+7+8+9+10) / 10));
    }

    @Test
    public void testSingleValuedField_WithValueScript() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").field("value").script("_value + 1"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (2+3+4+5+6+7+8+9+10+11) / 10));
    }

    @Test
    public void testSingleValuedField_WithValueScript_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").field("value").script("_value + inc").param("inc", 1))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (2+3+4+5+6+7+8+9+10+11) / 10));
    }


    @Test
    public void testMultiValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").field("values"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (2+3+3+4+4+5+5+6+6+7+7+8+8+9+9+10+10+11+11+12) / 20));
    }

    @Test
    public void testMultiValuedField_WithValueScript() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").field("values").script("_value + 1"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (3+4+4+5+5+6+6+7+7+8+8+9+9+10+10+11+11+12+12+13) / 20));
    }

    @Test
    public void testMultiValuedField_WithValueScript_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").field("values").script("_value + inc").param("inc", 1))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (3+4+4+5+5+6+6+7+7+8+8+9+9+10+10+11+11+12+12+13) / 20));
    }

    @Test
    public void testScript_SingleValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").script("doc['value'].value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (1+2+3+4+5+6+7+8+9+10) / 10));
    }

    @Test
    public void testScript_SingleValued_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").script("doc['value'].value + inc").param("inc", 1))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (2+3+4+5+6+7+8+9+10+11) / 10));
    }

    @Test
    public void testScript_ExplicitSingleValued_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").script("doc['value'].value + inc").param("inc", 1))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (2+3+4+5+6+7+8+9+10+11) / 10));
    }

    @Test
    public void testScript_MultiValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").script("new double[] { doc['value'].value, doc['value'].value + 1 }"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (1+2+2+3+3+4+4+5+5+6+6+7+7+8+8+9+9+10+10+11) / 20));
    }

    @Test
    public void testScript_ExplicitMultiValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").script("new double[] { doc['value'].value, doc['value'].value + 1 }"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (1+2+2+3+3+4+4+5+5+6+6+7+7+8+8+9+9+10+10+11) / 20));
    }

    @Test
    public void testScript_MultiValued_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").script("new double[] { doc['value'].value, doc['value'].value + inc }").param("inc", 1))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (1+2+2+3+3+4+4+5+5+6+6+7+7+8+8+9+9+10+10+11) / 20));
    }

}