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

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.calc.numeric.min.*;
import org.junit.Test;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.min;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class MinTests extends AbstractNumericTests {

    @Test
    public void testUnmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx2")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").field("value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(0l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(Double.POSITIVE_INFINITY));
    }

    @Test
    public void testSingleValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").field("value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(1.0));
    }

    @Test
    public void testSingleValuedField_PartiallyUnmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx", "idx2")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").field("value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(1.0));
    }

    @Test
    public void testSingleValuedField_WithValueScript() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").field("value").script("_value - 1"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(0.0));
    }

    @Test
    public void testSingleValuedField_WithValueScript_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").field("value").script("_value - dec").param("dec", 1))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(0.0));
    }

    @Test
    public void testMultiValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").field("values"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(2.0));
    }

    @Test
    public void testMultiValuedField_WithValueScript() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").field("values").script("_value - 1"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(1.0));
    }

    @Test
    public void testMultiValuedField_WithValueScript_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").field("values").script("_value - dec").param("dec", 1))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(1.0));
    }

    @Test
    public void testScript_SingleValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").script("doc['value'].value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(1.0));
    }

    @Test
    public void testScript_SingleValued_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").script("doc['value'].value - dec").param("dec", 1))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(0.0));
    }

    @Test
    public void testScript_ExplicitSingleValued_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").script("doc['value'].value - dec").param("dec", 1).multiValued(false))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(0.0));
    }

    @Test
    public void testScript_MultiValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").script("doc['values'].values"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(2.0));
    }

    @Test
    public void testScript_ExplicitMultiValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").script("doc['values'].values").multiValued(true))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(2.0));

        try {
            client().prepareSearch("idx")
                    .setQuery(matchAllQuery())
                    .addAggregation(min("min").script("doc['values'].values").multiValued(false))
                    .execute().actionGet();

            fail("expected an error to be thrown as we tell es to treat the value generated by the script as a single value thus it tries to " +
                    "cast it to a Number, but it should fail as the script returns a double array");

        } catch (ElasticSearchException ese) {
        }
    }

    @Test
    public void testScript_MultiValued_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").script("List values = doc['values'].values; double[] res = new double[values.length]; for (int i = 0; i < res.length; i++) { res[i] = values.get(i) - dec; }; return res;").param("dec", 1))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(1.0));
    }

}