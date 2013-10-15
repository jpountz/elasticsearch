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

package org.elasticsearch.search.aggregations.bucket.multi;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.aggregations.bucket.multi.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.multi.terms.Terms;
import org.elasticsearch.search.aggregations.calc.numeric.stats.Stats;
import org.elasticsearch.search.aggregations.calc.numeric.sum.Sum;
import org.elasticsearch.test.AbstractIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 *
 */
public class HistogramTests extends AbstractIntegrationTest {

    @Override
    public Settings getSettings() {
        return randomSettingsBuilder()
                .put("index.number_of_shards", between(1, 5))
                .put("index.number_of_replicas",  between(0, 1))
                .build();
    }

    @Before
    public void init() throws Exception {
        createIndex("idx");
        IndexRequestBuilder[] builders = new IndexRequestBuilder[9]; // NOCOMMIT randomize the size?

        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("idx", "type").setSource(jsonBuilder()
                    .startObject()
                    .field("value", i + 1)
                    .startArray("values").value(i + 1).value(i + 2).endArray()
                    .field("tag", "tag" + i)
                    .endObject());
        }
        indexRandom(true, builders);
        createIndex("idx_unmapped");
    }

    @Test
    public void singleValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(histogram("histo").field("value").interval(4))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        Histogram.Bucket bucket = histo.getByKey(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(0l));
        assertThat(bucket.getDocCount(), equalTo(3l)); // values: 1, 2, 3

        bucket = histo.getByKey(4);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(4l));
        assertThat(bucket.getDocCount(), equalTo(4l)); // values: 4, 5, 6, 7

        bucket = histo.getByKey(8);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(8l));
        assertThat(bucket.getDocCount(), equalTo(2l)); // values: 8, 9
    }

    @Test
    public void singleValuedField_OrderedByKeyAsc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(histogram("histo").field("value").interval(4).order(Histogram.Order.KEY_ASC))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        Histogram.Bucket bucket = histo.buckets().get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(0l));
        assertThat(bucket.getDocCount(), equalTo(3l)); // values: 1, 2, 3

        bucket = histo.buckets().get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(4l));
        assertThat(bucket.getDocCount(), equalTo(4l)); // values: 4, 5, 6, 7

        bucket = histo.buckets().get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(8l));
        assertThat(bucket.getDocCount(), equalTo(2l)); // values: 8, 9
    }

    @Test
    public void singleValuedField_OrderedByKeyDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(histogram("histo").field("value").interval(4).order(Histogram.Order.KEY_DESC))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        Histogram.Bucket bucket = histo.buckets().get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(8l));
        assertThat(bucket.getDocCount(), equalTo(2l)); // values: 8, 9

        bucket = histo.buckets().get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(4l));
        assertThat(bucket.getDocCount(), equalTo(4l)); // values: 4, 5, 6, 7

        bucket = histo.buckets().get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(0l));
        assertThat(bucket.getDocCount(), equalTo(3l)); // values: 1, 2, 3
    }

    @Test
    public void singleValuedField_OrderedByCountAsc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(histogram("histo").field("value").interval(4).order(Histogram.Order.COUNT_ASC))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        Histogram.Bucket bucket = histo.buckets().get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(8l));
        assertThat(bucket.getDocCount(), equalTo(2l)); // values: 8, 9

        bucket = histo.buckets().get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(0l));
        assertThat(bucket.getDocCount(), equalTo(3l)); // values: 1, 2, 3

        bucket = histo.buckets().get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(4l));
        assertThat(bucket.getDocCount(), equalTo(4l)); // values: 4, 5, 6, 7
    }

    @Test
    public void singleValuedField_OrderedByCountDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(histogram("histo").field("value").interval(4).order(Histogram.Order.COUNT_DESC))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        Histogram.Bucket bucket = histo.buckets().get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(4l));
        assertThat(bucket.getDocCount(), equalTo(4l)); // values: 4, 5, 6, 7

        bucket = histo.buckets().get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(0l));
        assertThat(bucket.getDocCount(), equalTo(3l)); // values: 1, 2, 3

        bucket = histo.buckets().get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(8l));
        assertThat(bucket.getDocCount(), equalTo(2l)); // values: 8, 9
    }

    @Test
    public void singleValuedField_WithSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(histogram("histo").field("value").interval(4)
                    .subAggregation(sum("sum").field("value")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        Histogram.Bucket bucket = histo.getByKey(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(0l));
        assertThat(bucket.getDocCount(), equalTo(3l)); // values: 1, 2, 3
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        Sum sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(6.0));

        bucket = histo.getByKey(4);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(4l));
        assertThat(bucket.getDocCount(), equalTo(4l)); // values: 4, 5, 6, 7
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(22.0));

        bucket = histo.getByKey(8);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(8l));
        assertThat(bucket.getDocCount(), equalTo(2l)); // values: 8, 9
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(17.0));
    }

    @Test
    public void singleValuedField_WithSubAggregation_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(histogram("histo").field("value").interval(4)
                        .subAggregation(sum("sum")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        Histogram.Bucket bucket = histo.getByKey(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(0l));
        assertThat(bucket.getDocCount(), equalTo(3l)); // values: 1, 2, 3
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        Sum sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(6.0));

        bucket = histo.getByKey(4);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(4l));
        assertThat(bucket.getDocCount(), equalTo(4l)); // values: 4, 5, 6, 7
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(22.0));

        bucket = histo.getByKey(8);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(8l));
        assertThat(bucket.getDocCount(), equalTo(2l)); // values: 8, 9
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(17.0));
    }

    @Test
    public void singleValuedField_OrderedBySubAggregationAsc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(histogram("histo").field("value").interval(4).order(Histogram.Order.aggregation("sum", true))
                        .subAggregation(sum("sum").field("value")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        Histogram.Bucket bucket = histo.buckets().get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(0l));
        assertThat(bucket.getDocCount(), equalTo(3l)); // values: 1, 2, 3
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        Sum sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(6.0));

        bucket = histo.buckets().get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(8l));
        assertThat(bucket.getDocCount(), equalTo(2l)); // values: 8, 9
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(17.0));

        bucket = histo.buckets().get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(4l));
        assertThat(bucket.getDocCount(), equalTo(4l)); // values: 4, 5, 6, 7
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(22.0));
    }

    @Test
    public void singleValuedField_OrderedBySubAggregationDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(histogram("histo").field("value").interval(4).order(Histogram.Order.aggregation("sum", false))
                        .subAggregation(sum("sum").field("value")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        Histogram.Bucket bucket = histo.buckets().get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(4l));
        assertThat(bucket.getDocCount(), equalTo(4l)); // values: 4, 5, 6, 7
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        Sum sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(22.0));

        bucket = histo.buckets().get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(8l));
        assertThat(bucket.getDocCount(), equalTo(2l)); // values: 8, 9
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(17.0));

        bucket = histo.buckets().get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(0l));
        assertThat(bucket.getDocCount(), equalTo(3l)); // values: 1, 2, 3
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(6.0));
    }

    @Test
    public void singleValuedField_OrderedByMultiValuedSubAggregationAsc_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(histogram("histo").field("value").interval(4).order(Histogram.Order.aggregation("stats.sum", true))
                        .subAggregation(stats("stats")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        Histogram.Bucket bucket = histo.buckets().get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(0l));
        assertThat(bucket.getDocCount(), equalTo(3l)); // values: 1, 2, 3
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        Stats stats = bucket.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getSum(), equalTo(6.0));

        bucket = histo.buckets().get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(8l));
        assertThat(bucket.getDocCount(), equalTo(2l)); // values: 8, 9
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        stats = bucket.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getSum(), equalTo(17.0));

        bucket = histo.buckets().get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(4l));
        assertThat(bucket.getDocCount(), equalTo(4l)); // values: 4, 5, 6, 7
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        stats = bucket.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getSum(), equalTo(22.0));
    }

    @Test
    public void singleValuedField_OrderedByMultiValuedSubAggregationDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(histogram("histo").field("value").interval(4).order(Histogram.Order.aggregation("stats.sum", false))
                        .subAggregation(stats("stats").field("value")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        Histogram.Bucket bucket = histo.buckets().get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(4l));
        assertThat(bucket.getDocCount(), equalTo(4l)); // values: 4, 5, 6, 7
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        Stats stats = bucket.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getSum(), equalTo(22.0));

        bucket = histo.buckets().get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(8l));
        assertThat(bucket.getDocCount(), equalTo(2l)); // values: 8, 9
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        stats = bucket.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getSum(), equalTo(17.0));

        bucket = histo.buckets().get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(0l));
        assertThat(bucket.getDocCount(), equalTo(3l)); // values: 1, 2, 3
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        stats = bucket.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getSum(), equalTo(6.0));
    }

    @Test
    public void singleValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(histogram("histo").field("value").script("_value + 1").interval(4))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        Histogram.Bucket bucket = histo.getByKey(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(0l));
        assertThat(bucket.getDocCount(), equalTo(2l)); // values: 2, 3

        bucket = histo.getByKey(4);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(4l));
        assertThat(bucket.getDocCount(), equalTo(4l)); // values: 4, 5, 6, 7

        bucket = histo.getByKey(8);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(8l));
        assertThat(bucket.getDocCount(), equalTo(3l)); // values: 8, 9, 10
    }

    @Test
    public void multiValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(histogram("histo").field("values").interval(4))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        Histogram.Bucket bucket = histo.getByKey(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(0l));
        assertThat(bucket.getDocCount(), equalTo(3l)); // docs: [1, 2], [2, 3], [3, 4]

        bucket = histo.getByKey(4);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(4l));
        assertThat(bucket.getDocCount(), equalTo(5l)); // docs: [3, 4], [4, 5], [5, 6], [6, 7], [7, 8]

        bucket = histo.getByKey(8);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(8l));
        assertThat(bucket.getDocCount(), equalTo(3l)); // docs: [8], [8. 9], [9, 10]
    }

    @Test
    public void multiValuedField_OrderedByKeyDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(histogram("histo").field("values").interval(4).order(Histogram.Order.KEY_DESC))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        Histogram.Bucket bucket = histo.buckets().get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(8l));
        assertThat(bucket.getDocCount(), equalTo(3l)); // docs: [8], [8. 9], [9, 10]

        bucket = histo.buckets().get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(4l));
        assertThat(bucket.getDocCount(), equalTo(5l)); // docs: [3, 4], [4, 5], [5, 6], [6, 7], [7, 8]

        bucket = histo.buckets().get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(0l));
        assertThat(bucket.getDocCount(), equalTo(3l)); // docs: [1, 2], [2, 3], [3, 4]
    }

    @Test
    public void multiValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(histogram("histo").field("values").script("_value + 1").interval(4))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        Histogram.Bucket bucket = histo.getByKey(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(0l));
        assertThat(bucket.getDocCount(), equalTo(2l)); // docs: [2, 3], [3, 4]

        bucket = histo.getByKey(4);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(4l));
        assertThat(bucket.getDocCount(), equalTo(5l)); // docs: [3, 4], [4, 5], [5, 6], [6, 7], [7, 8]

        bucket = histo.getByKey(8);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(8l));
        assertThat(bucket.getDocCount(), equalTo(4l)); // docs: [7, 8], [8, 9], [9. 10], [10, 11]
    }


//    2,3
//    3,4
//    4,5
//    5,6
//    6,7
//    7,8
//    8,9
//    9,10
//    10,11
//    11,12

//    0 - 2
//    4 - 5
//    8 - 5

    @Test
    public void multiValuedField_WithValueScript_WithInheritedSubAggregator() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(histogram("histo").field("values").script("_value + 1").interval(4)
                    .subAggregation(terms("values").order(Terms.Order.TERM_ASC)))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        Histogram.Bucket bucket = histo.getByKey(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(0l));
        assertThat(bucket.getDocCount(), equalTo(2l)); // docs: [2, 3], [3, 4]
        Terms terms = bucket.getAggregations().get("values");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("values"));
        assertThat(terms.buckets().size(), equalTo(2)); // values that match the bucket: 2, 3
        Iterator<Terms.Bucket> iter = terms.iterator();
        assertThat(iter.next().getTermAsNumber().longValue(), equalTo(2l));
        assertThat(iter.next().getTermAsNumber().longValue(), equalTo(3l));

        bucket = histo.getByKey(4);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(4l));
        assertThat(bucket.getDocCount(), equalTo(5l)); // docs: [3, 4], [4, 5], [5, 6], [6, 7], [7, 8]
        terms = bucket.getAggregations().get("values");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("values"));
        assertThat(terms.buckets().size(), equalTo(4)); // values that match the bucket: 4, 5, 6, 7
        iter = terms.iterator();
        assertThat(iter.next().getTermAsNumber().longValue(), equalTo(4l));
        assertThat(iter.next().getTermAsNumber().longValue(), equalTo(5l));
        assertThat(iter.next().getTermAsNumber().longValue(), equalTo(6l));
        assertThat(iter.next().getTermAsNumber().longValue(), equalTo(7l));

        bucket = histo.getByKey(8);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(8l));
        assertThat(bucket.getDocCount(), equalTo(4l)); // docs: [7, 8], [8, 9], [9. 10], [10, 11]
        terms = bucket.getAggregations().get("values");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("values"));
        assertThat(terms.buckets().size(), equalTo(4)); // values that match the bucket: 8, 9, 10, 11
        iter = terms.iterator();
        assertThat(iter.next().getTermAsNumber().longValue(), equalTo(8l));
        assertThat(iter.next().getTermAsNumber().longValue(), equalTo(9l));
        assertThat(iter.next().getTermAsNumber().longValue(), equalTo(10l));
        assertThat(iter.next().getTermAsNumber().longValue(), equalTo(11l));
    }

    @Test
    public void script_SingleValue() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(histogram("histo").script("doc['value'].value").interval(4))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        Histogram.Bucket bucket = histo.getByKey(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(0l));
        assertThat(bucket.getDocCount(), equalTo(3l)); // values: 1, 2, 3

        bucket = histo.getByKey(4);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(4l));
        assertThat(bucket.getDocCount(), equalTo(4l)); // values: 4, 5, 6, 7

        bucket = histo.getByKey(8);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(8l));
        assertThat(bucket.getDocCount(), equalTo(2l)); // values: 8, 9
    }

    @Test
    public void script_SingleValue_WithSubAggregator_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(histogram("histo").script("doc['value'].value").interval(4)
                    .subAggregation(sum("sum")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        Histogram.Bucket bucket = histo.getByKey(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(0l));
        assertThat(bucket.getDocCount(), equalTo(3l)); // values: 1, 2, 3
        Sum sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(6.0));

        bucket = histo.getByKey(4);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(4l));
        assertThat(bucket.getDocCount(), equalTo(4l)); // values: 4, 5, 6, 7
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(22.0));

        bucket = histo.getByKey(8);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(8l));
        assertThat(bucket.getDocCount(), equalTo(2l)); // values: 8, 9
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(17.0));
    }

    @Test
    public void script_MultiValued() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(histogram("histo").script("doc['values'].values").interval(4))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        Histogram.Bucket bucket = histo.getByKey(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(0l));
        assertThat(bucket.getDocCount(), equalTo(3l)); // docs: [1, 2], [2, 3], [3, 4]

        bucket = histo.getByKey(4);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(4l));
        assertThat(bucket.getDocCount(), equalTo(5l)); // docs: [3, 4], [4, 5], [5, 6], [6, 7], [7, 8]

        bucket = histo.getByKey(8);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(8l));
        assertThat(bucket.getDocCount(), equalTo(3l)); // docs: [8], [8. 9], [9, 10]
    }

    @Test
    public void script_MultiValued_WithAggregatorInherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(histogram("histo").script("doc['values'].values").interval(4)
                    .subAggregation(sum("sum")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        Histogram.Bucket bucket = histo.getByKey(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(0l));
        assertThat(bucket.getDocCount(), equalTo(3l)); // docs: [1, 2], [2, 3], [3, 4]
        Sum sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo((double) 1+2+2+3+3));

        bucket = histo.getByKey(4);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(4l));
        assertThat(bucket.getDocCount(), equalTo(5l)); // docs: [3, 4], [4, 5], [5, 6], [6, 7], [7, 8]
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo((double) 4+4+5+5+6+6+7+7));

        bucket = histo.getByKey(8);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(8l));
        assertThat(bucket.getDocCount(), equalTo(3l)); // docs: [7, 8], [8. 9], [9, 10]
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo((double) 8+8+9+9+10));
    }

    @Test
    public void unmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped")
                .addAggregation(histogram("histo").field("value").interval(4))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(0));
    }

    @Test
    public void partiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
                .addAggregation(histogram("histo").field("value").interval(4))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        Histogram.Bucket bucket = histo.getByKey(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(0l));
        assertThat(bucket.getDocCount(), equalTo(3l)); // values: 1, 2, 3

        bucket = histo.getByKey(4);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(4l));
        assertThat(bucket.getDocCount(), equalTo(4l)); // values: 4, 5, 6, 7

        bucket = histo.getByKey(8);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(8l));
        assertThat(bucket.getDocCount(), equalTo(2l)); // values: 8, 9
    }

}
