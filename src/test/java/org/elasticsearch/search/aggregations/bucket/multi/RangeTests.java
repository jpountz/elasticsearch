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

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.aggregations.bucket.multi.range.Range;
import org.elasticsearch.search.aggregations.calc.numeric.avg.Avg;
import org.elasticsearch.search.aggregations.calc.numeric.sum.Sum;
import org.elasticsearch.test.AbstractIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 *
 */
public class RangeTests extends AbstractIntegrationTest {

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

        for (int i = 0; i < 10; i++) {
            client().prepareIndex("idx", "type").setSource(jsonBuilder()
                    .startObject()
                    .field("value", i+1)
                    .startArray("values").value(i+1).value(i+2).endArray()
                    .endObject())
                    .execute().actionGet();
        }

        createIndex("idx_unmapped");

        client().admin().indices().prepareFlush().execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();
    }

    @Test
    public void singleValueField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(range("range")
                        .field("value")
                        .addUnboundedTo(3)
                        .addRange(3, 6)
                        .addUnboundedFrom(6))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("*-3.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("3.0-6.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(3l));

        bucket = range.getByKey("6.0-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(5l));
    }

    @Test
    public void singleValueField_WithCustomKey() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(range("range")
                        .field("value")
                        .addUnboundedTo("r1", 3)
                        .addRange("r2", 3, 6)
                        .addUnboundedFrom("r3", 6))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("r1");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("r1"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("r2");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("r2"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(3l));

        bucket = range.getByKey("r3");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("r3"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(5l));
    }

    @Test
    public void singleValuedField_WithSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(range("range")
                        .field("value")
                        .addUnboundedTo(3)
                        .addRange(3, 6)
                        .addUnboundedFrom(6)
                        .subAggregation(sum("sum").field("value")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("*-3.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(2l));
        Sum sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(3.0)); // 1 + 2

        bucket = range.getByKey("3.0-6.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(3l));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(12.0)); // 3 + 4 + 5

        bucket = range.getByKey("6.0-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(5l));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(40.0)); // 6 + 7 + 8 + 9 + 10
    }

    @Test
    public void singleValuedField_WithSubAggregation_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(range("range")
                        .field("value")
                        .addUnboundedTo(3)
                        .addRange(3, 6)
                        .addUnboundedFrom(6)
                        .subAggregation(avg("avg")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("*-3.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(2l));
        Avg avg = bucket.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getValue(), equalTo(1.5)); // (1 + 2) / 2

        bucket = range.getByKey("3.0-6.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(3l));
        avg = bucket.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getValue(), equalTo(4.0)); // (3 + 4 + 5) / 3

        bucket = range.getByKey("6.0-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(5l));
        avg = bucket.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getValue(), equalTo(8.0)); // (6 + 7 + 8 + 9 + 10) / 5
    }

    @Test
    public void singleValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(range("range")
                        .field("value")
                        .script("_value + 1")
                        .addUnboundedTo(3)
                        .addRange(3, 6)
                        .addUnboundedFrom(6))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("*-3.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(1l)); // 2

        bucket = range.getByKey("3.0-6.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(3l)); // 3, 4, 5

        bucket = range.getByKey("6.0-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(6l)); // 6, 7, 8, 9, 10, 11
    }

    /*
    [1, 2]
    [2, 3]
    [3, 4]
    [4, 5]
    [5, 6]
    [6, 7]
    [7, 8j
    [8, 9]
    [9, 10]
    [10, 11]
     */

    @Test
    public void multiValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(range("range")
                        .field("values")
                        .addUnboundedTo(3)
                        .addRange(3, 6)
                        .addUnboundedFrom(6))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("*-3.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("3.0-6.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(4l));

        bucket = range.getByKey("6.0-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(6l));
    }

    /*
    [2, 3]
    [3, 4]
    [4, 5]
    [5, 6]
    [6, 7]
    [7, 8j
    [8, 9]
    [9, 10]
    [10, 11]
    [11, 12]
     */

    @Test
    public void multiValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(range("range")
                        .field("values")
                        .script("_value + 1")
                        .addUnboundedTo(3)
                        .addRange(3, 6)
                        .addUnboundedFrom(6))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("*-3.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(1l));

        bucket = range.getByKey("3.0-6.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(4l));

        bucket = range.getByKey("6.0-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(7l));
    }

    /*
    [2, 3]
    [3, 4]
    [4, 5]
    [5, 6]
    [6, 7]
    [7, 8j
    [8, 9]
    [9, 10]
    [10, 11]
    [11, 12]

    r1: 2
    r2: 3, 3, 4, 4, 5, 5
    r3: 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12
     */

    @Test
    public void multiValuedField_WithValueScript_WithInheritedSubAggregator() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(range("range")
                        .field("values")
                        .script("_value + 1")
                        .addUnboundedTo(3)
                        .addRange(3, 6)
                        .addUnboundedFrom(6)
                        .subAggregation(sum("sum")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("*-3.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(1l));
        Sum sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo((double) 2));

        bucket = range.getByKey("3.0-6.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(4l));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo((double) 3+3+4+4+5+5));

        bucket = range.getByKey("6.0-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(7l));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo((double) 6+6+7+7+8+8+9+9+10+10+11+11+12));
    }

    @Test
    public void script_SingleValue() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(range("range")
                        .script("doc['value'].value")
                        .addUnboundedTo(3)
                        .addRange(3, 6)
                        .addUnboundedFrom(6))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("*-3.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("3.0-6.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(3l));

        bucket = range.getByKey("6.0-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(5l));
    }

    @Test
    public void script_SingleValue_WithSubAggregator_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(range("range")
                        .script("doc['value'].value")
                        .addUnboundedTo(3)
                        .addRange(3, 6)
                        .addUnboundedFrom(6)
                        .subAggregation(avg("avg")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("*-3.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(2l));
        Avg avg = bucket.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getValue(), equalTo(1.5)); // (1 + 2) / 2

        bucket = range.getByKey("3.0-6.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(3l));
        avg = bucket.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getValue(), equalTo(4.0)); // (3 + 4 + 5) / 3

        bucket = range.getByKey("6.0-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(5l));
        avg = bucket.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getValue(), equalTo(8.0)); // (6 + 7 + 8 + 9 + 10) / 5
    }

    @Test
    public void script_MultiValued() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(range("range")
                        .script("doc['values'].values")
                        .addUnboundedTo(3)
                        .addRange(3, 6)
                        .addUnboundedFrom(6))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("*-3.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("3.0-6.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(4l));

        bucket = range.getByKey("6.0-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(6l));
    }
    
    /*
    [1, 2]
    [2, 3]
    [3, 4]
    [4, 5]
    [5, 6]
    [6, 7]
    [7, 8j
    [8, 9]
    [9, 10]
    [10, 11]
    
    r1: 1, 2, 2
    r2: 3, 3, 4, 4, 5, 5
    r3: 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11
     */

    @Test
    public void script_MultiValued_WithAggregatorInherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(range("range")
                        .script("doc['values'].values")
                        .addUnboundedTo("r1", 3)
                        .addRange("r2", 3, 6)
                        .addUnboundedFrom("r3", 6)
                        .subAggregation(sum("sum")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("r1");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("r1"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(2l));
        Sum sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo((double) 1+2+2));

        bucket = range.getByKey("r2");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("r2"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(4l));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo((double) 3+3+4+4+5+5));

        bucket = range.getByKey("r3");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("r3"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(6l));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo((double) 6+6+7+7+8+8+9+9+10+10+11));
    }

    @Test
    public void unmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped")
                .addAggregation(range("range")
                        .field("value")
                        .addUnboundedTo(3)
                        .addRange(3, 6)
                        .addUnboundedFrom(6))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("*-3.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(0l));

        bucket = range.getByKey("3.0-6.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(0l));

        bucket = range.getByKey("6.0-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(0l));
    }

    @Test
    public void partiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
                .addAggregation(range("range")
                        .field("value")
                        .addUnboundedTo(3)
                        .addRange(3, 6)
                        .addUnboundedFrom(6))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        Range.Bucket bucket = range.getByKey("*-3.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-3.0"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getTo(), equalTo(3.0));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("3.0-6.0");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("3.0-6.0"));
        assertThat(bucket.getFrom(), equalTo(3.0));
        assertThat(bucket.getTo(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(3l));

        bucket = range.getByKey("6.0-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("6.0-*"));
        assertThat(bucket.getFrom(), equalTo(6.0));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(5l));
    }


}
