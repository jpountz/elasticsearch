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
import org.elasticsearch.search.aggregations.bucket.multi.range.date.DateRange;
import org.elasticsearch.search.aggregations.calc.numeric.max.Max;
import org.elasticsearch.search.aggregations.calc.numeric.min.Min;
import org.elasticsearch.search.aggregations.calc.numeric.sum.Sum;
import org.elasticsearch.test.AbstractIntegrationTest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;

/**
 *
 */
public class DateRangeTests extends AbstractIntegrationTest {

    @Override
    public Settings getSettings() {
        return randomSettingsBuilder()
                .put("index.number_of_shards", between(1, 5))
                .put("index.number_of_replicas", between(0, 1))
                .build();
    }

    private DateTime date(int month, int day) {
        return new DateTime(2012, month, day, 0, 0, DateTimeZone.UTC);
    }

    private void indexDoc(int month, int day, int value) throws Exception {
        client().prepareIndex("idx", "type").setSource(jsonBuilder()
                .startObject()
                .field("value", value)
                .field("date", date(month, day))
                .startArray("dates").value(date(month, day)).value(date(month + 1, day + 1)).endArray()
                .endObject())
                .execute().actionGet();
    }

    @Before
    public void init() throws Exception {
        createIndex("idx");
        // NOCOMMIT: we must randomize the docs here the risk is too high that we are depending on the order
        // we should also index way more docs that those (maybe just with dummy fields to get more variation or
        // use a filter and an alias to filter those that are relevant out)
        indexDoc(1, 2, 1);  // Jan 2
        indexDoc(2, 2, 2);  // Feb 2
        indexDoc(2, 15, 3); // Feb 15
        indexDoc(3, 2, 4);  // Mar 2
        indexDoc(3, 15, 5); // Mar 15
        indexDoc(3, 23, 6); // Mar 23

        createIndex("idx_unmapped");

        client().admin().indices().prepareFlush().execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();
    }

    @Test
    public void singleValueField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .field("date")
                        .addUnboundedTo(date(2, 15))
                        .addRange(date(2, 15), date(3, 15))
                        .addUnboundedFrom(date(3, 15)))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateRange range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        DateRange.Bucket bucket = range.getByKey("*-2012-02-15T00:00:00.000Z");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsDate(), nullValue());
        assertThat(bucket.getTo(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getFrom(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getTo(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("2012-03-15T00:00:00.000Z-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(bucket.getFrom(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsDate(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(2l));
    }

    @Test
    public void singleValueField_WithStringDates() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .field("date")
                        .addUnboundedTo("2012-02-15")
                        .addRange("2012-02-15", "2012-03-15")
                        .addUnboundedFrom("2012-03-15"))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateRange range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        DateRange.Bucket bucket = range.getByKey("*-2012-02-15T00:00:00.000Z");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsDate(), nullValue());
        assertThat(bucket.getTo(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getFrom(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getTo(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("2012-03-15T00:00:00.000Z-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(bucket.getFrom(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsDate(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(2l));
    }

    @Test
    public void singleValueField_WithStringDates_WithCustomFormat() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .field("date")
                        .format("yyyy-MM-dd")
                        .addUnboundedTo("2012-02-15")
                        .addRange("2012-02-15", "2012-03-15")
                        .addUnboundedFrom("2012-03-15"))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateRange range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        DateRange.Bucket bucket = range.getByKey("*-2012-02-15");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-2012-02-15"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsDate(), nullValue());
        assertThat(bucket.getTo(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("2012-02-15-2012-03-15");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-02-15-2012-03-15"));
        assertThat(bucket.getFrom(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getTo(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("2012-03-15-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-03-15-*"));
        assertThat(bucket.getFrom(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsDate(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(2l));
    }

    @Test
    public void singleValueField_WithDateMath() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .field("date")
                        .addUnboundedTo("2012-02-15")
                        .addRange("2012-02-15", "2012-02-15||+1M")
                        .addUnboundedFrom("2012-02-15||+1M"))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateRange range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        DateRange.Bucket bucket = range.getByKey("*-2012-02-15T00:00:00.000Z");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsDate(), nullValue());
        assertThat(bucket.getTo(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getFrom(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getTo(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("2012-03-15T00:00:00.000Z-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(bucket.getFrom(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsDate(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(2l));
    }

    @Test
    public void singleValueField_WithCustomKey() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .field("date")
                        .addUnboundedTo("r1", date(2, 15))
                        .addRange("r2", date(2, 15), date(3, 15))
                        .addUnboundedFrom("r3", date(3, 15)))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateRange range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        DateRange.Bucket bucket = range.getByKey("r1");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("r1"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsDate(), nullValue());
        assertThat(bucket.getTo(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("r2");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("r2"));
        assertThat(bucket.getFrom(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getTo(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("r3");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("r3"));
        assertThat(bucket.getFrom(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsDate(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(2l));
    }

    /*
        Jan 2,      1
        Feb 2,      2
        Feb 15,     3
        Mar 2,      4
        Mar 15,     5
        Mar 23,     6
     */

    @Test
    public void singleValuedField_WithSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .field("date")
                        .addUnboundedTo("r1", date(2, 15))
                        .addRange("r2", date(2, 15), date(3, 15))
                        .addUnboundedFrom("r3", date(3, 15))
                        .subAggregation(sum("sum").field("value")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateRange range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        DateRange.Bucket bucket = range.getByKey("r1");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("r1"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsDate(), nullValue());
        assertThat(bucket.getTo(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getDocCount(), equalTo(2l));
        Sum sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo((double) 1 + 2));

        bucket = range.getByKey("r2");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("r2"));
        assertThat(bucket.getFrom(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getTo(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getDocCount(), equalTo(2l));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo((double) 3 + 4));

        bucket = range.getByKey("r3");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("r3"));
        assertThat(bucket.getFrom(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsDate(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(2l));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo((double) 5 + 6));
    }

    @Test
    public void singleValuedField_WithSubAggregation_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .field("date")
                        .addUnboundedTo("r1", date(2, 15))
                        .addRange("r2", date(2, 15), date(3, 15))
                        .addUnboundedFrom("r3", date(3, 15))
                        .subAggregation(min("min")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateRange range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        DateRange.Bucket bucket = range.getByKey("r1");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("r1"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsDate(), nullValue());
        assertThat(bucket.getTo(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getDocCount(), equalTo(2l));
        Min min = bucket.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getValue(), equalTo((double) date(1, 2).getMillis()));

        bucket = range.getByKey("r2");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("r2"));
        assertThat(bucket.getFrom(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getTo(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getDocCount(), equalTo(2l));
        min = bucket.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getValue(), equalTo((double) date(2, 15).getMillis()));

        bucket = range.getByKey("r3");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("r3"));
        assertThat(bucket.getFrom(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsDate(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(2l));
        min = bucket.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getValue(), equalTo((double) date(3, 15).getMillis()));
    }

    /*
        Jan 2,  Feb 3,      1
        Feb 2,  Mar 3,      2
        Feb 15, Mar 16,     3
        Mar 2,  Apr 3,      4
        Mar 15, Apr 16      5
        Mar 23, Apr 24      6
     */

    @Test
    public void multiValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .field("dates")
                        .addUnboundedTo(date(2, 15))
                        .addRange(date(2, 15), date(3, 15))
                        .addUnboundedFrom(date(3, 15)))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateRange range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        DateRange.Bucket bucket = range.getByKey("*-2012-02-15T00:00:00.000Z");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsDate(), nullValue());
        assertThat(bucket.getTo(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getFrom(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getTo(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getDocCount(), equalTo(3l));

        bucket = range.getByKey("2012-03-15T00:00:00.000Z-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(bucket.getFrom(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsDate(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(4l));
    }

    /*
        Feb 2,  Mar 3,      1
        Mar 2,  Apr 3,      2
        Mar 15, Apr 16,     3
        Apr 2,  May 3,      4
        Apr 15, May 16      5
        Apr 23, May 24      6
     */


    @Test
    public void multiValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .field("dates")
                        .script("new DateTime(_value.longValue(), DateTimeZone.UTC).plusMonths(1).getMillis()")
                        .addUnboundedTo(date(2, 15))
                        .addRange(date(2, 15), date(3, 15))
                        .addUnboundedFrom(date(3, 15)))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateRange range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        DateRange.Bucket bucket = range.getByKey("*-2012-02-15T00:00:00.000Z");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsDate(), nullValue());
        assertThat(bucket.getTo(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getDocCount(), equalTo(1l));

        bucket = range.getByKey("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getFrom(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getTo(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("2012-03-15T00:00:00.000Z-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(bucket.getFrom(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsDate(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(5l));
    }

    /*
        Feb 2,  Mar 3,      1
        Mar 2,  Apr 3,      2
        Mar 15, Apr 16,     3
        Apr 2,  May 3,      4
        Apr 15, May 16      5
        Apr 23, May 24      6
     */

    @Test
    public void multiValuedField_WithValueScript_WithInheritedSubAggregator() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .field("dates")
                        .script("new DateTime(_value.longValue(), DateTimeZone.UTC).plusMonths(1).getMillis()")
                        .addUnboundedTo(date(2, 15))
                        .addRange(date(2, 15), date(3, 15))
                        .addUnboundedFrom(date(3, 15))
                        .subAggregation(max("max")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateRange range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        DateRange.Bucket bucket = range.getByKey("*-2012-02-15T00:00:00.000Z");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsDate(), nullValue());
        assertThat(bucket.getTo(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getDocCount(), equalTo(1l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) date(2, 2).getMillis()));

        bucket = range.getByKey("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getFrom(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getTo(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getDocCount(), equalTo(2l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) date(3, 3).getMillis()));

        bucket = range.getByKey("2012-03-15T00:00:00.000Z-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(bucket.getFrom(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsDate(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(5l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) date(5, 24).getMillis()));
    }

    @Test
    public void script_SingleValue() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .script("doc['date'].value")
                        .addUnboundedTo(date(2, 15))
                        .addRange(date(2, 15), date(3, 15))
                        .addUnboundedFrom(date(3, 15)))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateRange range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        DateRange.Bucket bucket = range.getByKey("*-2012-02-15T00:00:00.000Z");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsDate(), nullValue());
        assertThat(bucket.getTo(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getFrom(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getTo(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("2012-03-15T00:00:00.000Z-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(bucket.getFrom(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsDate(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(2l));
    }

    @Test
    public void script_SingleValue_WithSubAggregator_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .script("doc['date'].value")
                        .addUnboundedTo(date(2, 15))
                        .addRange(date(2, 15), date(3, 15))
                        .addUnboundedFrom(date(3, 15))
                        .subAggregation(max("max")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateRange range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        DateRange.Bucket bucket = range.getByKey("*-2012-02-15T00:00:00.000Z");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsDate(), nullValue());
        assertThat(bucket.getTo(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getDocCount(), equalTo(2l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) date(2, 2).getMillis()));

        bucket = range.getByKey("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getFrom(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getTo(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getDocCount(), equalTo(2l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) date(3, 2).getMillis()));

        bucket = range.getByKey("2012-03-15T00:00:00.000Z-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(bucket.getFrom(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsDate(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(2l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) date(3, 23).getMillis()));
    }

    /*
        Jan 2,  Feb 3,      1
        Feb 2,  Mar 3,      2
        Feb 15, Mar 16,     3
        Mar 2,  Apr 3,      4
        Mar 15, Apr 16      5
        Mar 23, Apr 24      6
     */

    @Test
    public void script_MultiValued() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .script("doc['dates'].values")
                        .addUnboundedTo(date(2, 15))
                        .addRange(date(2, 15), date(3, 15))
                        .addUnboundedFrom(date(3, 15)))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateRange range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        DateRange.Bucket bucket = range.getByKey("*-2012-02-15T00:00:00.000Z");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsDate(), nullValue());
        assertThat(bucket.getTo(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getFrom(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getTo(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getDocCount(), equalTo(3l));

        bucket = range.getByKey("2012-03-15T00:00:00.000Z-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(bucket.getFrom(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsDate(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(4l));
    }

    @Test
    public void script_MultiValued_WithAggregatorInherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateRange("range")
                        .script("doc['dates'].values")
                        .addUnboundedTo(date(2, 15))
                        .addRange(date(2, 15), date(3, 15))
                        .addUnboundedFrom(date(3, 15))
                        .subAggregation(min("min")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateRange range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        DateRange.Bucket bucket = range.getByKey("*-2012-02-15T00:00:00.000Z");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsDate(), nullValue());
        assertThat(bucket.getTo(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getDocCount(), equalTo(2l));
        Min min = bucket.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getValue(), equalTo((double) date(1, 2).getMillis()));

        bucket = range.getByKey("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getFrom(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getTo(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getDocCount(), equalTo(3l));
        min = bucket.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getValue(), equalTo((double) date(2, 15).getMillis()));

        bucket = range.getByKey("2012-03-15T00:00:00.000Z-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(bucket.getFrom(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsDate(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(4l));
        min = bucket.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getValue(), equalTo((double) date(3, 15).getMillis()));
    }

    @Test
    public void unmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped")
                .addAggregation(dateRange("range")
                        .field("date")
                        .addUnboundedTo(date(2, 15))
                        .addRange(date(2, 15), date(3, 15))
                        .addUnboundedFrom(date(3, 15)))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateRange range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        DateRange.Bucket bucket = range.getByKey("*-2012-02-15T00:00:00.000Z");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsDate(), nullValue());
        assertThat(bucket.getTo(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getDocCount(), equalTo(0l));

        bucket = range.getByKey("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getFrom(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getTo(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getDocCount(), equalTo(0l));

        bucket = range.getByKey("2012-03-15T00:00:00.000Z-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(bucket.getFrom(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsDate(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(0l));
    }

    @Test
    public void unmapped_WithStringDates() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped")
                .addAggregation(dateRange("range")
                        .field("date")
                        .addUnboundedTo("2012-02-15")
                        .addRange("2012-02-15", "2012-03-15")
                        .addUnboundedFrom("2012-03-15"))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateRange range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        DateRange.Bucket bucket = range.getByKey("*-2012-02-15T00:00:00.000Z");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsDate(), nullValue());
        assertThat(bucket.getTo(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getDocCount(), equalTo(0l));

        bucket = range.getByKey("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getFrom(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getTo(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getDocCount(), equalTo(0l));

        bucket = range.getByKey("2012-03-15T00:00:00.000Z-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(bucket.getFrom(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsDate(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(0l));
    }

    @Test
    public void partiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
                .addAggregation(dateRange("range")
                        .field("date")
                        .addUnboundedTo(date(2, 15))
                        .addRange(date(2, 15), date(3, 15))
                        .addUnboundedFrom(date(3, 15)))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateRange range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        DateRange.Bucket bucket = range.getByKey("*-2012-02-15T00:00:00.000Z");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-2012-02-15T00:00:00.000Z"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsDate(), nullValue());
        assertThat(bucket.getTo(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-02-15T00:00:00.000Z-2012-03-15T00:00:00.000Z"));
        assertThat(bucket.getFrom(), equalTo((double) date(2, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(2, 15)));
        assertThat(bucket.getTo(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getToAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getDocCount(), equalTo(2l));

        bucket = range.getByKey("2012-03-15T00:00:00.000Z-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("2012-03-15T00:00:00.000Z-*"));
        assertThat(bucket.getFrom(), equalTo((double) date(3, 15).getMillis()));
        assertThat(bucket.getFromAsDate(), equalTo(date(3, 15)));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsDate(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(2l));
    }


}
