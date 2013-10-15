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
import org.elasticsearch.search.aggregations.bucket.multi.terms.Terms;
import org.elasticsearch.search.aggregations.calc.numeric.sum.Sum;
import org.elasticsearch.test.AbstractIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 *
 */
public class DoubleTermsTests extends AbstractIntegrationTest {

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
        
        IndexRequestBuilder[] lowcardBuilders = new IndexRequestBuilder[5]; // nocommit randomize the size
        for (int i = 0; i < lowcardBuilders.length; i++) {
            lowcardBuilders[i] = client().prepareIndex("idx", "type").setSource(jsonBuilder()
                    .startObject()
                    .field("value", (double) i)
                    .startArray("values").value((double)i).value(i + 1d).endArray()
                    .endObject());
                    
        }
        indexRandom(randomBoolean(), lowcardBuilders);
        IndexRequestBuilder[] highCardBuilders = new IndexRequestBuilder[100]; // nocommit randomize the size
        for (int i = 0; i < highCardBuilders.length; i++) {
            highCardBuilders[i] = client().prepareIndex("idx", "high_card_type").setSource(jsonBuilder()
                    .startObject()
                    .field("value", (double) i)
                    .startArray("values").value((double)i).value(i + 1d).endArray()
                    .endObject());
        }
        indexRandom(true, highCardBuilders);

        createIndex("idx_unmapped");

    }

    @Test
    public void singleValueField() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("value"))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("" + (double)i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("" + (double)i));
            assertThat(bucket.getTermAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }

    @Test
    public void singleValueField_WithMaxSize() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("high_card_type")
                .addAggregation(terms("terms")
                        .field("value")
                        .size(20)
                        .order(Terms.Order.TERM_ASC)) // we need to sort by terms cause we're checking the first 20 values
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(20));

        for (int i = 0; i < 20; i++) {
            Terms.Bucket bucket = terms.getByTerm("" + (double) i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("" + (double) i));
            assertThat(bucket.getTermAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }

    @Test
    public void singleValueField_OrderedByTermAsc() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("value")
                        .order(Terms.Order.TERM_ASC))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        int i = 0;
        for (Terms.Bucket bucket : terms.buckets()) {
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("" + (double)i));
            assertThat(bucket.getTermAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            i++;
        }
    }

    @Test
    public void singleValueField_OrderedByTermDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("value")
                        .order(Terms.Order.TERM_DESC))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        int i = 4;
        for (Terms.Bucket bucket : terms.buckets()) {
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("" + (double) i));
            assertThat(bucket.getTermAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            i--;
        }
    }

    @Test
    public void singleValuedField_WithSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("value")
                        .subAggregation(sum("sum").field("values")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("" + (double) i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("" + (double) i));
            assertThat(bucket.getTermAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            Sum sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            assertThat((long) sum.getValue(), equalTo(i+i+1l));
        }
    }

    @Test
    public void singleValuedField_WithSubAggregation_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("value")
                        .subAggregation(sum("sum")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("" + (double) i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("" + (double) i));
            assertThat(bucket.getTermAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            Sum sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            assertThat(sum.getValue(), equalTo((double) i));
        }
    }

    @Test
    public void singleValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("value")
                        .script("_value + 1"))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("" + (i+1d));
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("" + (i+1d)));
            assertThat(bucket.getTermAsNumber().intValue(), equalTo(i+1));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }

    @Test
    public void multiValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("values"))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getByTerm("" + (double) i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("" + (double) i));
            assertThat(bucket.getTermAsNumber().intValue(), equalTo(i));
            if (i == 0 || i == 5) {
                assertThat(bucket.getDocCount(), equalTo(1l));
            } else {
                assertThat(bucket.getDocCount(), equalTo(2l));
            }
        }
    }

    @Test
    public void multiValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("values")
                        .script("_value + 1"))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getByTerm("" + (i+1d));
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("" + (i+1d)));
            assertThat(bucket.getTermAsNumber().intValue(), equalTo(i+1));
            if (i == 0 || i == 5) {
                assertThat(bucket.getDocCount(), equalTo(1l));
            } else {
                assertThat(bucket.getDocCount(), equalTo(2l));
            }
        }
    }

    /*

    [1, 2]
    [2, 3]
    [3, 4]
    [4, 5]
    [5, 6]

    1 - count: 1 - sum: 1
    2 - count: 2 - sum: 4
    3 - count: 2 - sum: 6
    4 - count: 2 - sum: 8
    5 - count: 2 - sum: 10
    6 - count: 1 - sum: 6

    */

    @Test
    public void multiValuedField_WithValueScript_WithInheritedSubAggregator() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("values")
                        .script("_value + 1")
                        .subAggregation(sum("sum")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getByTerm("" + (i+1d));
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("" + (i+1d)));
            assertThat(bucket.getTermAsNumber().doubleValue(), equalTo(i+1d));
            if (i == 0 | i == 5) {
                assertThat(bucket.getDocCount(), equalTo(1l));
                Sum sum = bucket.getAggregations().get("sum");
                assertThat(sum, notNullValue());
                assertThat(sum.getValue(), equalTo((i+1d)));
            } else {
                assertThat(bucket.getDocCount(), equalTo(2l));
                Sum sum = bucket.getAggregations().get("sum");
                assertThat(sum, notNullValue());
                assertThat(sum.getValue(), equalTo((i+1d + i+1d)));
            }
        }
    }

    @Test
    public void script_SingleValue() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .script("doc['value'].value"))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("" + (double) i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("" + (double) i));
            assertThat(bucket.getTermAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }

    @Test
    public void script_SingleValue_WithSubAggregator_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("value")
                        .subAggregation(sum("sum")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("" + (double) i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("" + (double) i));
            assertThat(bucket.getTermAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            Sum sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            assertThat(sum.getValue(), equalTo((double) i));
        }
    }

    @Test
    public void script_MultiValued() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .script("doc['values'].values"))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getByTerm("" + (double) i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("" + (double) i));
            assertThat(bucket.getTermAsNumber().intValue(), equalTo(i));
            if (i == 0 || i == 5) {
                assertThat(bucket.getDocCount(), equalTo(1l));
            } else {
                assertThat(bucket.getDocCount(), equalTo(2l));
            }
        }
    }

    @Test
    public void script_MultiValued_WithAggregatorInherited_NoExplicitType() throws Exception {

        // since no type is explicitly defined, es will assume all values returned by the script to be strings (bytes),
        // so the aggregation should fail, since the "sum" aggregation can only operation on numeric values.

        try {

            SearchResponse response = client().prepareSearch("idx").setTypes("type")
                    .addAggregation(terms("terms")
                            .script("doc['values'].values")
                            .subAggregation(sum("sum")))
                    .execute().actionGet();


            fail("expected to fail as sub-aggregation sum requires a numeric value source context, but there is none");

        } catch (Exception e) {
            // expected
        }

    }

    @Test
    public void script_MultiValued_WithAggregatorInherited_WithExplicitType() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .script("doc['values'].values")
                        .valueType(Terms.ValueType.DOUBLE)
                        .subAggregation(sum("sum")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getByTerm("" + i + ".0");
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("" + i + ".0"));
            assertThat(bucket.getTermAsNumber().intValue(), equalTo(i));
            if (i == 0 | i == 5) {
                assertThat(bucket.getDocCount(), equalTo(1l));
                Sum sum = bucket.getAggregations().get("sum");
                assertThat(sum, notNullValue());
                assertThat(sum.getValue(), equalTo((double) i));
            } else {
                assertThat(bucket.getDocCount(), equalTo(2l));
                Sum sum = bucket.getAggregations().get("sum");
                assertThat(sum, notNullValue());
                assertThat(sum.getValue(), equalTo((double) (i + i)));
            }
        }
    }

    @Test
    public void unmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped").setTypes("type")
                .addAggregation(terms("terms")
                        .field("value"))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(0));
    }

    @Test
    public void partiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped", "idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("value"))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("" + (double) i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("" + (double) i));
            assertThat(bucket.getTermAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }

}
