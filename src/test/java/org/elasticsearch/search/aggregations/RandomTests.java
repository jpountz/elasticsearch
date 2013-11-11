/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.aggregations;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.bucket.multi.range.Range;
import org.elasticsearch.search.aggregations.bucket.multi.range.Range.Bucket;
import org.elasticsearch.search.aggregations.bucket.multi.range.RangeBuilder;
import org.elasticsearch.search.aggregations.bucket.multi.terms.Terms;
import org.elasticsearch.test.AbstractIntegrationTest;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

/** Additional tests that aim at testing more complex aggregation trees on larger random datasets, so that things like the growth of dynamic arrays is tested. */
public class RandomTests extends AbstractIntegrationTest {

    @Override
    public Settings getSettings() {
        return randomSettingsBuilder()
                .put("index.number_of_shards", between(1, 5))
                .put("index.number_of_replicas", between(0, 1))
                .build();
    }

    // Make sure that unordered, reversed, disjoint and/or overlapping ranges are supported
    public void testRandomRanges() throws Exception {
        final int numDocs = atLeast(1000);
        final double[][] docs = new double[numDocs][];
        for (int i = 0; i < numDocs; ++i) {
            final int numValues = randomInt(5);
            docs[i] = new double[numValues];
            for (int j = 0; j < numValues; ++j) {
                docs[i][j] = randomDouble() * 100;
            }
        }

        createIndex("idx");
        for (int i = 0; i < docs.length; ++i) {
            XContentBuilder source = jsonBuilder()
                    .startObject()
                    .startArray("values");
            for (int j = 0; j < docs[i].length; ++j) {
                source = source.value(docs[i][j]);
            }
            source = source.endArray().endObject();
            client().prepareIndex("idx", "type").setSource(source).execute().actionGet();
        }
        assertNoFailures(client().admin().indices().prepareRefresh("idx").setIgnoreIndices(IgnoreIndices.MISSING).execute().get());

        final int numRanges = randomIntBetween(1, 20);
        final double[][] ranges = new double[numRanges][];
        for (int i = 0; i < ranges.length; ++i) {
            switch (randomInt(2)) {
            case 0:
                ranges[i] = new double[] { Double.NEGATIVE_INFINITY, randomInt(100) };
                break;
            case 1:
                ranges[i] = new double[] { randomInt(100), Double.POSITIVE_INFINITY };
                break;
            case 2:
                ranges[i] = new double[] { randomInt(100), randomInt(100) };
                break;
            default:
                throw new AssertionError();
            }
        }

        RangeBuilder query = range("range").field("values");
        for (int i = 0; i < ranges.length; ++i) {
            String key = Integer.toString(i);
            if (ranges[i][0] == Double.NEGATIVE_INFINITY) {
                query.addUnboundedTo(key, ranges[i][1]);
            } else if (ranges[i][1] == Double.POSITIVE_INFINITY) {
                query.addUnboundedFrom(key, ranges[i][0]);
            } else {
                query.addRange(key, ranges[i][0], ranges[i][1]);
            }
        }

        SearchResponse resp = client().prepareSearch("idx")
                .addAggregation(query).execute().actionGet();
        Range range = resp.getAggregations().get("range");

        for (int i = 0; i < ranges.length; ++i) {

            long count = 0;
            for (double[] values : docs) {
                for (double value : values) {
                    if (value >= ranges[i][0] && value < ranges[i][1]) {
                        ++count;
                        break;
                    }
                }
            }

            final Bucket bucket = range.getByKey(Integer.toString(i));
            assertEquals(bucket.getKey(), count, bucket.getDocCount());
        }
    }

    // test long/double/string terms aggs with high number of buckets that require array growth
    public void testDuellTerms() throws Exception {
        final int numDocs = atLeast(1000);
        final int numTerms = randomIntBetween(10, 10000);

        createIndex("idx");
        for (int i = 0; i < numDocs; ++i) {
            final int[] values = new int[randomInt(4)];
            for (int j = 0; j < values.length; ++j) {
                values[j] = randomInt(numTerms - 1);
            }
            XContentBuilder source = jsonBuilder()
                    .startObject()
                    .field("num", randomDouble())
                    .startArray("long_values");
            for (int j = 0; j < values.length; ++j) {
                source = source.value(values[j]);
            }
            source = source.endArray().startArray("double_values");
            for (int j = 0; j < values.length; ++j) {
                source = source.value((double) values[j]);
            }
            source = source.endArray().startArray("string_values");
            for (int j = 0; j < values.length; ++j) {
                source = source.value(Integer.toString(values[j]));
            }
            source = source.endArray().endObject();
            client().prepareIndex("idx", "type").setSource(source).execute().actionGet();
        }
        assertNoFailures(client().admin().indices().prepareRefresh("idx").setIgnoreIndices(IgnoreIndices.MISSING).execute().get());

        SearchResponse resp = client().prepareSearch("idx")
                .addAggregation(terms("long").field("long_values").size(numTerms).subAggregation(min("min").field("num")))
                .addAggregation(terms("double").field("double_values").size(numTerms).subAggregation(max("max").field("num")))
                .addAggregation(terms("string").field("string_values").size(numTerms).subAggregation(stats("stats").field("num"))).execute().actionGet();

        final Terms longTerms = resp.getAggregations().get("long");
        final Terms doubleTerms = resp.getAggregations().get("double");
        final Terms stringTerms = resp.getAggregations().get("string");

        assertEquals(longTerms.buckets().size(), doubleTerms.buckets().size());
        assertEquals(longTerms.buckets().size(), stringTerms.buckets().size());
        for (Terms.Bucket bucket : longTerms.buckets()) {
            final Terms.Bucket doubleBucket = doubleTerms.getByTerm(Double.toString(Long.parseLong(bucket.getTerm().string())));
            final Terms.Bucket stringBucket = stringTerms.getByTerm(bucket.getTerm().string());
            assertNotNull(doubleBucket);
            assertNotNull(stringBucket);
            assertEquals(bucket.getDocCount(), doubleBucket.getDocCount());
            assertEquals(bucket.getDocCount(), stringBucket.getDocCount());
        }
    }

}
