/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.cluster.routing;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.routing.operation.hash.djb.DjbHashFunction;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@ElasticsearchIntegrationTest.SuiteScopeTest
@TestLogging("_root:DEBUG")
public class BadRouteTests extends ElasticsearchIntegrationTest {
    
    private static int shard(String routing, int numShards) {
        final int hash = DjbHashFunction.DJB_HASH(routing);
        return Math.abs(hash % numShards);
    }

    static int badRoutes = 0;

    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        ensureGreen();
        NumShards numShards = getNumShards("idx");
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            final String id = Integer.toString(i);
            String routing = null;
            if (randomInt(5) == 0) {
                routing = RandomStrings.randomAsciiOfLength(getRandom(), 5);
                if (shard(routing, numShards.numPrimaries) != shard(id, numShards.numPrimaries)) {
                    badRoutes += 1;
                }
            }
            builders.add(client().prepareIndex("idx", "type", id).setRouting(routing).setSource("{}"));
        }

        indexRandom(true, builders);
    }

    public void testFindBadShards() {
        NumShards numShards = getNumShards("idx");
        int found = 0;
        for (int shard = 0; shard < numShards.numPrimaries; ++shard) {
            String routing = null;
            do {
                routing = RandomStrings.randomAsciiOfLength(getRandom(), 5);
            } while (shard(routing, numShards.numPrimaries) != shard);
            final String script = "readerField =  Class.forName('org.elasticsearch.search.lookup.IndexLookup').getDeclaredField('reader'); readerField.setAccessible(true); reader = readerField.get(_index); document = reader.document(_index.docId); routing = null/*document.get('_routing')*/; if (routing == null) { routing = document.get('_parent'); } if (routing == null) { uid = document.get('_uid'); routing = uid.substring(1 + uid.indexOf('#')); }; hash = org.elasticsearch.cluster.routing.operation.hash.djb.DjbHashFunction.DJB_HASH(routing); shard = Math.abs(hash % number_of_shards); return shard != expected_shard;";
            SearchResponse response = client().prepareSearch("idx").setRouting(routing).setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), FilterBuilders.scriptFilter(script).addParam("expected_shard", shard).addParam("number_of_shards", numShards.numPrimaries))).execute().actionGet();
            ElasticsearchAssertions.assertSearchResponse(response);
            found += response.getHits().totalHits();System.out.println("Shard " + shard);
            System.out.println(response);
        }
        assertEquals(badRoutes, found);System.out.println(found);
    }

    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(Class.forName("org.elasticsearch.search.lookup.IndexLookup").getMethods()));
        Field field = Class.forName("org.elasticsearch.search.lookup.IndexLookup").getDeclaredField("reader");
        field.get(null);
    }
    
}
