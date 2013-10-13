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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractIntegrationTest;
import org.junit.Before;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 *
 */
public abstract class AbstractNumericTests extends AbstractIntegrationTest {
    
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
        createIndex("idx2");
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("idx", "type", ""+i).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i+1)
                    .startArray("values").value(i+2).value(i+3).endArray()
                    .endObject())
                    .execute().actionGet();
        }
        client().admin().indices().prepareFlush().execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();
    }

    public abstract void testUnmapped() throws Exception;

    public abstract void testSingleValuedField() throws Exception;

    public abstract void testSingleValuedField_PartiallyUnmapped() throws Exception;

    public abstract void testSingleValuedField_WithValueScript() throws Exception;

    public abstract void testSingleValuedField_WithValueScript_WithParams() throws Exception;

    public abstract void testMultiValuedField() throws Exception;

    public abstract void testMultiValuedField_WithValueScript() throws Exception;

    public abstract void testMultiValuedField_WithValueScript_WithParams() throws Exception;

    public abstract void testScript_SingleValued() throws Exception;

    public abstract void testScript_SingleValued_WithParams() throws Exception;

    public abstract void testScript_ExplicitSingleValued_WithParams() throws Exception;

    public abstract void testScript_MultiValued() throws Exception;

    public abstract void testScript_ExplicitMultiValued() throws Exception;

    public abstract void testScript_MultiValued_WithParams() throws Exception;


}