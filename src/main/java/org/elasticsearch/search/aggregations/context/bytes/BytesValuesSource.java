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

package org.elasticsearch.search.aggregations.context.bytes;

import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.context.FieldDataSource;
import org.elasticsearch.search.aggregations.context.ValuesSource;

/**
 *
 */
public interface BytesValuesSource extends ValuesSource {

    public static class FieldData extends ValuesSource.FieldData<FieldDataSource> implements BytesValuesSource {

        public FieldData(FieldDataSource source) {
            super(source);
        }

    }

    public class Script extends ValuesSource.Script implements BytesValuesSource {

        private final ScriptBytesValues values;

        public Script(SearchScript script, boolean multiValue) {
            super(script);
            this.values = new ScriptBytesValues(script, multiValue);
        }

        @Override
        public BytesValues bytesValues() {
            return values;
        }
    }
}
