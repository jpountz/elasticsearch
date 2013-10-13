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

package org.elasticsearch.search.aggregations.context;

import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.script.SearchScript;

/**
 * An abstraction of a source from which values are resolved per document. There are two main value sources that are supported -
 * {@link FieldData field data} and {@link Script script}.
 */
public interface ValuesSource {

    /**
     * @return A key that uniquely identifies this value source.
     */
    Object key();

    /**
     * @return A {@link org.apache.lucene.util.BytesRef bytesref} view over the values that are resolved from this value source.
     */
    BytesValues bytesValues();

    /**
     * A base class for all field data based value sources. This value source is based on {@link FieldDataSource} from which field values
     * can be retrieved for every document.
     *
     * @param <FDS> The field data source this value source is based on.
     */
    public abstract static class FieldData<FDS extends FieldDataSource> implements ValuesSource {

        protected final FDS source;

        public FieldData(FDS source) {
            this.source = source;
        }

        @Override
        public BytesValues bytesValues() {
            return source.bytesValues();
        }

        /**
         * In the case of the field data, the unique key representing this value source is the field name.
         */
        @Override
        public Object key() {
            return source.field();
        }

    }

    /**
     * A base class for all values sources that resolve the values based on a dynamic script.
     */
    public abstract static class Script implements ValuesSource {

        protected final SearchScript script;

        public Script(SearchScript script) {
            this.script = script;
        }

        /**
         * In this case, the script itself serves as the unique key identifying the value source.
         */
        @Override
        public Object key() {
            return script;
        }

    }
}
