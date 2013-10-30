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

package org.elasticsearch.search.aggregations.context.numeric;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.context.FieldDataSource;
import org.elasticsearch.search.aggregations.context.ScriptValueType;
import org.elasticsearch.search.aggregations.context.ValuesSource;
import org.elasticsearch.search.aggregations.context.bytes.ScriptBytesValues;

/**
 *
 */
public interface NumericValuesSource extends ValuesSource {

    boolean isFloatingPoint();

    LongValues longValues();

    DoubleValues doubleValues();

    ValueFormatter formatter();

    ValueParser parser();

    public static class FieldData extends ValuesSource.FieldData<FieldDataSource.Numeric> implements NumericValuesSource {
        private final ValueFormatter formatter;
        private final ValueParser parser;

        public FieldData(FieldDataSource.Numeric source, @Nullable ValueFormatter formatter, @Nullable ValueParser parser) {
            super(source);
            this.formatter = formatter;
            this.parser = parser;
        }

        @Override
        public LongValues longValues() {
            return source.longValues();
        }

        @Override
        public DoubleValues doubleValues() {
            return source.doubleValues();
        }

        @Override
        public boolean isFloatingPoint() {
            return source.isFloatingPoint();
        }

        @Override
        public ValueFormatter formatter() {
            return formatter;
        }

        @Override
        public ValueParser parser() {
            return parser;
        }
    }

    public static class Script extends ValuesSource.Script implements NumericValuesSource {

        private final ValueFormatter formatter;
        private final ValueParser parser;
        private final ScriptValueType scriptValueType;

        private final ScriptDoubleValues doubleValues;
        private final ScriptLongValues longValues;
        private final ScriptBytesValues bytesValues;

        public Script(SearchScript script, ScriptValueType scriptValueType, @Nullable ValueFormatter formatter, @Nullable ValueParser parser) {
            super(script);
            this.formatter = formatter;
            this.parser = parser;
            this.scriptValueType = scriptValueType;
            longValues = new ScriptLongValues(script);
            doubleValues = new ScriptDoubleValues(script);
            bytesValues = new ScriptBytesValues(script);
        }

        @Override
        public boolean isFloatingPoint() {
            return scriptValueType != null ? scriptValueType.isFloatingPoint() : true;
        }

        @Override
        public LongValues longValues() {
            return longValues;
        }

        @Override
        public DoubleValues doubleValues() {
            return doubleValues;
        }

        @Override
        public BytesValues bytesValues() {
            return bytesValues;
        }

        @Override
        public ValueFormatter formatter() {
            return formatter;
        }

        @Override
        public ValueParser parser() {
            return parser;
        }

    }

    /**
     * Wraps another numeric values source, and associates with it a different formatter and/or parser
     */
    static class Delegate implements  NumericValuesSource {

        private final NumericValuesSource valuesSource;
        private final ValueFormatter formatter;
        private final ValueParser parser;

        public Delegate(NumericValuesSource valuesSource, ValueFormatter formatter) {
            this(valuesSource, formatter, valuesSource.parser());
        }

        public Delegate(NumericValuesSource valuesSource, ValueParser parser) {
            this(valuesSource, valuesSource.formatter(), parser);
        }

        public Delegate(NumericValuesSource valuesSource, ValueFormatter formatter, ValueParser parser) {
            this.valuesSource = valuesSource;
            this.formatter = formatter;
            this.parser = parser;
        }

        @Override
        public boolean isFloatingPoint() {
            return valuesSource.isFloatingPoint();
        }

        @Override
        public LongValues longValues() {
            return valuesSource.longValues();
        }

        @Override
        public DoubleValues doubleValues() {
            return valuesSource.doubleValues();
        }

        @Override
        public BytesValues bytesValues() {
            return valuesSource.bytesValues();
        }

        @Override
        public ValueFormatter formatter() {
            return formatter;
        }

        @Override
        public ValueParser parser() {
            return parser;
        }

        @Override
        public Object key() {
            return valuesSource.key();
        }
    }

}
