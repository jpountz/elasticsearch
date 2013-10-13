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

package org.elasticsearch.search.aggregations.calc.numeric;

import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.calc.CalcAggregation;
import org.elasticsearch.search.aggregations.context.numeric.ValueFormatter;

/**
 *
 */
public abstract class NumericAggregation extends InternalAggregation implements CalcAggregation {

    protected ValueFormatter valueFormatter;

    public static abstract class SingleValue extends NumericAggregation implements CalcAggregation.SingleValue {

        protected SingleValue() {}

        protected SingleValue(String name) {
            super(name);
        }

        public abstract double value();
    }

    public static abstract class MultiValue extends NumericAggregation implements CalcAggregation.MultiValue {

        protected MultiValue() {}

        protected MultiValue(String name) {
            super(name);
        }

        public abstract double value(String name);

    }

    protected NumericAggregation() {} // for serialization

    protected NumericAggregation(String name) {
        super(name);
    }

    public abstract void collect(int doc, double value);

    public static interface Factory<S extends NumericAggregation> {

        S create(String name);

        S createUnmapped(String name);

    }

}
