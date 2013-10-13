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

package org.elasticsearch.search.aggregations;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.search.aggregations.calc.CalcAggregation;

/**
 *
 */
public interface Aggregated {

    Aggregations getAggregations();

    static class Comparator<A extends Aggregated> implements java.util.Comparator<A> {

        private final String aggName;
        private final String valueName;
        private final boolean asc;

        public Comparator(String expression, boolean asc) {
            this.asc = asc;
            int i = expression.indexOf('.');
            if (i < 0) {
                this.aggName = expression;
                this.valueName = null;
            } else {
                this.aggName = expression.substring(0, i);
                this.valueName = expression.substring(i+1);
            }
        }

        public Comparator(String aggName, String valueName, boolean asc) {
            this.aggName = aggName;
            this.valueName = valueName;
            this.asc = asc;
        }

        public boolean asc() {
            return asc;
        }

        public String aggName() {
            return aggName;
        }

        public String valueName() {
            return valueName;
        }

        @Override
        public int compare(A a1, A a2) {
            double v1 = value(a1);
            double v2 = value(a2);
            if (v1 > v2) {
                return asc ? 1 : -1;
            } else if (v1 < v2) {
                return asc ? -1 : 1;
            }
            return 0;
        }

        private double value(A aggregated) {
            CalcAggregation aggregation = aggregated.getAggregations().get(aggName);
            if (aggregation == null) {
                throw new ElasticSearchIllegalArgumentException("Unknown aggregation named [" + aggName + "]");
            }
            if (aggregation instanceof CalcAggregation.SingleValue) {
                //TODO should we throw an exception if the value name is specified?
                return ((CalcAggregation.SingleValue) aggregation).value();
            }
            if (aggregation instanceof CalcAggregation.MultiValue) {
                if (valueName == null) {
                    throw new ElasticSearchIllegalArgumentException("Cannot sort on multi valued aggregation [" + aggName + "]. A value name is required");
                }
                return ((CalcAggregation.MultiValue) aggregation).value(valueName);
            }

            throw new ElasticSearchIllegalArgumentException("A mal attempt to sort terms by aggregation [" + aggregation.getName() +
                    "]. Terms can only be ordered by either standard order or direct calc aggregators of the terms");
        }
    }

}
