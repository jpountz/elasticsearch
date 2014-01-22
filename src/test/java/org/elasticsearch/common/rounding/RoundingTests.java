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

package org.elasticsearch.common.rounding;

import org.elasticsearch.test.ElasticsearchTestCase;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;


public class RoundingTests extends ElasticsearchTestCase {

    private static final double ACCEPTABLE_ERROR = 0.001;

    public void testInterval() {
        final double interval = randomDouble() * 100;
        Rounding.Interval rounding = new Rounding.Interval(interval);
        for (int i = 0; i < 10000000; ++i) {
            double d = (randomDouble() * 2 - 1) * Math.pow(2, 43);
            double r = rounding.round(d);
            String message = "round(" + d + ", interval=" + interval + ") = " + r;
            assertEquals(message, 0, r - Math.round(r / interval) * interval, ACCEPTABLE_ERROR);
            assertThat(message, r, lessThanOrEqualTo(d + ACCEPTABLE_ERROR));
            assertThat(message, r + interval, greaterThan(d - ACCEPTABLE_ERROR));
        }
    }

}
