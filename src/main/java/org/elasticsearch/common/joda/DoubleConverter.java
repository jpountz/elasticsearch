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

package org.elasticsearch.common.joda;

import org.joda.time.Chronology;
import org.joda.time.convert.AbstractConverter;
import org.joda.time.convert.ConverterManager;
import org.joda.time.convert.InstantConverter;

/**
 * An instant converter for {@link Double} values.
 */
public class DoubleConverter extends AbstractConverter implements InstantConverter {

    public static final DoubleConverter INSTANCE = new DoubleConverter();

    public static void register() {
        ConverterManager converterManager = ConverterManager.getInstance();
        for (InstantConverter converter : converterManager.getInstantConverters()) {
            if (converter.getClass().equals(Double.class)) {
                // there already is a converter for doubles
                return;
            }
        }
        final InstantConverter removed = converterManager.addInstantConverter(INSTANCE);
        assert removed == null;
    }

    public Class<?> getSupportedType() {
        return Double.class;
    }

    public long getInstantMillis(Object object, Chronology chrono) {
        return ((Double) object).longValue();
    }

    public long getDurationMillis(Object object) {
        return ((Double) object).longValue();
    }

}
