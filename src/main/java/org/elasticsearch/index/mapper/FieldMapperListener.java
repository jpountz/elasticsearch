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

package org.elasticsearch.index.mapper;

import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.List;

/**
 *
 */
public abstract class FieldMapperListener {

    /**
     * Get the list of field mappers that are under <code>mapper</code>.
     */
    public static Collection<FieldMapper<?>> getFieldMappers(Mapper mapper) {
        final ImmutableList.Builder<FieldMapper<?>> mappers = ImmutableList.builder();
        mapper.traverse(new FieldMapperListener() {
            @Override
            public void fieldMapper(FieldMapper<?> fieldMapper) {
                mappers.add(fieldMapper);
            }
        });
        return mappers.build();
    }

    /**
     * Callback for when a field mapper is traversed.
     */
    public abstract void fieldMapper(FieldMapper<?> fieldMapper);

    public final void fieldMappers(List<FieldMapper<?>>  fieldMappers) {
        for (FieldMapper<?> mapper : fieldMappers) {
            fieldMapper(mapper);
        }
    }
}
