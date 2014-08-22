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
import org.elasticsearch.index.mapper.object.ObjectMapper;

import java.util.Collection;

/**
 *
 */
public abstract class ObjectMapperListener {

    /**
     * Get the list of object mappers that are under <code>mapper</code>.
     */
    public static Collection<ObjectMapper> getObjectMappers(Mapper mapper) {
        final ImmutableList.Builder<ObjectMapper> mappers = ImmutableList.builder();
        mapper.traverse(new ObjectMapperListener() {
            @Override
            public void objectMapper(ObjectMapper objectMapper) {
                mappers.add(objectMapper);
            }
        });
        return mappers.build();
    }

    /**
     * Callback for when an object mapper is traversed.
     */
    public abstract void objectMapper(ObjectMapper objectMapper);

    public final void objectMappers(ObjectMapper... objectMappers) {
        for (ObjectMapper objectMapper : objectMappers) {
            objectMapper(objectMapper);
        }
    }
}
