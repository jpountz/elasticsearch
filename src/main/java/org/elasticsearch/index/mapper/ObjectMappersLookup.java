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

import org.elasticsearch.common.collect.CopyOnWriteHashMap;
import org.elasticsearch.index.mapper.object.ObjectMapper;

/**
 * A class that holds a map of object mappers from their full path.
 */
public final class ObjectMappersLookup {

    private final CopyOnWriteHashMap<String, ObjectMappers> fullPath;

    public ObjectMappersLookup() {
        this(new CopyOnWriteHashMap<String, ObjectMappers>());
    }

    private ObjectMappersLookup(CopyOnWriteHashMap<String, ObjectMappers> fullPath) {
        this.fullPath = fullPath;
    }

    /**
     * Adds a new set of object mappers.
     */
    public ObjectMappersLookup addNewMappers(Iterable<ObjectMapper> newMappers) {
        CopyOnWriteHashMap<String, ObjectMappers> fullPath = this.fullPath;

        for (ObjectMapper mapper : newMappers) {
            final String key = mapper.fullPath();
            ObjectMappers mappers = fullPath.get(key);
            if (mappers == null) {
                mappers = new ObjectMappers(mapper);
            } else {
                mappers = mappers.concat(mapper);
            }
            fullPath = fullPath.put(key, mappers);
        }

        return new ObjectMappersLookup(fullPath);
    }

    /**
     * Removes the set of mappers.
     */
    public ObjectMappersLookup removeMappers(Iterable<ObjectMapper> mappersToRemove) {
        CopyOnWriteHashMap<String, ObjectMappers> fullPath = this.fullPath;

        for (ObjectMapper mapper : mappersToRemove) {
            final String key = mapper.fullPath();
            ObjectMappers mappers = fullPath.get(key);
            if (mappers == null) {
                continue;
            }
            mappers = mappers.remove(mapper);
            if (mappers.isEmpty()) {
                fullPath = fullPath.remove(key);
            } else {
                fullPath = fullPath.put(key, mappers);
            }
        }

        return new ObjectMappersLookup(fullPath);
    }

}
