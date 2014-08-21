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

import com.google.common.collect.Sets;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.CopyOnWriteHashMap;
import org.elasticsearch.common.collect.CopyOnWriteHashSet;
import org.elasticsearch.common.regex.Regex;

import java.util.Iterator;
import java.util.Set;

/**
 * A class that holds a map of field mappers from name, index name, and full name.
 */
public final class FieldMappersLookup implements Iterable<FieldMapper<?>> {

    private final CopyOnWriteHashSet<FieldMapper<?>> mappers;
    private final CopyOnWriteHashMap<String, FieldMappers> name;
    private final CopyOnWriteHashMap<String, FieldMappers> indexName;
    private final CopyOnWriteHashMap<String, FieldMappers> fullName;

    public FieldMappersLookup() {
        this(new CopyOnWriteHashSet<FieldMapper<?>>(), new CopyOnWriteHashMap<String, FieldMappers>(),
                new CopyOnWriteHashMap<String, FieldMappers>(), new CopyOnWriteHashMap<String, FieldMappers>());
    }

    private FieldMappersLookup(CopyOnWriteHashSet<FieldMapper<?>> mappers, CopyOnWriteHashMap<String, FieldMappers> name,
            CopyOnWriteHashMap<String, FieldMappers> indexName, CopyOnWriteHashMap<String, FieldMappers> fullName) {
        this.mappers = mappers;
        this.name = name;
        this.indexName = indexName;
        this.fullName = fullName;
    }

    private static CopyOnWriteHashMap<String, FieldMappers> add(CopyOnWriteHashMap<String, FieldMappers> map, String name, FieldMapper<?> mapper) {
        final FieldMappers mappers = map.get(name);
        final FieldMappers newMappers;
        if (mappers == null) {
            newMappers = new FieldMappers(mapper);
        } else {
            newMappers = mappers.concat(mapper);
        }
        return map.put(name, newMappers);
    }

    private static CopyOnWriteHashMap<String, FieldMappers> remove(CopyOnWriteHashMap<String, FieldMappers> map, String name, FieldMapper<?> mapper) {
        final FieldMappers mappers = map.get(name);
        if (mappers != null) {
            final FieldMappers newMappers = mappers.remove(mapper);
            if (newMappers.isEmpty()) {
                return map.remove(name);
            } else {
                return map.put(name, newMappers);
            }
        } else {
            return map;
        }
    }

    /**
     * Adds a new set of mappers.
     */
    public FieldMappersLookup addNewMappers(Iterable<FieldMapper<?>> newMappers) {
        CopyOnWriteHashSet<FieldMapper<?>> mappers = this.mappers;
        CopyOnWriteHashMap<String, FieldMappers> name = this.name;
        CopyOnWriteHashMap<String, FieldMappers> indexName = this.indexName;
        CopyOnWriteHashMap<String, FieldMappers> fullName = this.fullName;

        for (FieldMapper<?> mapper : newMappers) {
            mappers = mappers.add(mapper);
            name = add(name, mapper.names().name(), mapper);
            indexName = add(indexName, mapper.names().indexName(), mapper);
            fullName = add(fullName, mapper.names().fullName(), mapper);
        }

        return new FieldMappersLookup(mappers, name, indexName, fullName);
    }

    /**
     * Removes the set of mappers.
     */
    public FieldMappersLookup removeMappers(Iterable<FieldMapper<?>> mappersToRemove) {
        CopyOnWriteHashSet<FieldMapper<?>> mappers = this.mappers;
        CopyOnWriteHashMap<String, FieldMappers> name = this.name;
        CopyOnWriteHashMap<String, FieldMappers> indexName = this.indexName;
        CopyOnWriteHashMap<String, FieldMappers> fullName = this.fullName;

        for (FieldMapper<?> mapper : mappersToRemove) {
            mappers = mappers.remove(mapper);
            name = remove(name, mapper.names().name(), mapper);
            indexName = remove(indexName, mapper.names().indexName(), mapper);
            fullName = remove(fullName, mapper.names().fullName(), mapper);
        }

        return new FieldMappersLookup(mappers, name, indexName, fullName);
    }

    @Override
    public Iterator<FieldMapper<?>> iterator() {
        return mappers.iterator();
    }

    /**
     * The list of all mappers.
     */
    public Set<FieldMapper<?>> mappers() {
        return mappers.asSet();
    }

    /**
     * Returns the field mappers based on the mapper name.
     */
    public FieldMappers name(String name) {
        return this.name.get(name);
    }

    /**
     * Returns the field mappers based on the mapper index name.
     */
    public FieldMappers indexName(String indexName) {
        return this.indexName.get(indexName);
    }

    /**
     * Returns the field mappers based on the mapper full name.
     */
    public FieldMappers fullName(String fullName) {
        return this.fullName.get(fullName);
    }

    /**
     * Returns a set of the index names of a simple match regex like pattern against full name, name and index name.
     */
    public Set<String> simpleMatchToIndexNames(String pattern) {
        Set<String> fields = Sets.newHashSet();
        for (FieldMapper<?> fieldMapper : mappers) {
            if (Regex.simpleMatch(pattern, fieldMapper.names().fullName())) {
                fields.add(fieldMapper.names().indexName());
            } else if (Regex.simpleMatch(pattern, fieldMapper.names().indexName())) {
                fields.add(fieldMapper.names().indexName());
            } else if (Regex.simpleMatch(pattern, fieldMapper.names().name())) {
                fields.add(fieldMapper.names().indexName());
            }
        }
        return fields;
    }

    /**
     * Returns a set of the full names of a simple match regex like pattern against full name, name and index name.
     */
    public Set<String> simpleMatchToFullName(String pattern) {
        Set<String> fields = Sets.newHashSet();
        for (FieldMapper<?> fieldMapper : mappers) {
            if (Regex.simpleMatch(pattern, fieldMapper.names().fullName())) {
                fields.add(fieldMapper.names().fullName());
            } else if (Regex.simpleMatch(pattern, fieldMapper.names().indexName())) {
                fields.add(fieldMapper.names().fullName());
            } else if (Regex.simpleMatch(pattern, fieldMapper.names().name())) {
                fields.add(fieldMapper.names().fullName());
            }
        }
        return fields;
    }

    /**
     * Tries to find first based on {@link #fullName(String)}, then by {@link #indexName(String)}, and last
     * by {@link #name(String)}.
     */
    @Nullable
    public FieldMappers smartName(String name) {
        FieldMappers fieldMappers = fullName(name);
        if (fieldMappers != null) {
            return fieldMappers;
        }
        fieldMappers = indexName(name);
        if (fieldMappers != null) {
            return fieldMappers;
        }
        return name(name);
    }

    /**
     * Tries to find first based on {@link #fullName(String)}, then by {@link #indexName(String)}, and last
     * by {@link #name(String)} and return the first mapper for it (see {@link org.elasticsearch.index.mapper.FieldMappers#mapper()}).
     */
    @Nullable
    public FieldMapper<?> smartNameFieldMapper(String name) {
        FieldMappers fieldMappers = smartName(name);
        if (fieldMappers == null) {
            return null;
        }
        return fieldMappers.mapper();
    }
}
