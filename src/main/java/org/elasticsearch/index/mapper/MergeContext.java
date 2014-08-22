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

import com.google.common.collect.Lists;
import org.elasticsearch.index.mapper.object.ObjectMapper;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class MergeContext {

    private final List<String> mergeConflicts = Lists.newArrayList();
    private final List<FieldMapper<?>> newFieldMappers = Lists.newArrayList();
    private final List<FieldMapper<?>> removedFieldMappers = Lists.newArrayList();
    private final List<ObjectMapper> newObjectMappers = Lists.newArrayList();
    private final List<ObjectMapper> removedObjectMappers = Lists.newArrayList();

    public void addConflict(String mergeFailure) {
        mergeConflicts.add(mergeFailure);
    }

    public boolean hasConflicts() {
        return !mergeConflicts.isEmpty();
    }

    public String[] buildConflicts() {
        return mergeConflicts.toArray(new String[mergeConflicts.size()]);
    }

    public Collection<FieldMapper<?>> newFieldMappers() {
        return Collections.unmodifiableList(newFieldMappers);
    }

    public void newFieldMapper(FieldMapper<?> fieldMapper) {
        newFieldMappers.add(fieldMapper);
    }

    public Collection<FieldMapper<?>> removedFieldMappers() {
        return Collections.unmodifiableList(removedFieldMappers);
    }

    public void removedFieldMapper(FieldMapper<?> fieldMapper) {
        removedFieldMappers.add(fieldMapper);
    }

    public Collection<ObjectMapper> newObjectMappers() {
        return Collections.unmodifiableList(newObjectMappers);
    }

    public void newObjectMapper(ObjectMapper objectMapper) {
        newObjectMappers.add(objectMapper);
    }

    public Collection<ObjectMapper> removedObjectMappers() {
        return Collections.unmodifiableList(removedObjectMappers);
    }

    public void removedObjectMapper(ObjectMapper objectMapper) {
        removedObjectMappers.add(objectMapper);
    }
}
