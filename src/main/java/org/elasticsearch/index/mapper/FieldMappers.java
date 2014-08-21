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

import org.elasticsearch.common.collect.CopyOnWriteHashSet;

import java.util.Iterator;
import java.util.Set;

/**
 * A holder for several {@link FieldMapper}.
 */
public final class FieldMappers implements Iterable<FieldMapper<?>> {

    private final CopyOnWriteHashSet<FieldMapper<?>> fieldMappers;

    public FieldMappers() {
        this(new CopyOnWriteHashSet<FieldMapper<?>>());
    }

    public FieldMappers(FieldMapper<?> fieldMapper) {
        this(new CopyOnWriteHashSet<FieldMapper<?>>().add(fieldMapper));
    }

    private FieldMappers(CopyOnWriteHashSet<FieldMapper<?>> fieldMappers) {
        this.fieldMappers = fieldMappers;
    }

    /**
     * Return any of the wrapped {@link FieldMapper}s.
     */
    public FieldMapper<?> mapper() {
        final Iterator<FieldMapper<?>> it = fieldMappers.iterator();
        if (it.hasNext()) {
            return it.next();
        } else {
            return null;
        }
    }

    public boolean isEmpty() {
        return fieldMappers.isEmpty();
    }

    public Set<FieldMapper<?>> mappers() {
        return fieldMappers.asSet();
    }

    @Override
    public Iterator<FieldMapper<?>> iterator() {
        return mappers().iterator();
    }

    /**
     * Concats and returns a new {@link FieldMappers}.
     */
    public FieldMappers concat(FieldMapper<?> mapper) {
        return new FieldMappers(fieldMappers.add(mapper));
    }

    public FieldMappers remove(FieldMapper<?> mapper) {
        return new FieldMappers(fieldMappers.remove(mapper));
    }
}
