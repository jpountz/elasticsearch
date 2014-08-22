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

/**
 * Index-level view of mappings: wraps several types and their associated
 * mapping.
 */
final class MappingsLookup {

    // maps types to their mapping
    public final CopyOnWriteHashMap<String, MappingLookup> mappings;

    // these lookup structures are index-wide
    public final FieldMappersLookup fieldMappers;
    public final ObjectMappersLookup objectMappers;

    public MappingsLookup() {
        this(new CopyOnWriteHashMap<String, MappingLookup>(), new FieldMappersLookup(), new ObjectMappersLookup());
    }

    private MappingsLookup(CopyOnWriteHashMap<String, MappingLookup> mappings,
            FieldMappersLookup fieldMappers, ObjectMappersLookup objectMappers) {
        this.mappings = mappings;
        this.fieldMappers = fieldMappers;
        this.objectMappers = objectMappers;
    }

    /**
     * Return the result of the merge of <code>mergeWith</code> into <code>this</code>.
     * The current mapping is not modified.
     */
    public MappingsLookup merge(String type, Mapping mergeWith) {
        final MappingLookup toMerge = mappings.get(type);
        CopyOnWriteHashMap<String, MappingLookup> mergedMappings;
        FieldMappersLookup mergedFieldMappers;
        ObjectMappersLookup mergedObjectMappers;

        if (toMerge == null) {
            mergedMappings = mappings.put(type, new MappingLookup(mergeWith));
            mergedFieldMappers = fieldMappers
                    .addNewMappers(FieldMapperListener.getFieldMappers(mergeWith.rootObjectMapper));
            mergedObjectMappers = objectMappers
                    .addNewMappers(ObjectMapperListener.getObjectMappers(mergeWith.rootObjectMapper));
        } else {
            final MergeContext mergeContext = new MergeContext();
            final MappingLookup mergedMapping = toMerge.merge(mergeWith, mergeContext);
            mergedMappings = mappings.put(type, mergedMapping);
            mergedFieldMappers = fieldMappers
                    .removeMappers(mergeContext.removedFieldMappers())
                    .addNewMappers(mergeContext.newFieldMappers());
            mergedObjectMappers = objectMappers
                    .removeMappers(mergeContext.removedObjectMappers())
                    .addNewMappers(mergeContext.newObjectMappers());
        }

        return new MappingsLookup(mergedMappings, mergedFieldMappers, mergedObjectMappers);
    }

    /**
     * Remove a type from these mappings. The current mappings are not modified.
     */
    public MappingsLookup remove(String type) {
        final MappingLookup toRemove = mappings.get(type);
        if (toRemove == null) {
            return this;
        }
        final CopyOnWriteHashMap<String, MappingLookup> mergedMappings = mappings.remove(type);
        final FieldMappersLookup mergedFieldMappers = fieldMappers
                .removeMappers(FieldMapperListener.getFieldMappers(toRemove.mapping.rootObjectMapper));
        final ObjectMappersLookup mergedObjectMappers = objectMappers
                .removeMappers(ObjectMapperListener.getObjectMappers(toRemove.mapping.rootObjectMapper));

        return new MappingsLookup(mergedMappings, mergedFieldMappers, mergedObjectMappers);
    }

}
