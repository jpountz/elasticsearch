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



/**
 * Same as {@link Mapping} but wraps additional redundant lookup structures.
 */
final class MappingLookup {

    public final Mapping mapping;
    public final FieldMappersLookup fieldMappers;
    public final ObjectMappersLookup objectMappers;

    public MappingLookup(Mapping mapping) {
        this(mapping,
                new FieldMappersLookup().addNewMappers(FieldMapperListener.getFieldMappers(mapping.rootObjectMapper)),
                new ObjectMappersLookup().addNewMappers(ObjectMapperListener.getObjectMappers(mapping.rootObjectMapper)));
    }

    private MappingLookup(Mapping mapping, FieldMappersLookup fieldMappers, ObjectMappersLookup objectMappers) {
        this.mapping = mapping;
        this.fieldMappers = fieldMappers;
        this.objectMappers = objectMappers;
    }

    /**
     * Return the result of the merge of <code>mergeWith</code> into <code>this</code>.
     * The current mapping is not modified.
     */
    public MappingLookup merge(Mapping mergeWith, MergeContext mergeContext) {
        final Mapping mergedMapping = mapping.merge(mergeWith, mergeContext);
        // We don't regenerate lookup structures from the merged mapping because this
        // could be very costly in case of large mappings. Instead, we try to just
        // apply the diff
        final FieldMappersLookup mergedFieldMappers = fieldMappers
                .removeMappers(mergeContext.removedFieldMappers())
                .addNewMappers(mergeContext.newFieldMappers());
        final ObjectMappersLookup mergeObjectMappers = objectMappers
                .removeMappers(mergeContext.removedObjectMappers())
                .addNewMappers(mergeContext.newObjectMappers());

        return new MappingLookup(mergedMapping, mergedFieldMappers, mergeObjectMappers);
    }
}
