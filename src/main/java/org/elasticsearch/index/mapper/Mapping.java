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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.object.RootObjectMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * This class represents the mapping of a specific type.
 */
final class Mapping implements Mapper {

    public final ImmutableMap<String, Object> meta;
    public final RootObjectMapper rootObjectMapper;
    public final ImmutableMap<Class<? extends RootMapper>, RootMapper> rootMappers;
    public final RootMapper[] rootMappersOrdered;
    public final RootMapper[] rootMappersNotIncludedInObject;

    public Mapping(ImmutableMap<String, Object> meta, RootObjectMapper rootObjectMapper,
            RootMapper[] rootMappersOrdered) {
        this.meta = meta;
        this.rootObjectMapper = rootObjectMapper;
        this.rootMappersOrdered = rootMappersOrdered;
        if (rootMappersOrdered == null) {
            rootMappers = null;
            rootMappersNotIncludedInObject = null;
        } else {
            ImmutableMap.Builder<Class<? extends RootMapper>, RootMapper> rootMappers = ImmutableMap.builder();
            for (RootMapper rootMapper : rootMappersOrdered) {
                rootMappers.put(rootMapper.getClass(), rootMapper);
            }
            this.rootMappers = rootMappers.build();
            List<RootMapper> rootMappersNotIncludedInObjectLst = Lists.newArrayList();
            for (RootMapper rootMapper : rootMappersOrdered) {
                if (!rootMapper.includeInObject()) {
                    rootMappersNotIncludedInObjectLst.add(rootMapper);
                }
            }
            rootMappersNotIncludedInObject = rootMappersNotIncludedInObjectLst.toArray(new RootMapper[rootMappersNotIncludedInObjectLst.size()]);
        }
    }

    private ImmutableMap<Class<? extends RootMapper>, RootMapper> mergeRootMappers(Mapping mergeWith, RootObjectMapper mergedRoot, MergeContext mergeContext) {
        final ImmutableMap.Builder<Class<? extends RootMapper>, RootMapper> mergedRootMappers = ImmutableMap.builder();

        // root mappers that are under the root object mapper have already been merged, get them back
        for (Mapper mapper : mergedRoot.mappers()) {
            if (mapper instanceof RootMapper) {
                RootMapper rootMapper = (RootMapper) mapper;
                mergedRootMappers.put(rootMapper.getClass(), rootMapper);
            }
        }

        // now merge other root mappers
        for (Map.Entry<Class<? extends RootMapper>, RootMapper> entry : rootMappers.entrySet()) {
            // root mappers included in root object got merged in the rootObjectMapper
            if (entry.getValue().includeInObject()) {
                continue;
            }
            RootMapper mergeWithRootMapper = mergeWith.rootMappers.get(entry.getKey());
            if (mergeWithRootMapper != null) {
                RootMapper merged = (RootMapper) entry.getValue().merge(mergeWithRootMapper, mergeContext);
                mergedRootMappers.put(merged.getClass(), merged);
            }
        }

        return mergedRootMappers.build();
    }

    /**
     * Return the result of the merge of <code>mergeWith</code> into <code>this</code>.
     * The current mapping is not modified.
     */
    @Override
    public Mapping merge(Mapper mergeWithMapper, MergeContext mergeContext) {
        if (!(mergeWithMapper instanceof Mapping)) {
            throw new AssertionError(); // nocommit
        }
        final Mapping mergeWith = (Mapping) mergeWithMapper;
        assert rootMappers.size() == mergeWith.rootMappers.size();

        final ImmutableMap<String, Object> mergedMeta = mergeWith.meta != null ? mergeWith.meta : meta;
        final RootObjectMapper mergedRoot;
        if (mergeWith.rootObjectMapper == null) {
            mergedRoot = rootObjectMapper;
        } else {
            mergedRoot = (RootObjectMapper) rootObjectMapper.merge(mergeWith.rootObjectMapper, mergeContext);
        }
        final RootMapper[] mergedRootMappers;
        if (mergeWith.rootMappersOrdered == null) {
            mergedRootMappers = rootMappersOrdered;
        } else {
            mergedRootMappers = new RootMapper[this.rootMappersOrdered.length];
            for (int i = 0; i < mergedRootMappers.length; ++i) {
                mergedRootMappers[i] = rootMappersOrdered[i].merge(mergeWith.rootMappers.get(rootMappersOrdered[i].getClass()), mergeContext);
                assert mergedRootMappers[i] != null;
            }
        }
        return new Mapping(mergedMeta, mergedRoot, mergedRootMappers);
    }

    /**
     * Parse and return a {@link Mapping} update if new mappings have been
     * introduced via dynamic mappings or null if no mapping changes have been
     * performed.
     *
     * Note that the returned mapping is generally not usable as-in and should
     * only be used to perform merges.
     */
    public Mapping parse(ParseContext context) throws IOException {
        RootMapper[] rootMappersUpdate = null;
        RootObjectMapper rootUpdate = null;

        for (int i = 0; i < rootMappersOrdered.length; ++i) {
            RootMapper rootMapper = rootMappersOrdered[i];
            RootMapper update = rootMapper.preParse(context);
            if (update != null) {
                if (rootMappersUpdate == null) {
                    rootMappersUpdate = Arrays.copyOf(rootMappersOrdered, rootMappersOrdered.length);
                }
                rootMappersUpdate[i] = update;
            }
        }

        if (context.parser().currentToken() != XContentParser.Token.END_OBJECT) {
            rootUpdate = (RootObjectMapper) rootObjectMapper.parse(context);
        }

        for (int i = 0; i < rootMappersOrdered.length; ++i) {
            RootMapper rootMapper = rootMappersOrdered[i];
            RootMapper update = rootMapper.postParse(context);
            if (update != null) {
                if (rootMappersUpdate == null) {
                    rootMappersUpdate = Arrays.copyOf(rootMappersOrdered, rootMappersOrdered.length);
                }
                rootMappersUpdate[i] = update;
            }
        }

        if (rootUpdate == null && rootMappersUpdate == null) {
            return null;
        } else {
            return new Mapping(null, rootUpdate, rootMappersUpdate);
        }
    }

    @Override
    public void traverse(FieldMapperListener listener) {
        for (RootMapper rootMapper : rootMappersOrdered) {
            if (!rootMapper.includeInObject() && rootMapper instanceof FieldMapper) {
                listener.fieldMapper((FieldMapper<?>) rootMapper);
            }
        }
        rootObjectMapper.traverse(listener);
    }

    @Override
    public void traverse(ObjectMapperListener listener) {
        rootObjectMapper.traverse(listener);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }

    @Override
    public String name() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        Releasables.close(Iterables.concat(Arrays.asList(rootMappersNotIncludedInObject), Collections.singleton(rootObjectMapper)));
    }

}
