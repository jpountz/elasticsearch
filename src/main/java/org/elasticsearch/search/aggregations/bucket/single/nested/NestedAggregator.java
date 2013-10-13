/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.single.nested;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.ReaderContextAware;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.search.nested.NonNestedDocsFilter;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.single.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValueSpace;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class NestedAggregator extends SingleBucketAggregator implements ReaderContextAware {

    private final Filter parentFilter;
    private final Filter childFilter;

    private Bits childDocs;
    private FixedBitSet parentDocs;

    private long docCount;
    private InternalAggregations aggregations;

    public NestedAggregator(String name, List<Aggregator.Factory> factories, String nestedPath, AggregationContext aggregationContext, Aggregator parent) {
        super(name, factories, aggregationContext, parent);
        MapperService.SmartNameObjectMapper mapper = aggregationContext.searchContext().smartNameObjectMapper(nestedPath);
        if (mapper == null) {
            throw new AggregationExecutionException("facet nested path [" + nestedPath + "] not found");
        }
        ObjectMapper objectMapper = mapper.mapper();
        if (objectMapper == null) {
            throw new AggregationExecutionException("facet nested path [" + nestedPath + "] not found");
        }
        if (!objectMapper.nested().isNested()) {
            throw new AggregationExecutionException("facet nested path [" + nestedPath + "] is not nested");
        }
        parentFilter = aggregationContext.searchContext().filterCache().cache(NonNestedDocsFilter.INSTANCE);
        childFilter = aggregationContext.searchContext().filterCache().cache(objectMapper.nestedTypeFilter());

        aggregationContext.registerReaderContextAware(this);
    }

    @Override
    protected Collector collector(Aggregator[] aggregators) {
        return new Collector(aggregators);
    }

    @Override
    protected InternalAggregation buildAggregation(InternalAggregations aggregations) {
        return new InternalNested(name, docCount, aggregations);
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        try {
            DocIdSet docIdSet = parentFilter.getDocIdSet(reader, null);
            // Im ES if parent is deleted, then also the children are deleted. Therefore acceptedDocs can also null here.
            childDocs = DocIdSets.toSafeBits(reader.reader(), childFilter.getDocIdSet(reader, null));
            if (DocIdSets.isEmpty(docIdSet)) {
                parentDocs = null;
            } else {
                parentDocs = (FixedBitSet) docIdSet;
            }
        } catch (IOException ioe) {
            throw new AggregationExecutionException("Failed to aggregate [" + name + "]", ioe);
        }
    }

    class Collector implements Aggregator.Collector {

        private final Aggregator[] subAggregators;
        private final Aggregator.Collector[] collectors;

        private long docCount;

        public Collector(Aggregator[] subAggregators) {
            this.subAggregators = subAggregators;
            this.collectors = new Aggregator.Collector[subAggregators.length];
            for (int i = 0; i < subAggregators.length; i++) {
                collectors[i] = subAggregators[i].collector();
            }
        }

        @Override
        public final void postCollection() {
            for (int i = 0; i < collectors.length; i++) {
                if (collectors[i] != null) {
                    collectors[i].postCollection();
                }
            }
            NestedAggregator.this.docCount = docCount;
            NestedAggregator.this.aggregations = buildAggregations(subAggregators);
        }

        @Override
        public void collect(int parentDoc, ValueSpace valueSpace) throws IOException {
            if (parentDoc == 0 || parentDocs == null) {
                return;
            }
            int prevParentDoc = parentDocs.prevSetBit(parentDoc - 1);
            for (int i = (parentDoc - 1); i > prevParentDoc; i--) {
                if (childDocs.get(i)) {
                    docCount++;
                    for (int j = 0; j < collectors.length; j++) {
                        if (collectors[j] != null) {
                            collectors[j].collect(i, valueSpace);
                        }
                    }
                }
            }
        }

    }

    public static class Factory extends Aggregator.CompoundFactory {

        private final String path;

        public Factory(String name, String path) {
            super(name);
            this.path = path;
        }

        @Override
        public NestedAggregator create(AggregationContext context, Aggregator parent) {
            return new NestedAggregator(name, factories, path, context, parent);
        }
    }
}
