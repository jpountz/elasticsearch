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

package org.elasticsearch.search.aggregations.context;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.lucene.ReaderContextAware;
import org.elasticsearch.common.lucene.ScorerAware;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.context.bytes.BytesValuesSource;
import org.elasticsearch.search.aggregations.context.geopoints.GeoPointValuesSource;
import org.elasticsearch.search.aggregations.context.numeric.NumericValuesSource;
import org.elasticsearch.search.aggregations.context.numeric.ValueFormatter;
import org.elasticsearch.search.aggregations.context.numeric.ValueParser;
import org.elasticsearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
@SuppressWarnings({"unchecked", "ForLoopReplaceableByForEach"})
public class AggregationContext implements ReaderContextAware, ScorerAware {

    private final SearchContext searchContext;

    private ObjectObjectOpenHashMap<String, FieldDataSource>[] perDepthFieldDataSources = new ObjectObjectOpenHashMap[4];
    private List<ReaderContextAware> readerAwares = new ArrayList<ReaderContextAware>();
    private List<ScorerAware> scorerAwares = new ArrayList<ScorerAware>();

    private AtomicReaderContext reader;
    private Scorer scorer;

    public AggregationContext(SearchContext searchContext) {
        this.searchContext = searchContext;
    }

    public SearchContext searchContext() {
        return searchContext;
    }

    public CacheRecycler cacheRecycler() {
        return searchContext.cacheRecycler();
    }

    public AtomicReaderContext currentReader() {
        return reader;
    }

    public Scorer currentScorer() {
        return scorer;
    }

    public void setNextReader(AtomicReaderContext reader) {
        this.reader = reader;
        for (int i = 0; i < readerAwares.size(); i++) {
            readerAwares.get(i).setNextReader(reader);
        }
        for (int k = 0; k < perDepthFieldDataSources.length; ++k) {
            final ObjectObjectOpenHashMap<String, FieldDataSource> fieldDataSources = perDepthFieldDataSources[k];
            if (fieldDataSources == null) {
                continue;
            }
            Object[] sources = fieldDataSources.values;
            for (int i = 0; i < fieldDataSources.allocated.length; i++) {
                if (fieldDataSources.allocated[i]) {
                    ((FieldDataSource) sources[i]).setNextReader(reader);
                }
            }
        }
    }

    public void setScorer(Scorer scorer) {
        this.scorer = scorer;
        for (int i = 0; i < scorerAwares.size(); i++) {
            scorerAwares.get(i).setScorer(scorer);
        }
    }

    /** Get a value source given its configuration and the depth of the aggregator in the aggregation tree. */
    public <VS extends ValuesSource> VS valuesSource(ValuesSourceConfig<VS> config, int depth) {
        assert config.valid() : "value source config is invalid - must have either a field context or a script or marked as unmapped";
        assert !config.unmapped : "value source should not be created for unmapped fields";

        if (perDepthFieldDataSources.length <= depth) {
            perDepthFieldDataSources = Arrays.copyOf(perDepthFieldDataSources, ArrayUtil.oversize(1 + depth, RamUsageEstimator.NUM_BYTES_OBJECT_REF));
        }
        if (perDepthFieldDataSources[depth] == null) {
            perDepthFieldDataSources[depth] = new ObjectObjectOpenHashMap<String, FieldDataSource>();
        }
        final ObjectObjectOpenHashMap<String, FieldDataSource> fieldDataSources = perDepthFieldDataSources[depth];

        if (config.fieldContext == null) {
            if (NumericValuesSource.class.isAssignableFrom(config.valueSourceType)) {
                return (VS) numericScript(config.script, config.scriptValueType, config.formatter, config.parser);
            }
            if (BytesValuesSource.class.isAssignableFrom(config.valueSourceType)) {
                return (VS) bytesScript(config.script);
            }
            throw new AggregationExecutionException("value source of type [" + config.valueSourceType.getSimpleName() + "] is not supported by scripts");
        }

        if (NumericValuesSource.class.isAssignableFrom(config.valueSourceType)) {
            return (VS) numericField(fieldDataSources, config.fieldContext, config.script, config.formatter, config.parser);
        }
        if (GeoPointValuesSource.class.isAssignableFrom(config.valueSourceType)) {
            return (VS) geoPointField(fieldDataSources, config.fieldContext);
        }
        // falling back to bytes values
        return (VS) bytesField(fieldDataSources, config.fieldContext, config.script, config.needsHashes);
    }

    private NumericValuesSource.Script numericScript(SearchScript script, ScriptValueType scriptValueType, ValueFormatter formatter, ValueParser parser) {
        setScorerIfNeeded(script);
        setReaderIfNeeded(script);
        scorerAwares.add(script);
        readerAwares.add(script);
        return new NumericValuesSource.Script(script, scriptValueType, formatter, parser);
    }

    private NumericValuesSource numericField(ObjectObjectOpenHashMap<String, FieldDataSource> fieldDataSources, FieldContext fieldContext, SearchScript script, ValueFormatter formatter, ValueParser parser) {
        FieldDataSource.Numeric dataSource = (FieldDataSource.Numeric) fieldDataSources.get(fieldContext.field());
        if (dataSource == null) {
            dataSource = new FieldDataSource.Numeric(fieldContext.field(), fieldContext.indexFieldData());
            setReaderIfNeeded(dataSource);
            fieldDataSources.put(fieldContext.field(), dataSource);
        }
        if (script != null) {
            setScorerIfNeeded(script);
            setReaderIfNeeded(script);
            scorerAwares.add(script);
            readerAwares.add(script);
            dataSource = new FieldDataSource.Numeric.WithScript(dataSource, script);
        }
        return new NumericValuesSource.FieldData(dataSource, formatter, parser);
    }

    private BytesValuesSource bytesField(ObjectObjectOpenHashMap<String, FieldDataSource> fieldDataSources, FieldContext fieldContext, SearchScript script, boolean needsHashes) {
        FieldDataSource dataSource = fieldDataSources.get(fieldContext.field());
        if (dataSource == null) {
            dataSource = new FieldDataSource.Bytes(fieldContext.field(), fieldContext.indexFieldData(), needsHashes && script == null);
            setReaderIfNeeded(dataSource);
            fieldDataSources.put(fieldContext.field(), dataSource);
        }
        if (script != null) {
            setScorerIfNeeded(script);
            setReaderIfNeeded(script);
            scorerAwares.add(script);
            readerAwares.add(script);
            dataSource = new FieldDataSource.WithScript(dataSource, script);
        }
        return new BytesValuesSource.FieldData(dataSource);
    }

    private BytesValuesSource bytesScript(SearchScript script) {
        setScorerIfNeeded(script);
        setReaderIfNeeded(script);
        scorerAwares.add(script);
        readerAwares.add(script);
        return new BytesValuesSource.Script(script);
    }

    private GeoPointValuesSource geoPointField(ObjectObjectOpenHashMap<String, FieldDataSource> fieldDataSources, FieldContext fieldContext) {
        FieldDataSource.GeoPoint dataSource = (FieldDataSource.GeoPoint) fieldDataSources.get(fieldContext.field());
        if (dataSource == null) {
            dataSource = new FieldDataSource.GeoPoint(fieldContext.field(), fieldContext.indexFieldData());
            setReaderIfNeeded(dataSource);
            fieldDataSources.put(fieldContext.field(), dataSource);
        }
        return new GeoPointValuesSource.FieldData(dataSource);
    }

    public void registerReaderContextAware(ReaderContextAware readerContextAware) {
        setReaderIfNeeded(readerContextAware);
        readerAwares.add(readerContextAware);
    }

    public void registerScorerAware(ScorerAware scorerAware) {
        setScorerIfNeeded(scorerAware);
        scorerAwares.add(scorerAware);
    }

    private void setReaderIfNeeded(ReaderContextAware readerContextAware) {
        if (reader != null) {
            readerContextAware.setNextReader(reader);
        }
    }

    private void setScorerIfNeeded(ScorerAware scorerAware) {
        if (scorer != null) {
            scorerAware.setScorer(scorer);
        }
    }
}
