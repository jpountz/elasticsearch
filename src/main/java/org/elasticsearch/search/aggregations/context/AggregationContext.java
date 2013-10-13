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
import java.util.List;

/**
 *
 */
@SuppressWarnings({"unchecked", "ForLoopReplaceableByForEach"})
public class AggregationContext implements ReaderContextAware, ScorerAware {

    private final SearchContext searchContext;

    private ObjectObjectOpenHashMap<String, FieldDataSource> fieldDataSources = new ObjectObjectOpenHashMap<String, FieldDataSource>();
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
        Object[] sources = fieldDataSources.values;
        for (int i = 0; i < fieldDataSources.allocated.length; i++) {
            if (fieldDataSources.allocated[i]) {
                ((FieldDataSource) sources[i]).setNextReader(reader);
            }
        }
    }

    public void setScorer(Scorer scorer) {
        this.scorer = scorer;
        for (int i = 0; i < scorerAwares.size(); i++) {
            scorerAwares.get(i).setScorer(scorer);
        }
    }

    public <VS extends ValuesSource> VS valuesSource(ValuesSourceConfig<VS> config) {
        assert config.valid() : "value source config is invalid - must have either a field context or a script or marked as unmapped";
        assert !config.unmapped : "value source should not be created for unmapped fields";

        if (config.fieldContext == null) {
            if (NumericValuesSource.class.isAssignableFrom(config.valueSourceType)) {
                return (VS) numericScript(config.script, config.multiValued, config.scriptValueType, config.formatter, config.parser);
            }
            if (BytesValuesSource.class.isAssignableFrom(config.valueSourceType)) {
                return (VS) bytesScript(config.script, config.multiValued);
            }
            throw new AggregationExecutionException("value source of type [" + config.valueSourceType.getSimpleName() + "] is not supported by scripts");
        }

        if (NumericValuesSource.class.isAssignableFrom(config.valueSourceType)) {
            return (VS) numericField(config.fieldContext, config.script, config.formatter, config.parser);
        }
        if (BytesValuesSource.class.isAssignableFrom(config.valueSourceType)) {
            return (VS) bytesField(config.fieldContext, config.script);
        }
        if (GeoPointValuesSource.class.isAssignableFrom(config.valueSourceType)) {
            return (VS) geoPointField(config.fieldContext);
        }

        throw new AggregationExecutionException("value source of type [" + config.valueSourceType.getSimpleName() + "] is not supported");
    }

    private NumericValuesSource.Script numericScript(SearchScript script, boolean multiValued, ScriptValueType scriptValueType, ValueFormatter formatter, ValueParser parser) {
        setScorerIfNeeded(script);
        setReaderIfNeeded(script);
        scorerAwares.add(script);
        readerAwares.add(script);
        return new NumericValuesSource.Script(script, multiValued, scriptValueType, formatter, parser);
    }

    private NumericValuesSource numericField(FieldContext fieldContext, SearchScript script, ValueFormatter formatter, ValueParser parser) {
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

    private BytesValuesSource bytesField(FieldContext fieldContext, SearchScript script) {
        FieldDataSource dataSource = fieldDataSources.get(fieldContext.field());
        if (dataSource == null) {
            dataSource = new FieldDataSource.Bytes(fieldContext.field(), fieldContext.indexFieldData());
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

    private BytesValuesSource bytesScript(SearchScript script, boolean multiValued) {
        setScorerIfNeeded(script);
        setReaderIfNeeded(script);
        scorerAwares.add(script);
        readerAwares.add(script);
        return new BytesValuesSource.Script(script, multiValued);
    }

    private GeoPointValuesSource geoPointField(FieldContext fieldContext) {
        FieldDataSource.GeoPoint dataSource = (FieldDataSource.GeoPoint) fieldDataSources.get(fieldContext.field());
        if (dataSource == null) {
            dataSource = new FieldDataSource.GeoPoint(fieldContext.field(), fieldContext.indexFieldData());
            setReaderIfNeeded(dataSource);
            fieldDataSources.put(fieldContext.field(), dataSource);
        }
        return new GeoPointValuesSource.FieldData(dataSource);
    }

    public void registerReaderContextAware(ReaderContextAware readerContextAware) {
        readerAwares.add(readerContextAware);
    }

    public void registerScorerAware(ScorerAware scorerAware) {
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
