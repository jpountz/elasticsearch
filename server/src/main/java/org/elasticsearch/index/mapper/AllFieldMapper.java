/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.Collections;

import static org.apache.lucene.index.IndexWriter.MAX_TERM_LENGTH;
import static org.elasticsearch.index.mapper.KeywordFieldMapper.failIfExceedsMaxLength;

public class AllFieldMapper extends MetadataFieldMapper {

    static final byte FIELD_VALUE_SEPARATOR = 0;  // nul code point
    public static final String NAME = "_all";
    public static final AllFieldMapper ENABLED_INSTANCE = new AllFieldMapper(true);
    private static final AllFieldMapper DISABLED_INSTANCE = new AllFieldMapper(false);

    private final boolean enabled;

    public static class Defaults {
        public static final FieldType FIELD_TYPE;

        static {
            FieldType ft = new FieldType();
            ft.setTokenized(false);
            ft.setOmitNorms(true);
            ft.setIndexOptions(IndexOptions.DOCS);
            ft.setDocValuesType(DocValuesType.NONE);
            ft.setStored(false);
            FIELD_TYPE = freezeAndDeduplicateFieldType(ft);
        }

        public static TextSearchInfo TEXT_SEARCH_INFO = new TextSearchInfo(
            FIELD_TYPE,
            null,
            Lucene.KEYWORD_ANALYZER,
            Lucene.KEYWORD_ANALYZER
        );

    }

    public static class Builder extends MetadataFieldMapper.Builder {

        private final Parameter<Boolean> enabled;

        public Builder() {
            super(NAME);
            this.enabled = Parameter.boolParam("enabled", true, m -> toType(m).enabled, false)
                .setMergeValidator((previous, current, conflicts) -> previous == current);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { enabled };
        }

        @Override
        public MetadataFieldMapper build() {
            return enabled.getValue() ? ENABLED_INSTANCE : DISABLED_INSTANCE;
        }

        private static AllFieldMapper toType(FieldMapper in) {
            return (AllFieldMapper) in;
        }
    }

    public static final class AllFieldType extends MappedFieldType {

        static final AllFieldType INSTANCE = new AllFieldType();

        private AllFieldType() {
            super(NAME, true, false, false, Defaults.TEXT_SEARCH_INFO, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return NAME;
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support term queries");
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support exists queries");
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            throw new UnsupportedOperationException();
        }
    }

    public static final TypeParser PARSER = new ConfigurableTypeParser(c -> DISABLED_INSTANCE, c -> new AllFieldMapper.Builder());

    private AllFieldMapper(boolean enabled) {
        super(AllFieldType.INSTANCE);
        this.enabled = enabled;
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder().init(this);
    }

    public void addToAll(DocumentParserContext context, IndexableField indexableField) throws IOException {
        if (enabled == false) {
            return;
        }

        try (TokenStream tokenStream = indexableField.tokenStream(Lucene.STANDARD_ANALYZER, null)) {
            CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
            tokenStream.reset();
            while (tokenStream.incrementToken()) {
                addToAll(context, new BytesRef(termAtt.toString()), indexableField.name());
            }
            tokenStream.end();
        }
    }

    public void addToAll(DocumentParserContext context, BytesRef value, String field) {
        if (enabled == false) {
            return;
        }

        BytesRef term = toAllFieldTerm(value, new BytesRef(field));
        if (term.length > MAX_TERM_LENGTH) {
            failIfExceedsMaxLength(field, term);
            assert false;
            return;
        }
        context.doc().add(new KeywordFieldMapper.KeywordField(NAME, term, Defaults.FIELD_TYPE));
    }

    public static BytesRef toAllFieldTerm(BytesRef fieldValueBytes, BytesRef fieldNameBytes) {
        BytesRefBuilder builder = new BytesRefBuilder();
        builder.append(fieldValueBytes);
        builder.append(FIELD_VALUE_SEPARATOR);
        builder.append(fieldNameBytes);
        return builder.toBytesRef();
    }

    @Override
    protected String contentType() {
        return NAME;
    }

    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        return SourceLoader.SyntheticFieldLoader.NOTHING;
    }
}
