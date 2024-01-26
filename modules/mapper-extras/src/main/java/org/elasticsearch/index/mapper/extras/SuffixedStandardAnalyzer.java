/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.BytesRef;

public class SuffixedStandardAnalyzer extends Analyzer {

    private static final int MAX_TOKEN_LENGTH = 255;

    private final SuffixedTermAttributeFactory factory;

    public SuffixedStandardAnalyzer(BytesRef suffix) {
        factory = new SuffixedTermAttributeFactory(suffix);
    }

    @Override
    protected TokenStreamComponents createComponents(final String fieldName) {
        final StandardTokenizer src = new StandardTokenizer(factory);
        src.setMaxTokenLength(MAX_TOKEN_LENGTH);
        TokenStream tok = new LowerCaseFilter(src);
        return new TokenStreamComponents(src::setReader, tok);
    }

    @Override
    protected TokenStream normalize(String fieldName, TokenStream in) {
        return new LowerCaseFilter(in);
    }

}
