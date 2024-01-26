/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.BytesRef;

public class SuffixedTermAttributeFactory extends AttributeFactory.StaticImplementationAttributeFactory<SuffixedCharTermAttributeImpl> {

    private final BytesRef suffix;

    public SuffixedTermAttributeFactory(BytesRef suffix) {
        super(TokenStream.DEFAULT_TOKEN_ATTRIBUTE_FACTORY, SuffixedCharTermAttributeImpl.class);
        this.suffix = suffix;
    }

    @Override
    protected SuffixedCharTermAttributeImpl createInstance() {
        return new SuffixedCharTermAttributeImpl(suffix);
    }
}
