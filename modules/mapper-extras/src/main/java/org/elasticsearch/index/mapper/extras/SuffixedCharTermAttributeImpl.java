/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.analysis.tokenattributes.CharTermAttributeImpl;
import org.apache.lucene.util.BytesRef;

import java.util.Objects;

public class SuffixedCharTermAttributeImpl extends CharTermAttributeImpl {

    private final BytesRef suffix;

    public SuffixedCharTermAttributeImpl(BytesRef suffix) {
        this.suffix = Objects.requireNonNull(suffix);
    }

    @Override
    public BytesRef getBytesRef() {
        builder.copyChars(buffer(), 0, length());
        builder.append(suffix);
        return builder.get();
    }

}
