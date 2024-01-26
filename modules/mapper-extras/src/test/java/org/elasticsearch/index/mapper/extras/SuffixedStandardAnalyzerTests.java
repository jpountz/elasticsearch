/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class SuffixedStandardAnalyzerTests extends ESTestCase {

    public void testBasic() throws IOException {
        Analyzer analyzer = new SuffixedStandardAnalyzer(new BytesRef("abc"));
        try (TokenStream ts = analyzer.tokenStream("field", "java.lang.OutOfMemoryError: out of memory")) {
            CharTermAttribute termAttribute = ts.addAttribute(CharTermAttribute.class);
            TermToBytesRefAttribute binaryTermAttribute = ts.addAttribute(TermToBytesRefAttribute.class);
            ts.reset();

            assertTrue(ts.incrementToken());
            assertEquals("java.lang.outofmemoryerror", termAttribute.toString());
            assertEquals(new BytesRef("java.lang.outofmemoryerrorabc"), binaryTermAttribute.getBytesRef());

            assertTrue(ts.incrementToken());
            assertEquals("out", termAttribute.toString());
            assertEquals(new BytesRef("outabc"), binaryTermAttribute.getBytesRef());

            assertTrue(ts.incrementToken());
            assertEquals("of", termAttribute.toString());
            assertEquals(new BytesRef("ofabc"), binaryTermAttribute.getBytesRef());

            assertTrue(ts.incrementToken());
            assertEquals("memory", termAttribute.toString());
            assertEquals(new BytesRef("memoryabc"), binaryTermAttribute.getBytesRef());

            assertFalse(ts.incrementToken());

            ts.end();
        }
    }

}
