/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.util;

import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.util.bkd.BKDReader;

/**
 * Utility class that maps {@link StackTraceElement}s to the operation that they are associated with.
 */
public final class StackTraceElementTagger {

    static final String STORE_READ = "Reading bytes from disk";
    static final String STORE_WRITE = "Writing bytes to disk";
    static final String ORDINAL_MAP_BUILD = "Building a map of segment to global ordinals";
    static final String DISI_ITER = "Iterating over matching documents";
    static final String TWO_PHASE_MATCH = "Verifying a match";
    static final String TERMS_ITER = "Walking a terms dictionary";
    static final String TERMS_SEEK = "Seeking a terms dictionary";
    static final String INDEX_SEARCH = "Searching a shard";
    static final String BKD_VISIT = "Visiting a BKD tree";
    static final String SEGMENT_MERGE = "Merging segments";
    static final String SEGMENT_FLUSH = "Flushing a segment";
    static final String ANALYSIS = "Analyzing text";

    private StackTraceElementTagger() {}

    public static String tag(StackTraceElement ste) {
        switch (ste.getMethodName()) {
        case "readBytes":
        case "readByte":
        case "readShort":
        case "readInt":
        case "readLong":
            if (ste.getClassName().endsWith("IndexInput")) {
                return STORE_READ;
            }
            break;
        case "writeByte":
        case "writeBytes":
            if (ste.getClassName().endsWith("IndexOutput")) {
                return STORE_WRITE;
            }
            break;
        case "build":
            if (ste.getClassName().equals(OrdinalMap.class.getName())) {
                return ORDINAL_MAP_BUILD;
            }
            break;
        case "nextDoc":
        case "advance":
        case "advanceExact":
            return DISI_ITER;
        case "matches":
            // TODO: check class or file name?
            return TWO_PHASE_MATCH;
        case "next":
        case "seekCeil":
            if (ste.getClassName().endsWith("TermsEnum")) {
                return TERMS_ITER;
            }
            break;
        case "seekExact":
            if (ste.getClassName().endsWith("TermsEnum")) {
                return TERMS_SEEK;
            }
            break;
        case "search":
            if (ste.getClassName().endsWith("IndexSearcher")) {
                return INDEX_SEARCH;
            }
            break;
        case "intersect":
            if (ste.getClassName().equals(BKDReader.class.getName())) {
                return BKD_VISIT;
            }
            break;
        case "merge":
            if (ste.getClassName().endsWith(".SegmentMerger")) {
                return SEGMENT_MERGE;
            }
            break;
        case "flush":
            if (ste.getClassName().endsWith(".DefaultIndexingChain")) {
                return SEGMENT_FLUSH;
            }
        case "incrementToken":
            return ANALYSIS;
        }
        return null;
    }

}
