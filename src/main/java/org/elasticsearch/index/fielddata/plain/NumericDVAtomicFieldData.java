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

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.Bits;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.index.fielddata.AtomicFieldData;

import java.io.IOException;

/** {@link AtomicFieldData} impl on top of Lucene's numeric doc values. */
public class NumericDVAtomicFieldData extends AbstractAtomicLongFieldData {

    private final AtomicReader reader;
    private final String field;

    public NumericDVAtomicFieldData(AtomicReader reader, String field) {
        this.reader = reader;
        this.field = field;
    }

    @Override
    public void close() {
        // no-op
    }

    @Override
    public long ramBytesUsed() {
        // TODO: cannot be computed from Lucene
        return -1;
    }

    private static class DocValuesAndBits {
        final NumericDocValues values;
        final Bits docsWithField;

        DocValuesAndBits(NumericDocValues values, Bits docsWithField) {
            super();
            this.values = values;
            this.docsWithField = docsWithField;
        }
    }

    private DocValuesAndBits getDocValues() {
        try {
            final NumericDocValues values = DocValues.getNumeric(reader, field);
            final Bits docsWithField = DocValues.getDocsWithField(reader, field);
            return new DocValuesAndBits(values, docsWithField);
        } catch (IOException e) {
            throw new ElasticsearchIllegalStateException("Cannot load doc values", e);
        }
    }

    @Override
    public SortedNumericDocValues getLongValues() {
        final DocValuesAndBits docValues = getDocValues();
        return DocValues.singleton(docValues.values, docValues.docsWithField);
    }

}
