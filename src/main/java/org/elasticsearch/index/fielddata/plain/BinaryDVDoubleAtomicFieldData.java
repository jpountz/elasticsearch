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

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;

final class BinaryDVDoubleAtomicFieldData extends AbstractAtomicDoubleFieldData {

    private final BinaryDocValues values;
    private final NumericType numericType;

    BinaryDVDoubleAtomicFieldData(BinaryDocValues values, NumericType numericType) {
        this.values = values;
        this.numericType = numericType;
    }

    @Override
    public SortedNumericDoubleValues getDoubleValues() {
        switch (numericType) {
        case FLOAT:
            return new SortedNumericDoubleValues() {

                BytesRef bytes;
                int valueCount = 0;

                @Override
                public void setDocument(int docId) {
                    bytes = values.get(docId);
                    assert bytes.length % 4 == 0;
                    valueCount = bytes.length / 4;
                }

                @Override
                public int count() {
                    return valueCount;
                }

                @Override
                public double valueAt(int index) {
                    return ByteUtils.readFloatLE(bytes.bytes, bytes.offset + index * 4);
                }

            };
        case DOUBLE:
            return new SortedNumericDoubleValues() {

                BytesRef bytes;
                int valueCount = 0;

                @Override
                public void setDocument(int docId) {
                    bytes = values.get(docId);
                    assert bytes.length % 8 == 0;
                    valueCount = bytes.length / 8;
                }

                @Override
                public int count() {
                    return valueCount;
                }

                @Override
                public double valueAt(int index) {
                    return ByteUtils.readDoubleLE(bytes.bytes, bytes.offset + index * 8);
                }

            };
        default:
            throw new AssertionError();
        }
    }

    @Override
    public long ramBytesUsed() {
        return -1; // Lucene doesn't expose it
    }

    @Override
    public void close() {
        // no-op
    }

}
