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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdinalsBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.fielddata.breaker.CircuitBreakerService;

public class IndexIndexFieldData extends AbstractIndexOrdinalsFieldData {

    public static class Builder implements IndexFieldData.Builder {

        @Override
        public IndexFieldData<?> build(Index index, Settings indexSettings, FieldMapper<?> mapper, IndexFieldDataCache cache,
                CircuitBreakerService breakerService, MapperService mapperService, GlobalOrdinalsBuilder globalOrdinalBuilder) {
            return new IndexIndexFieldData(index, mapper.names());
        }

    }

    private static class IndexAtomicFieldData extends AbstractAtomicOrdinalsFieldData {

        private final String index;

        IndexAtomicFieldData(String index) {
            this.index = index;
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }

        @Override
        public RandomAccessOrds getOrdinalsValues() {
            final BytesRef term = new BytesRef(index);
            return new AbstractRandomAccessOrds() {
                
                @Override
                public BytesRef lookupOrd(long ord) {
                    return term;
                }
                
                @Override
                public long getValueCount() {
                    return 1;
                }
                
                @Override
                public long ordAt(int index) {
                    return 0;
                }
                
                @Override
                public int cardinality() {
                    return 1;
                }
                
                @Override
                protected void doSetDocument(int docID) {
                    // no-op
                }
            };
        }

        @Override
        public void close() {
        }

    }

    private IndexIndexFieldData(Index index, FieldMapper.Names names) {
        super(index, ImmutableSettings.EMPTY, names, new FieldDataType("string"), null, null, null);
    }

    @Override
    public void clear() {
    }

    @Override
    public void clear(IndexReader reader) {
    }

    @Override
    public AtomicOrdinalsFieldData loadDirect(AtomicReaderContext context)
            throws Exception {
        return new IndexAtomicFieldData(index().name());
    }

    @Override
    public IndexOrdinalsFieldData loadGlobal(IndexReader indexReader) {
        return this;
    }

    @Override
    public IndexOrdinalsFieldData localGlobalDirect(IndexReader indexReader) throws Exception {
        return loadGlobal(indexReader);
    }

}
