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
package org.elasticsearch.index.fielddata.ordinals;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.packed.AppendingPackedLongBuffer;
import org.apache.lucene.util.packed.MonotonicAppendingLongBuffer;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.internal.SearchContext;

/**
 * {@link IndexFieldData} impl based on global ordinals.
 */
public final class GlobalOrdinalsIndexFieldData extends AbstractIndexComponent implements IndexFieldData.WithOrdinals, RamUsage {

    private final FieldMapper.Names fieldNames;
    private final Atomic[] atomicReaders;
    private final long memorySizeInBytes;
    private final long maxGlobalOrdinal;

    public GlobalOrdinalsIndexFieldData(Index index, Settings settings, FieldMapper.Names fieldNames, AtomicFieldData.WithOrdinals[] segmentAfd, AppendingPackedLongBuffer globalOrdToFirstSegment, MonotonicAppendingLongBuffer globalOrdToFirstSegmentOrd, MonotonicAppendingLongBuffer[] segmentOrdToGlobalOrds, long memorySizeInBytes, long higestGlobalOrdinal) {
        super(index, settings);
        this.fieldNames = fieldNames;
        this.maxGlobalOrdinal = higestGlobalOrdinal + 1;
        this.atomicReaders = new Atomic[segmentAfd.length];
        for (int i = 0; i < segmentAfd.length; i++) {
            atomicReaders[i] = new Atomic(segmentAfd[i], globalOrdToFirstSegment, globalOrdToFirstSegmentOrd, segmentOrdToGlobalOrds[i], maxGlobalOrdinal);
        }
        this.memorySizeInBytes = memorySizeInBytes;
    }

    @Override
    public AtomicFieldData.WithOrdinals load(AtomicReaderContext context) {
        return atomicReaders[context.ord];
    }

    @Override
    public AtomicFieldData.WithOrdinals loadDirect(AtomicReaderContext context) throws Exception {
        return load(context);
    }

    @Override
    public WithOrdinals loadGlobal(IndexReader indexReader) {
        return this;
    }

    @Override
    public WithOrdinals localGlobalDirect(IndexReader indexReader) throws Exception {
        return this;
    }

    @Override
    public FieldMapper.Names getFieldNames() {
        return fieldNames;
    }

    @Override
    public boolean valuesOrdered() {
        return false;
    }

    @Override
    public XFieldComparatorSource comparatorSource(@Nullable Object missingValue, SortMode sortMode) {
        return null;
    }

    @Override
    public void clear() {

    }

    @Override
    public void clear(IndexReader reader) {

    }

    @Override
    public long getMemorySizeInBytes() {
        return memorySizeInBytes;
    }

    private final class Atomic implements AtomicFieldData.WithOrdinals {

        private final long maxOrd;
        private final AtomicFieldData.WithOrdinals afd;
        private final MonotonicAppendingLongBuffer segmentOrdToGlobalOrdLookup;
        private final AppendingPackedLongBuffer globalOrdToFirstSegment;
        private final MonotonicAppendingLongBuffer globalOrdToFirstSegmentOrd;

        private Atomic(WithOrdinals afd, AppendingPackedLongBuffer globalOrdToFirstSegment, MonotonicAppendingLongBuffer globalOrdToFirstSegmentOrd, MonotonicAppendingLongBuffer segmentOrdToGlobalOrdLookup, long maxOrd) {
            this.afd = afd;
            this.segmentOrdToGlobalOrdLookup = segmentOrdToGlobalOrdLookup;
            this.globalOrdToFirstSegment = globalOrdToFirstSegment;
            this.globalOrdToFirstSegmentOrd = globalOrdToFirstSegmentOrd;
            this.maxOrd = maxOrd;
        }

        @Override
        public BytesValues.WithOrdinals getBytesValues(boolean needsHashes) {
            BytesValues.WithOrdinals values = afd.getBytesValues(false);
            Ordinals.Docs actual = values.ordinals();
            Ordinals.Docs wrapper;
            // TODO: Think harder how nicely inject BigArrays here...
            if (false && actual.getMaxOrd() < 512 && SearchContext.current() != null) { // TODO: Maybe for small segments, we should not use global ord cache?
                wrapper = new Caching(actual, segmentOrdToGlobalOrdLookup, maxOrd);
            } else {
                wrapper = new SegmentOrdinalsToGlobalOrdinalsWrapper(actual, segmentOrdToGlobalOrdLookup, maxOrd);
            }
            return new BytesValues.WithOrdinals(wrapper) {

                int readerIndex;
                final IntObjectOpenHashMap<BytesValues.WithOrdinals> bytesValuesCache = new IntObjectOpenHashMap<>();

                @Override
                public BytesRef getValueByOrd(long globalOrd) {
                    final long segmentOrd = globalOrdToFirstSegmentOrd.get(globalOrd);
                    readerIndex = (int) globalOrdToFirstSegment.get(globalOrd);
                    if (bytesValuesCache.containsKey(readerIndex)) {
                        return bytesValuesCache.lget().getValueByOrd(segmentOrd);
                    } else {
                        BytesValues.WithOrdinals k = atomicReaders[readerIndex].afd.getBytesValues(false);
                        bytesValuesCache.put(readerIndex, k);
                        return k.getValueByOrd(segmentOrd);
                    }
                }

                @Override
                public BytesRef copyShared() {
                    return bytesValuesCache.get(readerIndex).copyShared();
                }
            };
        }

        @Override
        public boolean isMultiValued() {
            return afd.isMultiValued();
        }

        @Override
        public boolean isValuesOrdered() {
            return false;
        }

        @Override
        public int getNumDocs() {
            return afd.getNumDocs();
        }

        @Override
        public long getNumberUniqueValues() {
            return afd.getNumberUniqueValues();
        }

        @Override
        public long getMemorySizeInBytes() {
            return afd.getMemorySizeInBytes();
        }

        @Override
        public ScriptDocValues getScriptValues() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }

        private class SegmentOrdinalsToGlobalOrdinalsWrapper implements Ordinals.Docs {

            protected final Ordinals.Docs segmentOrdinals;
            protected final long maxOrd;
            private final MonotonicAppendingLongBuffer segmentOrdToGlobalOrdLookup;

            protected long currentGlobalOrd;

            private SegmentOrdinalsToGlobalOrdinalsWrapper(Ordinals.Docs segmentOrdinals, MonotonicAppendingLongBuffer segmentOrdToGlobalOrdLookup, long maxOrd) {
                this.segmentOrdinals = segmentOrdinals;
                this.segmentOrdToGlobalOrdLookup = segmentOrdToGlobalOrdLookup;
                this.maxOrd = maxOrd;
            }

            @Override
            public Ordinals ordinals() {
                return new Ordinals() {
                    @Override
                    public long getMemorySizeInBytes() {
                        return GlobalOrdinalsIndexFieldData.this.getMemorySizeInBytes();
                    }

                    @Override
                    public boolean isMultiValued() {
                        return SegmentOrdinalsToGlobalOrdinalsWrapper.this.isMultiValued();
                    }

                    @Override
                    public int getNumDocs() {
                        return SegmentOrdinalsToGlobalOrdinalsWrapper.this.getNumDocs();
                    }

                    @Override
                    public long getNumOrds() {
                        return SegmentOrdinalsToGlobalOrdinalsWrapper.this.getNumOrds();
                    }

                    @Override
                    public long getMaxOrd() {
                        return SegmentOrdinalsToGlobalOrdinalsWrapper.this.getMaxOrd();
                    }

                    @Override
                    public Docs ordinals() {
                        return SegmentOrdinalsToGlobalOrdinalsWrapper.this;
                    }
                };
            }

            @Override
            public int getNumDocs() {
                return segmentOrdinals.getNumDocs();
            }

            @Override
            public long getNumOrds() {
                return maxOrd - Ordinals.MIN_ORDINAL;
            }

            @Override
            public long getMaxOrd() {
                return maxOrd;
            }

            @Override
            public boolean isMultiValued() {
                return segmentOrdinals.isMultiValued();
            }

            @Override
            public long getOrd(int docId) {
                long segmentOrd = segmentOrdinals.getOrd(docId);
                return currentGlobalOrd = segmentOrdToGlobalOrdLookup.get(segmentOrd);
            }

            @Override
            public LongsRef getOrds(int docId) {
                LongsRef refs = segmentOrdinals.getOrds(docId);
                for (int i = refs.offset; i < refs.length; i++) {
                    refs.longs[i] = segmentOrdToGlobalOrdLookup.get(refs.longs[i]);
                }
                return refs;
            }

            @Override
            public long nextOrd() {
                long segmentOrd = segmentOrdinals.nextOrd();
                return currentGlobalOrd = segmentOrdToGlobalOrdLookup.get(segmentOrd);
            }

            @Override
            public int setDocument(int docId) {
                return segmentOrdinals.setDocument(docId);
            }

            @Override
            public long currentOrd() {
                return currentGlobalOrd;
            }
        }

        private final class Caching extends SegmentOrdinalsToGlobalOrdinalsWrapper implements Releasable {

            private final LongArray globalOrdinalCache;

            private Caching(Ordinals.Docs segmentOrdinals, MonotonicAppendingLongBuffer segmentOrdToGlobalOrdLookup, long maxOrd) {
                super(segmentOrdinals, segmentOrdToGlobalOrdLookup, maxOrd);
                this.globalOrdinalCache = SearchContext.current().bigArrays().newLongArray(maxOrd, false);
                globalOrdinalCache.fill(0, maxOrd, -1L);
            }

            @Override
            public long getOrd(int docId) {
                long segmentOrd = segmentOrdinals.getOrd(docId);
                currentGlobalOrd = globalOrdinalCache.get(segmentOrd);
                if (currentGlobalOrd < 0) {
                    // unlikely condition on a low-cardinality field
                    globalOrdinalCache.set(segmentOrd, currentGlobalOrd = segmentOrdToGlobalOrdLookup.get(segmentOrd));
                }
                return currentGlobalOrd;
            }

            @Override
            public LongsRef getOrds(int docId) {
                LongsRef refs = segmentOrdinals.getOrds(docId);
                for (int i = refs.offset; i < refs.length; i++) {
                    long globalOrd = globalOrdinalCache.get(refs.longs[i]);
                    if (currentGlobalOrd < 0) {
                        // unlikely condition on a low-cardinality field
                        globalOrdinalCache.set(refs.longs[i], globalOrd = segmentOrdToGlobalOrdLookup.get(refs.longs[i]));
                    }
                    refs.longs[i] = globalOrd;
                }
                return refs;
            }

            @Override
            public long nextOrd() {
                long segmentOrd = segmentOrdinals.nextOrd();
                currentGlobalOrd = globalOrdinalCache.get(segmentOrd);
                if (currentGlobalOrd < 0) {
                    // unlikely condition on a low-cardinality field
                    globalOrdinalCache.set(segmentOrd, currentGlobalOrd = segmentOrdToGlobalOrdLookup.get(segmentOrd));
                }
                return currentGlobalOrd;
            }

            @Override
            public boolean release() throws ElasticsearchException {
                Releasables.release(globalOrdinalCache);
                return true;
            }
        }
    }
}
