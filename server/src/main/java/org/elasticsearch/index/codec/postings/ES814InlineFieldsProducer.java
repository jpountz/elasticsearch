/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.postings;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SlowImpactsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.packed.MonotonicBlockPackedReader;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.lucene.util.BinaryInterpolativeCoding;
import org.elasticsearch.lucene.util.BitStreamInput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.codec.postings.ES814InlinePostingsFormat.INVERTED_INDEX_CODEC;
import static org.elasticsearch.index.codec.postings.ES814InlinePostingsFormat.INVERTED_INDEX_EXTENSION;
import static org.elasticsearch.index.codec.postings.ES814InlinePostingsFormat.META_CODEC;
import static org.elasticsearch.index.codec.postings.ES814InlinePostingsFormat.META_EXTENSION;
import static org.elasticsearch.index.codec.postings.ES814InlinePostingsFormat.POSTINGS_BLOCK_SIZE;
import static org.elasticsearch.index.codec.postings.ES814InlinePostingsFormat.PROXIMITY_CODEC;
import static org.elasticsearch.index.codec.postings.ES814InlinePostingsFormat.PROXIMITY_EXTENSION;
import static org.elasticsearch.index.codec.postings.ES814InlinePostingsFormat.TERM_INDEX_BLOCK_SIZE;
import static org.elasticsearch.index.codec.postings.ES814InlinePostingsFormat.TERM_INDEX_CODEC;
import static org.elasticsearch.index.codec.postings.ES814InlinePostingsFormat.TERM_INDEX_EXTENSION;
import static org.elasticsearch.index.codec.postings.ES814InlinePostingsFormat.VERSION_CURRENT;
import static org.elasticsearch.index.codec.postings.ES814InlinePostingsFormat.VERSION_START;

final class ES814InlineFieldsProducer extends FieldsProducer {

    private record Meta(
        IndexOptions options,
        boolean hasPayloads,
        long indexOffset,
        long proxOffset,
        long size,
        long numBlocks,
        int docCount,
        long sumDocFreq,
        long sumTotalTermFreq,
        int maxTermLength,
        int packedIntVersion,
        TermIndexMeta termIndexMeta
    ) {}

    private record TermIndexMeta(long entries, long termIndexOffset, long blockAddress) {}

    private final IndexInput index, termIndex, prox;
    private final SegmentReadState state;
    private final Map<String, Meta> meta;

    ES814InlineFieldsProducer(SegmentReadState state) throws IOException {
        String metaFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, META_EXTENSION);
        String indexFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, INVERTED_INDEX_EXTENSION);
        String termIndexFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, TERM_INDEX_EXTENSION);
        String proxFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, PROXIMITY_EXTENSION);
        boolean success = false;
        try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName, IOContext.READONCE)) {
            index = state.directory.openInput(indexFileName, state.context);
            CodecUtil.checkIndexHeader(
                index,
                INVERTED_INDEX_CODEC,
                VERSION_START,
                VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            CodecUtil.retrieveChecksum(index);

            termIndex = state.directory.openInput(termIndexFileName, state.context);
            CodecUtil.checkIndexHeader(
                termIndex,
                TERM_INDEX_CODEC,
                VERSION_START,
                VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            CodecUtil.retrieveChecksum(termIndex);

            if (state.fieldInfos.hasProx()) {
                prox = state.directory.openInput(proxFileName, state.context);
                CodecUtil.checkIndexHeader(
                    prox,
                    PROXIMITY_CODEC,
                    VERSION_START,
                    VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                CodecUtil.retrieveChecksum(prox);
            } else {
                prox = null;
            }

            Throwable priorE = null;
            try {
                CodecUtil.checkIndexHeader(
                    meta,
                    META_CODEC,
                    VERSION_START,
                    VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                this.meta = new LinkedHashMap<>();
                for (byte flag = meta.readByte(); flag != -1; flag = meta.readByte()) {
                    IndexOptions opts = switch (flag) {
                        case 0 -> IndexOptions.DOCS;
                        case 1 -> IndexOptions.DOCS_AND_FREQS;
                        case 2 -> IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
                        case 3 -> IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
                        default -> throw new CorruptIndexException("Unexpected flag: " + flag, meta);
                    };
                    String fieldName = meta.readString();
                    long indexOffset = meta.readLong();
                    long proxOffset = -1L;
                    if (opts.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
                        proxOffset = meta.readLong();
                    }
                    long size = meta.readLong();
                    long numBlocks = meta.readLong();
                    assert size >= numBlocks : size + " < " + numBlocks;
                    int docCount = meta.readInt();
                    long sumDocFreq = meta.readLong();
                    long sumTotalTermFreq = sumDocFreq;
                    if (opts.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0) {
                        sumTotalTermFreq = meta.readLong();
                    }
                    int maxTermLength = meta.readInt();
                    int hasPayloads = meta.readByte();
                    if (hasPayloads < 0 || hasPayloads > 1) {
                        throw new CorruptIndexException("Unexpected boolean " + hasPayloads, meta);
                    }
                    int packedIntVersion = meta.readVInt();
                    var termIndexMeta = new TermIndexMeta(meta.readLong(), meta.readLong(), meta.readLong());
                    this.meta.put(
                        fieldName,
                        new Meta(
                            opts,
                            hasPayloads != 0,
                            indexOffset,
                            proxOffset,
                            size,
                            numBlocks,
                            docCount,
                            sumDocFreq,
                            sumTotalTermFreq,
                            maxTermLength,
                            packedIntVersion,
                            termIndexMeta
                        )
                    );
                }
            } catch (Throwable e) {
                priorE = e;
                throw e;
            } finally {
                CodecUtil.checkFooter(meta, priorE);
            }
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
        this.state = state;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(index, termIndex, prox);
    }

    @Override
    public void checkIntegrity() throws IOException {
        CodecUtil.checksumEntireFile(index);
        if (prox != null) {
            CodecUtil.checksumEntireFile(prox);
        }
    }

    @Override
    public Iterator<String> iterator() {
        return meta.keySet().iterator();
    }

    @Override
    public int size() {
        return meta.size();
    }

    @Override
    public Terms terms(String field) throws IOException {
        Meta meta = this.meta.get(field);
        if (meta == null) {
            return null;
        }
        return new InlineTerms(meta);
    }

    private class InlineTerms extends Terms {

        private final Meta meta;

        InlineTerms(Meta meta) {
            this.meta = meta;
        }

        @Override
        public TermsEnum iterator() throws IOException {
            return new InlineTermsEnum(meta);
        }

        @Override
        public long size() throws IOException {
            return meta.size;
        }

        @Override
        public long getSumTotalTermFreq() throws IOException {
            return meta.sumTotalTermFreq;
        }

        @Override
        public long getSumDocFreq() throws IOException {
            return meta.sumDocFreq;
        }

        @Override
        public int getDocCount() throws IOException {
            return meta.docCount;
        }

        @Override
        public boolean hasFreqs() {
            return meta.options.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
        }

        @Override
        public boolean hasOffsets() {
            return meta.options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
        }

        @Override
        public boolean hasPositions() {
            return meta.options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
        }

        @Override
        public boolean hasPayloads() {
            return meta.hasPayloads;
        }
    }

    private class InlineTermsEnum extends BaseTermsEnum {

        private final IndexInput index = ES814InlineFieldsProducer.this.index.clone();
        private final long termIndexOffsetStart;
        private final MonotonicBlockPackedReader termIndexOffsets;

        private final long blockStartAddress;
        private final MonotonicBlockPackedReader blockAddresses;

        private final Meta meta;
        private final BytesRefBuilder term;
        private final ES814InlinePostingsFormat.InlineTermState state = new ES814InlinePostingsFormat.InlineTermState();

        // term buffers for zstd
        private byte[] zCompressedTerms = new byte[1024];
        private byte[] zDecompressedTerms = new byte[1024];
        private long loadedFrameIndex = -1;
        private ByteArrayDataInput termsReader;

        InlineTermsEnum(Meta meta) throws IOException {
            this.meta = meta;
            this.term = new BytesRefBuilder();
            this.term.grow(meta.maxTermLength);

            IndexInput offsetInput = termIndex.clone();
            offsetInput.seek(meta.termIndexMeta.termIndexOffset);
            termIndexOffsetStart = offsetInput.readLong();
            termIndexOffsets = MonotonicBlockPackedReader.of(
                offsetInput,
                meta.packedIntVersion,
                TERM_INDEX_BLOCK_SIZE,
                meta.termIndexMeta.entries
            );

            IndexInput addressInput = termIndex.clone();
            addressInput.seek(meta.termIndexMeta.blockAddress);
            blockStartAddress = addressInput.readLong();
            blockAddresses = MonotonicBlockPackedReader.of(
                addressInput,
                meta.packedIntVersion,
                TERM_INDEX_BLOCK_SIZE,
                meta.termIndexMeta.entries
            );
            reset();
        }

        private void reset() throws IOException {
            index.seek(meta.indexOffset);
            state.blockIndex = -1;
            state.termIndexInBlock = 0;
            state.numTermsInBlock = -1;
            state.postingsFP = 0;
            state.postingsBytes = 0;
        }

        private long findBlockIndex(BytesRef target) throws IOException {
            long lo = 0;
            long hi = termIndexOffsets.size() - 2;
            while (hi >= lo) {
                long mid = (lo + hi) >>> 1;
                long offset = termIndexOffsets.get(mid);
                int length = Math.toIntExact(termIndexOffsets.get(mid + 1) - offset);
                termIndex.seek(termIndexOffsetStart + offset);
                term.setLength(length);
                termIndex.readBytes(term.bytes(), 0, length);
                int cmp = target.compareTo(term.get());
                if (cmp == 0) {
                    return mid;
                } else if (cmp < 0) {
                    hi = mid - 1;
                } else {
                    lo = mid + 1;
                }
            }
            return hi + 1;
        }

        @Override
        public SeekStatus seekCeil(BytesRef target) throws IOException {
            final int blockIndex = Math.toIntExact(findBlockIndex(target));
            if (blockIndex == meta.numBlocks) {
                return SeekStatus.END;
            }
            state.blockIndex = blockIndex;
            if (state.blockIndex == loadedFrameIndex) {
                resetFrame();
            } else {
                index.seek(blockStartAddress + blockAddresses.get(state.blockIndex));
                loadFrame();
            }
            // TODO: should we support binary search here?
            int cmp;
            do {
                scanNextTermInCurrentFrame();
            } while ((cmp = term.get().compareTo(target)) < 0 && state.termIndexInBlock < state.numTermsInBlock);

            if (cmp == 0) {
                return SeekStatus.FOUND;
            } else if (cmp > 0) {
                return SeekStatus.NOT_FOUND;
            } else {
                return SeekStatus.END;
            }
        }

        @Override
        public void seekExact(long ord) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public TermState termState() throws IOException {
            return state.clone();
        }

        @Override
        public void seekExact(BytesRef text, TermState state) throws IOException {
            var other = (ES814InlinePostingsFormat.InlineTermState) state;
            if (this.state.blockIndex != other.blockIndex || this.state.termIndexInBlock != other.termIndexInBlock) {
                loadedFrameIndex = -1;
            }
            this.state.copyFrom(other);
            index.seek(this.state.docOffset);
            term.copyBytes(text);
        }

        @Override
        public BytesRef term() throws IOException {
            return term.get();
        }

        @Override
        public long ord() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int docFreq() throws IOException {
            return state.docFreq;
        }

        @Override
        public long totalTermFreq() throws IOException {
            return state.totalTermFreq;
        }

        @Override
        public ImpactsEnum impacts(int flags) throws IOException {
            return new SlowImpactsEnum(postings(null, flags));
        }

        @Override
        public BytesRef next() throws IOException {
            if (state.termIndexInBlock >= state.numTermsInBlock) {
                if (++state.blockIndex >= meta.numBlocks) {
                    return null; // exhausted
                }
                if (state.blockIndex > 0) {
                    index.seek(state.postingsFP + state.postingsBytes);
                }
                loadFrame();
            } else if (state.blockIndex != loadedFrameIndex) {
                long termIndexInBlock = state.termIndexInBlock;
                index.seek(blockStartAddress + blockAddresses.get(state.blockIndex));
                loadFrame();
                while (termIndexInBlock-- > 0) {
                    scanNextTermInCurrentFrame();
                }
            }
            scanNextTermInCurrentFrame();
            return term.get();
        }

        private void decompressTerms(int compressedBytes, int originalBytes) throws IOException {
            zCompressedTerms = ArrayUtil.growNoCopy(zCompressedTerms, compressedBytes);
            zDecompressedTerms = ArrayUtil.growNoCopy(zDecompressedTerms, originalBytes);
            index.readBytes(zCompressedTerms, 0, compressedBytes);
            Zstd.decompress(zDecompressedTerms, originalBytes, zCompressedTerms, compressedBytes);
            termsReader = new ByteArrayDataInput(zDecompressedTerms, 0, originalBytes);
        }

        private void loadFrame() throws IOException {
            state.termIndexInBlock = 0;
            state.numTermsInBlock = index.readVInt();
            final long originalTermsBytes = index.readVLong();
            final long termBytes = index.readVLong();
            state.postingsBytes = index.readVLong();
            long termsFP = index.getFilePointer();
            state.postingsFP = termsFP + termBytes;
            decompressTerms((int) termBytes, (int) originalTermsBytes);
            loadedFrameIndex = state.blockIndex;
        }

        private void resetFrame() throws IOException {
            assert loadedFrameIndex == state.blockIndex : loadedFrameIndex + " != " + state.blockIndex;
            state.termIndexInBlock = 0;
            termsReader.setPosition(0);
            index.seek(state.postingsFP);
        }

        private void scanNextTermInCurrentFrame() throws IOException {
            assert loadedFrameIndex == state.blockIndex : loadedFrameIndex + " != " + state.blockIndex;
            term.setLength(termsReader.readVInt());
            termsReader.readBytes(term.bytes(), 0, term.length());
            state.docFreq = termsReader.readVInt();

            if (meta.options.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0) {
                state.totalTermFreq = termsReader.readVLong();
            } else {
                state.totalTermFreq = state.docFreq;
            }
            if (meta.options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
                state.proxOffset = termsReader.readLong();
            }
            state.docOffset = state.postingsFP + termsReader.readLong();
            state.termIndexInBlock++;
        }

        @Override
        public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
            if (flags == PostingsEnum.NONE) {
                InlineDocsEnum docs;
                if (reuse != null
                    && reuse instanceof InlineDocsEnum inlineDocsEnum
                    && inlineDocsEnum.canReuse(ES814InlineFieldsProducer.this)) {
                    docs = inlineDocsEnum;
                } else {
                    docs = new InlineDocsEnum(ES814InlineFieldsProducer.this, index.clone());
                }
                docs.reset(meta.options, meta.hasPayloads, state.docFreq, state.docOffset);
                return docs;
            }
            InlinePostingsEnum postings;
            if (reuse != null
                && reuse instanceof InlinePostingsEnum inlinePostingsEnum
                && inlinePostingsEnum.canReuse(ES814InlineFieldsProducer.this, flags)) {
                postings = inlinePostingsEnum;
            } else {
                IndexInput prox = null;
                if (ES814InlineFieldsProducer.this.prox != null) {
                    prox = ES814InlineFieldsProducer.this.prox.clone();
                }
                postings = new InlinePostingsEnum(ES814InlineFieldsProducer.this, flags, index.clone(), prox);
            }
            postings.reset(meta.options, meta.hasPayloads, state.docFreq, state.docOffset, state.proxOffset);
            return postings;
        }
    }

    private static class InlineDocsEnum extends PostingsEnum {

        private final ES814InlineFieldsProducer producer;
        private final int maxDoc;
        private IndexOptions options;
        private boolean hasPayloads;
        private int docFreq;
        private final IndexInput index;
        private int docIndex;
        private int doc;
        private final long[] docBuffer = new long[POSTINGS_BLOCK_SIZE];
        private int docBufferIndex;
        private int skipDoc; // last doc in the current block
        private final PForUtil2 pforUtil = new PForUtil2(new ForUtil());

        InlineDocsEnum(ES814InlineFieldsProducer producer, IndexInput index) {
            this.producer = Objects.requireNonNull(producer);
            this.maxDoc = producer.state.segmentInfo.maxDoc();
            this.index = Objects.requireNonNull(index);
        }

        boolean canReuse(ES814InlineFieldsProducer producer) {
            return this.producer == producer;
        }

        void reset(IndexOptions options, boolean hasPayloads, int docFreq, long docOffset) throws IOException {
            this.options = Objects.requireNonNull(options);
            this.hasPayloads = hasPayloads;
            this.docFreq = docFreq;
            docIndex = -1;
            docBufferIndex = POSTINGS_BLOCK_SIZE - 1; // Trigger a refill on the next read
            doc = -1;
            skipDoc = -1;
            index.seek(docOffset);
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() throws IOException {
            if (++docIndex >= docFreq) {
                return doc = DocIdSetIterator.NO_MORE_DOCS;
            }
            if (++docBufferIndex == POSTINGS_BLOCK_SIZE) {
                refillDocs(0);
                docBufferIndex = 0;
            }
            return doc = (int) docBuffer[docBufferIndex];
        }

        @Override
        public int advance(int target) throws IOException {
            if (target > skipDoc) { // Target is not in the current block
                // Move to the boundary between this block and the next one
                docIndex = docIndex - docBufferIndex + POSTINGS_BLOCK_SIZE;
                docBufferIndex = 0;
                while (true) {
                    refillDocs(target);
                    if (target <= skipDoc) { // Just found the target block, or last block
                        break;
                    } else {
                        docIndex += POSTINGS_BLOCK_SIZE;
                    }
                }
                doc = (int) docBuffer[docBufferIndex];
                if (doc >= target) {
                    return doc;
                }
            }
            // This is essentially an inlined version of slowAdvance(target).
            for (int i = docBufferIndex + 1; i < POSTINGS_BLOCK_SIZE; ++i) {
                if (docBuffer[i] >= target) {
                    docIndex += i - docBufferIndex;
                    docBufferIndex = i;
                    return doc = (int) docBuffer[i];
                }
            }
            throw new AssertionError();
        }

        /**
         * Refill blocks of documents, or skip data if none of the documents in the next block may be greater than or equal
         * to {@code target}.
         */
        private void refillDocs(int target) throws IOException {
            final int remaining = docFreq - docIndex;
            if (remaining >= POSTINGS_BLOCK_SIZE) {
                // Full block
                final int lastDocInPrevBlock = skipDoc;
                skipDoc += index.readVInt() + POSTINGS_BLOCK_SIZE;
                if (options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
                    index.readVLong(); // numPositionsInBlock
                    index.readVLong(); // nextBlockProxOffset delta
                }
                if (skipDoc >= target) {
                    pforUtil.decodeAndPrefixSum(index, lastDocInPrevBlock, docBuffer);
                    docBuffer[ForUtil.BLOCK_SIZE] = skipDoc;
                } else {
                    // Skip block
                    pforUtil.skip(index);
                }
                if (options.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0) {
                    pforUtil.skip(index);
                    index.readVLong(); // last freq of the block
                }
            } else {
                // Tail postings
                if (options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
                    index.readVLong(); // numPositionsInBlock
                }
                BitStreamInput bitIn = new BitStreamInput(index);
                BinaryInterpolativeCoding.decodeIncreasing(bitIn, docBuffer, 0, remaining - 1, skipDoc + 1, maxDoc - 1);
                // skip freqs, if any
                bitIn.done();
                // Add NO_MORE_DOCS to the docBuffer so that advance() can just scan until it finds a doc >= target
                docBuffer[remaining] = skipDoc = NO_MORE_DOCS;
            }
        }

        @Override
        public int freq() throws IOException {
            return 1;
        }

        @Override
        public int nextPosition() throws IOException {
            return -1;
        }

        @Override
        public int startOffset() throws IOException {
            return -1;
        }

        @Override
        public int endOffset() throws IOException {
            return -1;
        }

        @Override
        public BytesRef getPayload() throws IOException {
            return null;
        }

        @Override
        public long cost() {
            return docFreq;
        }
    }

    private static class InlinePostingsEnum extends PostingsEnum {

        private final ES814InlineFieldsProducer producer;
        private final int maxDoc;
        private final int flags;
        private IndexOptions options;
        private boolean hasPayloads;
        private int docFreq;
        private final IndexInput index, prox;
        private int docIndex;
        private int posIndex;
        private int doc;
        private int freq;
        private int pos;
        private int startOffset, endOffset;
        private BytesRef payload;
        private int payloadOffset;
        private final long[] docBuffer = new long[POSTINGS_BLOCK_SIZE];
        private final long[] freqBuffer = new long[POSTINGS_BLOCK_SIZE];
        private final long[] posBuffer = new long[ForUtil.BLOCK_SIZE];
        private final long[] offsetBuffer = new long[ForUtil.BLOCK_SIZE];
        private final long[] lengthBuffer = new long[ForUtil.BLOCK_SIZE];
        private final long[] payloadLengthBuffer = new long[ForUtil.BLOCK_SIZE];
        private byte[] payloadBytesBuffer = BytesRef.EMPTY_BYTES;
        private long posIndexInBlock;
        private int docBufferIndex;
        private int skipDoc; // last doc in the current block
        private long numPositionsInBlock;
        private long nextBlockProxOffset; // start offset of proximity data for the next block
        private final PForUtil2 pforUtil = new PForUtil2(new ForUtil());
        private long skippedPositions;

        InlinePostingsEnum(ES814InlineFieldsProducer producer, int flags, IndexInput index, IndexInput prox) {
            this.producer = Objects.requireNonNull(producer);
            this.maxDoc = producer.state.segmentInfo.maxDoc();
            this.flags = flags;
            this.index = Objects.requireNonNull(index);
            this.prox = prox;
        }

        boolean canReuse(ES814InlineFieldsProducer producer, int flags) {
            // NOTE: tests assume it's illegal to reuse an enum that was constructed with different flags
            return this.producer == producer && this.flags == flags;
        }

        void reset(IndexOptions options, boolean hasPayloads, int docFreq, long docOffset, long proxOffset) throws IOException {
            this.options = Objects.requireNonNull(options);
            this.hasPayloads = hasPayloads;
            this.docFreq = docFreq;
            docIndex = -1;
            docBufferIndex = POSTINGS_BLOCK_SIZE - 1; // Trigger a refill on the next read
            doc = -1;
            skipDoc = -1;
            freq = 1;
            posIndex = 1;
            index.seek(docOffset);
            if (options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
                prox.seek(proxOffset);
                nextBlockProxOffset = proxOffset;
            }
            pos = -1;
            startOffset = -1;
            endOffset = -1;
            payload = null;
            payloadOffset = 0;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() throws IOException {
            if (options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
                skippedPositions += freq - 1 - posIndex;
            }

            if (++docIndex >= docFreq) {
                return doc = DocIdSetIterator.NO_MORE_DOCS;
            }
            if (++docBufferIndex == POSTINGS_BLOCK_SIZE) {
                refillDocs(0);
                docBufferIndex = 0;
            }
            freq = (int) freqBuffer[docBufferIndex];
            resetProxData();
            return doc = (int) docBuffer[docBufferIndex];
        }

        @Override
        public int advance(int target) throws IOException {
            if (target > skipDoc) { // Target is not in the current block
                // Move to the boundary between this block and the next one
                docIndex = docIndex - docBufferIndex + POSTINGS_BLOCK_SIZE;
                docBufferIndex = 0;
                while (true) {
                    long proxOffset = nextBlockProxOffset;
                    refillDocs(target);
                    if (target <= skipDoc) { // Just found the target block, or last block
                        if (options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
                            prox.seek(proxOffset);
                        }
                        break;
                    } else {
                        docIndex += POSTINGS_BLOCK_SIZE;
                    }
                }
                freq = (int) freqBuffer[docBufferIndex];
                doc = (int) docBuffer[docBufferIndex];
                resetProxData();
                if (doc >= target) {
                    return doc;
                }
            }
            return slowAdvance(target);
        }

        private void resetProxData() {
            if (options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
                posIndex = -1;
                pos = 0;
                if (options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0) {
                    endOffset = 0;
                }
            }
        }

        /**
         * Refill blocks of documents, or skip data if none of the documents in the next block may be greater than or equal
         * to {@code target}.
         */
        private void refillDocs(int target) throws IOException {
            if (options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
                // Don't let the cursor on prox data lag behind
                prox.seek(nextBlockProxOffset); // effectively the current block at this point
                skippedPositions = 0;
                posIndexInBlock = 0;
            }

            final int remaining = docFreq - docIndex;
            if (remaining >= POSTINGS_BLOCK_SIZE) {
                // Full block
                final int lastDocInPrevBlock = skipDoc;
                skipDoc += index.readVInt() + POSTINGS_BLOCK_SIZE;
                if (options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
                    numPositionsInBlock = index.readVLong();
                    nextBlockProxOffset += index.readVLong();
                }
                if (skipDoc >= target) {
                    pforUtil.decodeAndPrefixSum(index, lastDocInPrevBlock, docBuffer);
                    docBuffer[ForUtil.BLOCK_SIZE] = skipDoc;
                    if (options.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0) {
                        pforUtil.decode(index, freqBuffer);
                        for (int i = 0; i < ForUtil.BLOCK_SIZE; ++i) {
                            freqBuffer[i] += 1L;
                        }
                        freqBuffer[ForUtil.BLOCK_SIZE] = 1L + index.readVLong();
                    } else {
                        Arrays.fill(freqBuffer, 1L);
                    }
                } else {
                    // Skip block
                    pforUtil.skip(index);
                    if (options.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0) {
                        pforUtil.skip(index);
                        index.readVLong();
                    }
                }
            } else {
                // Tail postings
                if (options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
                    numPositionsInBlock = index.readVLong();
                }
                BitStreamInput bitIn = new BitStreamInput(index);
                BinaryInterpolativeCoding.decodeIncreasing(bitIn, docBuffer, 0, remaining - 1, skipDoc + 1, maxDoc - 1);
                if (options.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0) {
                    for (int i = 0; i < remaining; ++i) {
                        freqBuffer[i] = bitIn.readDeltaCode();
                    }
                } else {
                    Arrays.fill(freqBuffer, 0, remaining, 1L);
                }
                bitIn.done();
                skipDoc = NO_MORE_DOCS;
                nextBlockProxOffset = -1L;
            }
        }

        private void refillPositions() throws IOException {
            final long remaining = numPositionsInBlock - posIndexInBlock;
            if (remaining >= ForUtil.BLOCK_SIZE) {
                pforUtil.decode(prox, posBuffer);
                if (options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0) {
                    pforUtil.decode(prox, offsetBuffer);
                    pforUtil.decode(prox, lengthBuffer);
                }
                if (hasPayloads) {
                    pforUtil.decode(prox, payloadLengthBuffer);
                    for (int i = 0; i < ForUtil.BLOCK_SIZE; ++i) {
                        payloadLengthBuffer[i] -= 1;
                    }
                }
            } else {
                // Tail block
                BitStreamInput bitIn = new BitStreamInput(prox);
                for (int i = 0; i < remaining; ++i) {
                    posBuffer[i] = bitIn.readDeltaCode() - 1;
                }
                if (options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0) {
                    for (int i = 0; i < remaining; ++i) {
                        offsetBuffer[i] = bitIn.readDeltaCode() - 1;
                        lengthBuffer[i] = bitIn.readDeltaCode() - 1;
                    }
                }
                if (hasPayloads) {
                    for (int i = 0; i < remaining; ++i) {
                        payloadLengthBuffer[i] = bitIn.readDeltaCode() - 2;
                    }
                }
                bitIn.done();
            }
            if (hasPayloads) {
                int blockSize = (int) Math.min(ForUtil.BLOCK_SIZE, remaining);
                long lengthSum = 0;
                for (int i = 0; i < blockSize; ++i) {
                    lengthSum += Math.max(0, payloadLengthBuffer[i]);
                }
                payloadBytesBuffer = ArrayUtil.growNoCopy(payloadBytesBuffer, Math.toIntExact(lengthSum));
                prox.readBytes(payloadBytesBuffer, 0, Math.toIntExact(lengthSum));
                payloadOffset = 0;
            }
        }

        @Override
        public int freq() throws IOException {
            return freq;
        }

        @Override
        public int nextPosition() throws IOException {
            if (options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
                assert pos == -1;
                return -1;
            }
            if (skippedPositions > 0) {
                skipPositions(skippedPositions);
                skippedPositions = 0;
            }
            if (++posIndex >= freq) {
                throw new IllegalStateException();
            }
            readPosition();
            return pos;
        }

        private void skipPositions(long skippedPositions) throws IOException {
            assert skippedPositions > 0;
            assert posIndex == -1;

            int p = (int) (posIndexInBlock & (ForUtil.BLOCK_SIZE - 1));
            if (p != 0) {
                // Read positions from the current block until either the end of the block, or skippedPositions have been skipped
                final int remainingPositionsInCurrentBlock = ForUtil.BLOCK_SIZE - p;
                final int toSkip = (int) Math.min(skippedPositions, remainingPositionsInCurrentBlock);
                skippedPositions -= toSkip;
                for (int i = 0; i < toSkip; ++i) {
                    readPosition();
                }
            }

            if (skippedPositions > 0) {
                assert (posIndexInBlock & (ForUtil.BLOCK_SIZE - 1)) == 0;

                while (skippedPositions >= ForUtil.BLOCK_SIZE) {
                    // We can skip an entire block of positions
                    pforUtil.skip(prox);
                    if (options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0) {
                        pforUtil.skip(prox);
                        pforUtil.skip(prox);
                    }
                    if (hasPayloads) {
                        pforUtil.decode(prox, payloadLengthBuffer);
                        long lengthSum = 0;
                        for (int i = 0; i < ForUtil.BLOCK_SIZE; ++i) {
                            lengthSum += Math.max(1, payloadLengthBuffer[i]) - 1;
                        }
                        prox.skipBytes(lengthSum);
                    }
                    posIndexInBlock += ForUtil.BLOCK_SIZE;
                    skippedPositions -= ForUtil.BLOCK_SIZE;
                }

                for (long i = 0; i < skippedPositions; ++i) {
                    readPosition();
                }
            }

            resetProxData();
        }

        private void readPosition() throws IOException {
            if (posIndexInBlock >= numPositionsInBlock) {
                throw new IllegalStateException();
            }
            int p = (int) (posIndexInBlock & (ForUtil.BLOCK_SIZE - 1));
            if (p == 0) {
                refillPositions();
            }
            pos += (int) posBuffer[p];
            if (options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0) {
                startOffset = endOffset + (int) offsetBuffer[p];
                endOffset = startOffset + (int) lengthBuffer[p];
            }
            if (hasPayloads) {
                int payloadLength = (int) payloadLengthBuffer[p];
                if (payloadLength == -1) {
                    payload = null;
                } else {
                    assert payloadLength >= 0;
                    payload = new BytesRef(payloadBytesBuffer, payloadOffset, payloadLength);
                    payloadOffset += payloadLength;
                }
            }
            ++posIndexInBlock;
        }

        @Override
        public int startOffset() throws IOException {
            return startOffset;
        }

        @Override
        public int endOffset() throws IOException {
            return endOffset;
        }

        @Override
        public BytesRef getPayload() throws IOException {
            return payload;
        }

        @Override
        public long cost() {
            return docFreq;
        }
    }
}
