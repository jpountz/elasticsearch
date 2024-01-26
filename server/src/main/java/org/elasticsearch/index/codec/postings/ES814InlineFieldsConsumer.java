/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.postings;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.packed.MonotonicBlockPackedWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.lucene.util.BinaryInterpolativeCoding;
import org.elasticsearch.lucene.util.BitStreamOutput;

import java.io.IOException;

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
import static org.elasticsearch.index.codec.postings.ES814InlinePostingsFormat.ZSTD_COMPRESSION_LEVEL;

final class ES814InlineFieldsConsumer extends FieldsConsumer {

    private final IndexOutput meta, index, termIndex, prox;
    private final SegmentWriteState state;

    ES814InlineFieldsConsumer(SegmentWriteState state) throws IOException {
        String metaFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, META_EXTENSION);
        String indexFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, INVERTED_INDEX_EXTENSION);
        String termIndexFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, TERM_INDEX_EXTENSION);
        String proxFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, PROXIMITY_EXTENSION);
        boolean success = false;
        try {
            meta = state.directory.createOutput(metaFileName, state.context);
            CodecUtil.writeIndexHeader(meta, META_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
            index = state.directory.createOutput(indexFileName, state.context);
            CodecUtil.writeIndexHeader(index, INVERTED_INDEX_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
            termIndex = state.directory.createOutput(termIndexFileName, state.context);
            CodecUtil.writeIndexHeader(termIndex, TERM_INDEX_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);

            if (state.fieldInfos.hasProx()) {
                prox = state.directory.createOutput(proxFileName, state.context);
                CodecUtil.writeIndexHeader(prox, PROXIMITY_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
            } else {
                prox = null;
            }
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
        this.state = state;
    }

    static final long MAX_BLOCK_BYTES = 1024 * 1024;
    static final int MAX_BLOCK_TERMS = 128;

    @Override
    public void write(Fields fields, NormsProducer norms) throws IOException {
        for (String field : fields) {
            Terms terms = fields.terms(field);
            final boolean hasFreqs = terms.hasFreqs();
            final boolean hasPositions = terms.hasPositions();
            final boolean hasOffsets = terms.hasOffsets();
            final boolean hasPayloads = terms.hasPayloads();

            if (hasOffsets) {
                meta.writeByte((byte) 3);
            } else if (hasPositions) {
                meta.writeByte((byte) 2);
            } else if (hasFreqs) {
                meta.writeByte((byte) 1);
            } else {
                meta.writeByte((byte) 0);
            }
            meta.writeString(field);
            meta.writeLong(index.getFilePointer());
            if (hasPositions) {
                meta.writeLong(prox.getFilePointer());
            }

            int flags = PostingsEnum.NONE;
            if (hasFreqs) {
                flags |= PostingsEnum.FREQS;
            }
            if (hasPositions) {
                flags |= PostingsEnum.POSITIONS;
            }
            if (hasOffsets) {
                flags |= PostingsEnum.OFFSETS;
            }

            ZstdDataOutput termsOut = new ZstdDataOutput();
            ByteBuffersDataOutput postingsOut = new ByteBuffersDataOutput();

            PostingsWriter writer = new PostingsWriter(hasFreqs, hasPositions, hasOffsets, hasPayloads);
            TermIndexWriter termIndexWriter = new TermIndexWriter(termIndex, index.getFilePointer());
            TermsEnum te = terms.iterator();
            PostingsEnum pe = null;
            long numBlocks = 0;
            long numTerms = 0;
            int maxTermLength = 0;
            int numPending = 0;
            BytesRefBuilder prevTerm = new BytesRefBuilder();
            long prevProxOffset = 0;
            for (BytesRef term = te.next(); term != null; term = te.next()) {
                pe = te.postings(pe, flags);
                if (pe.nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
                    continue;
                }
                long proxOffset = hasPositions ? prox.getFilePointer() : -1L;
                long postingsStartPointer = postingsOut.size();
                writer.write(pe, postingsOut);
                int prefix;
                if (term.length == 0) {
                    assert prevTerm.length() == 0;
                    prefix = 0;
                } else {
                    prefix = StringHelper.bytesDifference(prevTerm.get(), term);
                }
                termsOut.writeVInt(prefix);
                termsOut.writeVInt(term.length - prefix);
                termsOut.writeBytes(term.bytes, term.offset + prefix, term.length - prefix);
                termsOut.writeVInt(writer.docFreq);
                if (hasFreqs) {
                    termsOut.writeVLong(writer.totalTermFreq - writer.docFreq);
                }
                if (hasPositions) {
                    termsOut.writeVLong(proxOffset - prevProxOffset);
                    prevProxOffset = proxOffset;
                }
                termsOut.writeVLong(postingsOut.size() - postingsStartPointer);
                ++numPending;
                maxTermLength = Math.max(maxTermLength, term.length);
                ++numTerms;
                if (numPending >= MAX_BLOCK_TERMS || (termsOut.rawSize() + postingsOut.size()) >= MAX_BLOCK_BYTES) {
                    termIndexWriter.addTerm(term, index.getFilePointer());
                    flushBlock(numPending, termsOut, postingsOut);
                    numPending = 0;
                    ++numBlocks;
                    prevTerm.setLength(0);
                    prevProxOffset = 0;
                } else {
                    prevTerm.copyBytes(term);
                }
            }
            termIndexWriter.finish(index.getFilePointer());
            if (numPending > 0) {
                flushBlock(numPending, termsOut, postingsOut);
                ++numBlocks;
            }
            meta.writeLong(numTerms);
            meta.writeLong(numBlocks);
            meta.writeInt(writer.docsWithField.cardinality()); // docCount
            meta.writeLong(writer.sumDocFreq);
            if (hasFreqs) {
                meta.writeLong(writer.sumTotalTermFreq);
            }
            meta.writeInt(maxTermLength);
            meta.writeByte((byte) (writer.hasPayloads ? 1 : 0));
            // term index
            meta.writeVInt(PackedInts.VERSION_CURRENT);
            meta.writeLong(termIndexWriter.entries);
            meta.writeLong(termIndexWriter.termOffsetPointer);
            meta.writeLong(termIndexWriter.blockAddressPointer);
        }
        meta.writeByte((byte) -1); // no more fields
        CodecUtil.writeFooter(meta);
        CodecUtil.writeFooter(index);
        CodecUtil.writeFooter(termIndex);
        if (prox != null) {
            CodecUtil.writeFooter(prox);
        }
    }

    private void flushBlock(int numPending, ZstdDataOutput termsOut, ByteBuffersDataOutput postingsOut) throws IOException {
        index.writeVInt(numPending); // num terms
        index.writeVLong(termsOut.rawSize()); // original term size
        termsOut.compress();
        index.writeVLong(termsOut.compressedSize()); // compressed terms byte
        index.writeVLong(postingsOut.size()); // postings bytes
        termsOut.copyTo(index);
        postingsOut.copyTo(index);
        postingsOut.reset();
    }

    private class PostingsWriter {

        private final boolean hasFreqs, hasPositions, hasOffsets, hasPayloads;
        final FixedBitSet docsWithField = new FixedBitSet(state.segmentInfo.maxDoc());
        int docFreq;
        long sumDocFreq, totalTermFreq, sumTotalTermFreq;

        private final long[] docBuffer = new long[POSTINGS_BLOCK_SIZE];
        private final long[] freqBuffer = new long[POSTINGS_BLOCK_SIZE];
        private final long[] posBuffer = new long[ForUtil.BLOCK_SIZE];
        private final long[] offsetBuffer = new long[ForUtil.BLOCK_SIZE];
        private final long[] lengthBuffer = new long[ForUtil.BLOCK_SIZE];
        private final long[] payloadLengthBuffer = new long[ForUtil.BLOCK_SIZE];
        private final ByteBuffersDataOutput payloadsBuffer = new ByteBuffersDataOutput();
        private final PForUtil2 pforUtil = new PForUtil2(new ForUtil());
        private final ByteBuffersDataOutput spare = new ByteBuffersDataOutput();

        PostingsWriter(boolean hasFreqs, boolean hasPositions, boolean hasOffsets, boolean hasPayloads) {
            this.hasFreqs = hasFreqs;
            this.hasPositions = hasPositions;
            this.hasOffsets = hasOffsets;
            this.hasPayloads = hasPayloads;
        }

        void write(PostingsEnum pe, ByteBuffersDataOutput index) throws IOException {
            docFreq = 0;
            totalTermFreq = 0;
            int docBufferSize = 0;
            int posBufferSize = 0;
            int lastDocInPrevBlock = -1;
            long lastProxOffset = -1L;
            if (hasPositions) {
                lastProxOffset = prox.getFilePointer();
            }
            long lastBlockTTF = 0;
            for (int doc = pe.docID(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = pe.nextDoc()) {
                docsWithField.set(doc);
                ++docFreq;
                docBuffer[docBufferSize] = doc;
                int freq;
                if (hasFreqs) {
                    freq = pe.freq();
                } else {
                    freq = 1;
                }
                freqBuffer[docBufferSize] = freq;
                totalTermFreq += freq;
                if (hasPositions) {
                    int prevPosition = 0;
                    int prevOffset = 0;
                    for (int i = 0; i < freq; ++i) {
                        int pos = pe.nextPosition();
                        posBuffer[posBufferSize] = pos - prevPosition;
                        prevPosition = pos;
                        if (hasOffsets) {
                            offsetBuffer[posBufferSize] = pe.startOffset() - prevOffset;
                            lengthBuffer[posBufferSize] = pe.endOffset() - pe.startOffset();
                            prevOffset = pe.endOffset();
                        }
                        if (hasPayloads) {
                            BytesRef payload = pe.getPayload();
                            if (payload == null) {
                                payloadLengthBuffer[posBufferSize] = 0;
                            } else {
                                payloadLengthBuffer[posBufferSize] = 1 + payload.length;
                                payloadsBuffer.writeBytes(payload.bytes, payload.offset, payload.length);
                            }
                        }
                        if (++posBufferSize == ForUtil.BLOCK_SIZE) {
                            pforUtil.encode(posBuffer, prox, spare);
                            if (hasOffsets) {
                                pforUtil.encode(offsetBuffer, prox, spare);
                                pforUtil.encode(lengthBuffer, prox, spare);
                            }
                            if (hasPayloads) {
                                pforUtil.encode(payloadLengthBuffer, prox, spare);
                                payloadsBuffer.copyTo(prox);
                                payloadsBuffer.reset();
                            }
                            posBufferSize = 0;
                        }
                    }
                }

                if (++docBufferSize == POSTINGS_BLOCK_SIZE) {
                    // Write the last doc in the block first, which we can use as skip data, to know whether or not to decompress the block
                    index.writeVInt(doc - lastDocInPrevBlock - POSTINGS_BLOCK_SIZE);
                    if (hasPositions) {
                        writeTailProxData(posBufferSize);
                        index.writeVLong(totalTermFreq - lastBlockTTF); // number of positions in this block
                        lastBlockTTF = totalTermFreq;
                        index.writeVLong(prox.getFilePointer() - lastProxOffset);
                        lastProxOffset = prox.getFilePointer();
                        posBufferSize = 0;
                    }
                    // Delta-code postings
                    for (int i = ForUtil.BLOCK_SIZE - 1; i > 0; --i) {
                        docBuffer[i] = docBuffer[i] - docBuffer[i - 1] - 1;
                    }
                    docBuffer[0] = docBuffer[0] - lastDocInPrevBlock - 1;
                    pforUtil.encode(docBuffer, index, spare);
                    if (hasFreqs) {
                        for (int i = 0; i < ForUtil.BLOCK_SIZE; ++i) {
                            freqBuffer[i] -= 1;
                        }
                        pforUtil.encode(freqBuffer, index, spare);
                        index.writeVLong(freqBuffer[ForUtil.BLOCK_SIZE] - 1);
                    }
                    docBufferSize = 0;
                    lastDocInPrevBlock = doc;
                }
            }
            // Tail prox data
            if (hasPositions) {
                index.writeVLong(totalTermFreq - lastBlockTTF); // number of positions in this block
                writeTailProxData(posBufferSize);
            }
            // Tail postings
            BitStreamOutput bitOut = new BitStreamOutput(index);
            BinaryInterpolativeCoding.encodeIncreasing(
                docBuffer,
                0,
                docBufferSize - 1,
                lastDocInPrevBlock + 1,
                state.segmentInfo.maxDoc() - 1,
                bitOut
            );
            if (hasFreqs) {
                for (int i = 0; i < docBufferSize; ++i) {
                    bitOut.writeDeltaCode((int) freqBuffer[i]);
                }
            }
            bitOut.flush();
            sumDocFreq += docFreq;
            sumTotalTermFreq += totalTermFreq;
        }

        private void writeTailProxData(int bufferSize) throws IOException {
            assert hasPositions;
            BitStreamOutput bitOut = new BitStreamOutput(prox);
            for (int i = 0; i < bufferSize; ++i) {
                bitOut.writeDeltaCode((int) posBuffer[i] + 1);
            }
            if (hasOffsets) {
                for (int i = 0; i < bufferSize; ++i) {
                    bitOut.writeDeltaCode((int) offsetBuffer[i] + 1);
                    bitOut.writeDeltaCode((int) lengthBuffer[i] + 1);
                }
            }
            if (hasPayloads) {
                for (int i = 0; i < bufferSize; ++i) {
                    bitOut.writeDeltaCode((int) payloadLengthBuffer[i] + 1);
                }
            }
            bitOut.flush();
            if (hasPayloads) {
                payloadsBuffer.copyTo(prox);
                payloadsBuffer.reset();
            }
        }
    }

    // similar to LuceneFixedGap
    private static class TermIndexWriter {
        // offsets of the terms
        final long offsetStart;
        final ByteBuffersDataOutput offsetsBuffer = new ByteBuffersDataOutput();
        final MonotonicBlockPackedWriter offsets = new MonotonicBlockPackedWriter(offsetsBuffer, TERM_INDEX_BLOCK_SIZE);

        // block addresses
        final long blockStartAddress;
        final ByteBuffersDataOutput blockAddressBuffer = new ByteBuffersDataOutput();
        final MonotonicBlockPackedWriter blockAddresses = new MonotonicBlockPackedWriter(blockAddressBuffer, TERM_INDEX_BLOCK_SIZE);

        final IndexOutput out;

        long entries = 0;
        long termOffsetPointer = -1; // not available until finish
        long blockAddressPointer = -1; // not available until finish

        TermIndexWriter(IndexOutput out, long blockStartAddress) throws IOException {
            this.out = out;
            this.offsetStart = out.getFilePointer();
            this.blockStartAddress = blockStartAddress;
        }

        void addTerm(BytesRef term, long blockAddress) throws IOException {
            long offset = out.getFilePointer();
            offsets.add(offset - offsetStart);
            // TODO: write prefix only
            out.writeBytes(term.bytes, term.offset, term.length);
            blockAddresses.add(blockAddress - blockStartAddress);
            entries++;
        }

        void finish(long blockAddress) throws IOException {
            long offset = out.getFilePointer();
            offsets.add(offset - offsetStart);
            offsets.finish();
            this.termOffsetPointer = out.getFilePointer();
            out.writeLong(offsetStart);
            offsetsBuffer.copyTo(out);

            this.blockAddressPointer = out.getFilePointer();
            out.writeLong(blockStartAddress);
            blockAddresses.add(blockAddress - blockStartAddress);
            blockAddresses.finish();
            blockAddressBuffer.copyTo(out);
            entries++;
        }
    }

    static final class ZstdDataOutput extends DataOutput {
        private final BytesRefBuilder raw = new BytesRefBuilder();
        private final BytesRefBuilder compressed = new BytesRefBuilder();

        @Override
        public void writeByte(byte b) {
            raw.append(b);
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {
            raw.append(b, offset, length);
        }

        void compress() {
            compressed.setLength(0);
            Zstd.compress(raw, compressed, ZSTD_COMPRESSION_LEVEL);
            raw.setLength(0);
        }

        long rawSize() {
            return raw.length();
        }

        long compressedSize() {
            return compressed.length();
        }

        void copyTo(DataOutput out) throws IOException {
            out.writeBytes(compressed.bytes(), 0, compressed.length());
            compressed.setLength(0);
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(meta, index, termIndex, prox);
    }

}
