/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.util;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.mockfile.FilterFileChannel;
import org.apache.lucene.mockfile.FilterFileSystemProvider;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.common.lucene.store.FilterIndexOutput;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.spi.FileSystemProvider;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class StackTraceElementTaggerTests extends ESTestCase {

    private static class StackTraceElementMatcher extends BaseMatcher<StackTraceElement> {

        private final Matcher<String> tagMatcher;

        public StackTraceElementMatcher(Matcher<String> tagMatcher) {
            this.tagMatcher = tagMatcher;
        }

        @Override
        public boolean matches(Object item) {
            return tagMatcher.matches(StackTraceElementTagger.tag((StackTraceElement) item));
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("tag ").appendDescriptionOf(tagMatcher);
        }

    }

    // TODO: How can we test the same with MMAPDirectory?
    public void testReadBytesNIO() throws Exception {
        AtomicReference<RuntimeException> ex = new AtomicReference<>();

        FileSystem fs = FileSystems.getDefault();
        FileSystemProvider mockFS = new FilterFileSystemProvider("intercept://", fs) {
            @Override
            public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
                return new FilterFileChannel(super.newFileChannel(path, options, attrs)) {
                    @Override
                    public int read(ByteBuffer dst, long position) throws IOException {
                        ex.set(new RuntimeException());
                        return super.read(dst, position);
                    }
                };
            }
        };

        final Path path = createTempDir();
        Path dirPath = mockFS.getPath(path.toUri());
        try (Directory dir = new NIOFSDirectory(dirPath)) {
            try (IndexOutput out = dir.createOutput("foo", IOContext.DEFAULT)) {
                out.writeBytes(new byte[8], 8);
            }
            try (IndexInput in = dir.openInput("foo", IOContext.DEFAULT)) {
                ex.set(null);
                in.readByte();
                assertNotNull(ex.get());
                assertThat(ex.get().getStackTrace(), Matchers.hasItemInArray(
                        new StackTraceElementMatcher(Matchers.equalTo(StackTraceElementTagger.STORE_READ))));
            }
            try (IndexInput in = dir.openInput("foo", IOContext.DEFAULT)) {
                ex.set(null);
                in.readBytes(new byte[1], 0, 1);
                assertNotNull(ex.get());
                assertThat(ex.get().getStackTrace(), Matchers.hasItemInArray(
                        new StackTraceElementMatcher(Matchers.equalTo(StackTraceElementTagger.STORE_READ))));
            }
            try (IndexInput in = dir.openInput("foo", IOContext.DEFAULT)) {
                ex.set(null);
                in.readShort();
                assertNotNull(ex.get());
                assertThat(ex.get().getStackTrace(), Matchers.hasItemInArray(
                        new StackTraceElementMatcher(Matchers.equalTo(StackTraceElementTagger.STORE_READ))));
            }
            try (IndexInput in = dir.openInput("foo", IOContext.DEFAULT)) {
                ex.set(null);
                in.readInt();
                assertNotNull(ex.get());
                assertThat(ex.get().getStackTrace(), Matchers.hasItemInArray(
                        new StackTraceElementMatcher(Matchers.equalTo(StackTraceElementTagger.STORE_READ))));
            }
            try (IndexInput in = dir.openInput("foo", IOContext.DEFAULT)) {
                ex.set(null);
                in.readLong();
                assertNotNull(ex.get());
                assertThat(ex.get().getStackTrace(), Matchers.hasItemInArray(
                        new StackTraceElementMatcher(Matchers.equalTo(StackTraceElementTagger.STORE_READ))));
            }
        }
    }

    private static class InterceptingIndexInput extends IndexInput {

        private final IndexInput in;
        private final Runnable onRead;

        protected InterceptingIndexInput(IndexInput in, Runnable onRead) {
            super(in.toString());
            this.in = in;
            this.onRead = onRead;
        }

        @Override
        public IndexInput clone() {
            return new InterceptingIndexInput(in.clone(), onRead);
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            onRead.run();
            in.readBytes(b, offset, len);
        }

        @Override
        public byte readByte() throws IOException {
            onRead.run();
            return in.readByte();
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            return new InterceptingIndexInput(in.slice(sliceDescription, offset, length), onRead);
        }

        @Override
        public void seek(long pos) throws IOException {
            in.seek(pos);
        }

        @Override
        public long length() {
            return in.length();
        }

        @Override
        public long getFilePointer() {
            return in.getFilePointer();
        }

        @Override
        public void close() throws IOException {
            in.close();
        }

    }

    private static class InterceptingDirectory extends FilterDirectory {

        InterceptingDirectory(Directory in) {
            super(in);
        }

        protected void onRead() {}

        protected void onWrite() {}

        @Override
        public IndexInput openInput(String name, IOContext context)
                throws IOException {
            return new InterceptingIndexInput(super.openInput(name, context), this::onRead);
        }

        @Override
        public IndexOutput createOutput(String name, IOContext context) throws IOException {
            IndexOutput out = super.createOutput(name, context);
            return new FilterIndexOutput(out.toString(), out) {
                @Override
                public void writeByte(byte b) throws IOException {
                    onWrite();
                    super.writeByte(b);
                }

                @Override
                public void writeBytes(byte[] b, int offset, int length) throws IOException {
                    onWrite();
                    super.writeBytes(b, offset, length);
                }
            };
        }

    }

    public void testMerge() throws IOException {
        final AtomicBoolean hasMerged = new AtomicBoolean();
        try (Directory dir = new InterceptingDirectory(newDirectory()) {
            @Override
            protected void onWrite() {
                RuntimeException e = new RuntimeException();
                for (StackTraceElement ste : e.getStackTrace()) {
                    if (StackTraceElementTagger.SEGMENT_MERGE.equals(StackTraceElementTagger.tag(ste))) {
                        hasMerged.set(true);
                    }
                }
            }
        }; IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
            w.addDocument(new Document());
            DirectoryReader.open(w).close();
            w.addDocument(new Document());
            assertFalse(hasMerged.get());
            w.forceMerge(1);
            assertTrue(hasMerged.get());
        }
    }

    public void testFlush() throws IOException {
        final AtomicBoolean hasFlushed = new AtomicBoolean();
        try (Directory dir = new InterceptingDirectory(newDirectory()) {
            @Override
            protected void onWrite() {
                RuntimeException e = new RuntimeException();
                for (StackTraceElement ste : e.getStackTrace()) {
                    if (StackTraceElementTagger.SEGMENT_FLUSH.equals(StackTraceElementTagger.tag(ste))) {
                        hasFlushed.set(true);
                    }
                }
            }
        }; IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
            w.addDocument(new Document());
            assertFalse(hasFlushed.get());
            DirectoryReader.open(w).close();
            assertTrue(hasFlushed.get());
        }
    }

    public void testSearchTermQuery() throws IOException {
        final AtomicBoolean hasSought = new AtomicBoolean();
        final AtomicBoolean hasIterated = new AtomicBoolean();
        final AtomicBoolean hasSearched = new AtomicBoolean();
        try (Directory dir = new InterceptingDirectory(newDirectory()) {
            @Override
            protected void onRead() {
                RuntimeException e = new RuntimeException();
                for (StackTraceElement ste : e.getStackTrace()) {
                    String tag = StackTraceElementTagger.tag(ste);
                    if (StackTraceElementTagger.TERMS_SEEK.equals(tag)) {
                        hasSought.set(true);
                    } else if (StackTraceElementTagger.DISI_ITER.equals(tag)) {
                        hasIterated.set(true);
                    } else if (StackTraceElementTagger.INDEX_SEARCH.equals(tag)) {
                        hasSearched.set(true);
                    }
                }
            }
        }; IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
            Document doc = new Document();
            doc.add(new StringField("foo", "bar", Store.NO));
            w.addDocument(doc);
            w.addDocument(doc); // we need 2 matches otherwise postings are inlined in the terms dict
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                assertFalse(hasSearched.get());
                assertFalse(hasSought.get());
                assertFalse(hasIterated.get());
                new IndexSearcher(reader).search(new TermQuery(new Term("foo", "bar")), 10);
                assertTrue(hasSearched.get());
                assertTrue(hasSought.get());
                assertTrue(hasIterated.get());
            }
        }
    }

}
