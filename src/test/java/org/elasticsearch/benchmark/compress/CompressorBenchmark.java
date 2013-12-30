/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.benchmark.compress;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.util.Arrays;
import java.util.Collection;

public class CompressorBenchmark {

    static int DUMMY;

    public static class Stats {
        final long compress, uncompress;
        final float ratio;

        Stats(long compress, long uncompress, float ratio) {
            this.compress = compress;
            this.uncompress = uncompress;
            this.ratio = ratio;
        }

        public String toString() {
            return "{ ratio=" + ratio + ", compress=" + compress + ", uncompress=" + uncompress + "}";
        }
    }

    public static Stats bench(Compressor compressor, byte[] data, int offset, int len) throws Exception {
        long startCompress = System.nanoTime();
        final byte[] compressed = compressor.compress(data, offset, len);
        long endCompress = System.nanoTime();
        DUMMY = Arrays.hashCode(compressed);
        long startUncompress = System.nanoTime();
        final byte[] restored = compressor.uncompress(compressed, 0, compressed.length);
        long endUncompress = System.nanoTime();
        DUMMY = Arrays.hashCode(restored);
        return new Stats(endCompress - startCompress, endUncompress - startUncompress, (float) compressed.length / restored.length);
    }

    public static void main(String[] args) throws Exception {
        CompressorFactory.configure(ImmutableSettings.EMPTY);
        final BytesReference mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("designation")
                        .field("type", "string")
                        .field("index", "analyzed")
                        .field("boost", 10)
                    .startObject("description")
                        .field("type", "string")
                        .field("index", "analyzed")
                    .endObject()
                    .startObject("category")
                        .field("type", "string")
                        .field("index", "not_analyzed")
                        .field("omit_norms", "true")
                        .startObject("fielddata")
                            .field("format", "fst")
                        .endObject()
                    .endObject()
                    .startObject("brand")
                        .field("type", "string")
                        .field("index", "not_analyzed")
                        .field("omit_norms", "true")
                    .endObject()
                    .startObject("price")
                        .field("type", "double")
                    .endObject()
                    .startObject("ctr")
                        .field("type", "long")
                        .field("doc_values", true)
                        .field("index", "no")
                        .startObject("fielddata")
                            .field("format", "doc_values")
                        .endObject()
                    .endObject()
                .endObject().endObject().endObject().bytes();

        final Collection<Compressor> compressors = Arrays.<Compressor>asList(CompressorFactory.LZ4, CompressorFactory.LZF);

        // warm-up
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 10L * 1000) {
            for (Compressor compressor : compressors) {
                bench(compressor, mapping.array(), mapping.arrayOffset(), mapping.length());
            }
        }

        // run manual gc so that gc doesn't kick in during the bench
        System.gc();
        Thread.sleep(5 * 1000);

        // run another round to get the recycled buffers allocated
        for (Compressor compressor : compressors) {
            bench(compressor, mapping.array(), mapping.arrayOffset(), mapping.length());
        }

        // bench
        for (int i = 0; i < 20; ++i) {
            for (Compressor compressor : compressors) {
                System.out.println(compressor.type() + "\t" + bench(compressor, mapping.array(), mapping.arrayOffset(), mapping.length()));
            }
        }
    }

}
