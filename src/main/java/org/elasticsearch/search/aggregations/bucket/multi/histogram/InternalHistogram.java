/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.multi.histogram;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.context.numeric.ValueFormatter;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class InternalHistogram extends AbstractHistogramBase<Histogram.Bucket> implements Histogram {

    public final static Type TYPE = new Type("histogram", "histo");
    public final static Factory FACTORY = new Factory();

    private final static AggregationStreams.Stream<InternalHistogram> STREAM = new AggregationStreams.Stream<InternalHistogram>() {
        @Override
        public InternalHistogram readResult(StreamInput in) throws IOException {
            InternalHistogram histogram = new InternalHistogram();
            histogram.readFrom(in);
            return histogram;
        }
    };

    public static void registerStream() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    static class Bucket extends AbstractHistogramBase.Bucket implements Histogram.Bucket {

        Bucket(long key, long docCount, InternalAggregations aggregations) {
            super(key, docCount, aggregations);
        }

    }

    static class Factory implements AbstractHistogramBase.Factory<Histogram.Bucket> {

        private Factory() {
        }

        public AbstractHistogramBase create(String name, List<Histogram.Bucket> buckets, InternalOrder order, Rounding rounding, ValueFormatter formatter, boolean keyed) {
            return new InternalHistogram(name, buckets, order, rounding, formatter, keyed);
        }

        public Bucket createBucket(long key, long docCount, List<InternalAggregation> aggregations) {
            return new Bucket(key, docCount, new InternalAggregations(aggregations));
        }

    }

    public InternalHistogram() {} // for serialization

    public InternalHistogram(String name, List<Histogram.Bucket> buckets, InternalOrder order, Rounding rounding, ValueFormatter formatter, boolean keyed) {
        super(name, buckets, order, rounding, formatter, keyed);
    }

    @Override
    public Type type() {
        return TYPE;
    }

    protected Bucket createBucket(long key, long docCount, InternalAggregations aggregations) {
        return new Bucket(key, docCount, aggregations);
    }
}
