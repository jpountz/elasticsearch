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

package org.elasticsearch.search.aggregations.metrics.termcount;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregation;
import org.elasticsearch.search.aggregations.support.numeric.ValueFormatterStreams;

import java.io.IOException;
import java.util.List;

public final class InternalTermCount extends MetricsAggregation.SingleValue implements TermCount {

    public final static Type TYPE = new Type("term_count");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalTermCount readResult(StreamInput in) throws IOException {
            InternalTermCount result = new InternalTermCount();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    private HyperLogLogPlusPlus counts;

    InternalTermCount(String name, HyperLogLogPlusPlus counts) {
        super(name);
        this.counts = counts;
    }

    private InternalTermCount() {
    }

    @Override
    public double value() {
        return getValue();
    }

    @Override
    public long getValue() {
        return counts.cardinality(0);
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        valueFormatter = ValueFormatterStreams.readOptional(in);
        counts = HyperLogLogPlusPlus.readFrom(in, BigArrays.NON_RECYCLING_INSTANCE);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        ValueFormatterStreams.writeOptional(valueFormatter, out);
        counts.writeTo(0, out);
    }

    @Override
    public InternalAggregation reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        if (aggregations.size() == 1) {
            return (InternalTermCount) aggregations.get(0);
        }

        InternalTermCount reduced = null;
        for (InternalAggregation aggregation : aggregations) {
            final InternalTermCount termCount = (InternalTermCount) aggregation;
            if (reduced == null) {
                reduced = termCount;
            } else {
                reduced.merge(termCount);
            }
        }

        return reduced;
    }

    public void merge(InternalTermCount other) {
        counts.merge(other.counts, 0, 0);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        final long termCount = getValue();
        builder.field(CommonFields.VALUE, termCount);
        if (valueFormatter != null) {
            builder.field(CommonFields.VALUE_AS_STRING, valueFormatter.format(termCount));
        }
        builder.endObject();
        return builder;
    }

}
