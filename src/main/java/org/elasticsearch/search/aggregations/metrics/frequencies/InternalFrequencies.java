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

package org.elasticsearch.search.aggregations.metrics.frequencies;

import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public final class InternalFrequencies extends InternalNumericMetricsAggregation.MultiValue implements Frequencies {

    public final static Type TYPE = new Type("frequencies");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalFrequencies readResult(StreamInput in) throws IOException {
            InternalFrequencies result = new InternalFrequencies();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    private CountMinSketch frequencies;

    InternalFrequencies(String name, CountMinSketch frequencies, @Nullable ValueFormatter formatter, Map<String, Object> metaData) {
        super(name, metaData);
        this.frequencies = frequencies;
        this.valueFormatter = formatter;
    }

    private InternalFrequencies() {
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        valueFormatter = ValueFormatterStreams.readOptional(in);
        if (in.readBoolean()) {
            frequencies = CountMinSketch.readFrom(in, BigArrays.NON_RECYCLING_INSTANCE);
        } else {
            frequencies = null;
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        ValueFormatterStreams.writeOptional(valueFormatter, out);
        if (frequencies != null) {
            out.writeBoolean(true);
            frequencies.writeTo(0, out);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public InternalAggregation reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        InternalFrequencies reduced = null;
        for (InternalAggregation aggregation : aggregations) {
            final InternalFrequencies frequencies = (InternalFrequencies) aggregation;
            if (frequencies.frequencies != null) {
                if (reduced == null) {
                    reduced = new InternalFrequencies(name, new CountMinSketch(
                            frequencies.frequencies.d(),
                            frequencies.frequencies.lgW(),
                            frequencies.frequencies.lgMaxFreq(),
                            BigArrays.NON_RECYCLING_INSTANCE), this.valueFormatter, getMetaData());
                }
                reduced.merge(frequencies);
            }
        }

        if (reduced == null) { // all empty
            return aggregations.get(0);
        } else {
            return reduced;
        }
    }

    public void merge(InternalFrequencies other) {
        assert frequencies != null && other != null;
        frequencies.merge(0, other.frequencies, 0);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        final long cardinality = getValue();
        builder.field(CommonFields.VALUE, cardinality);
        if (valueFormatter != null) {
            builder.field(CommonFields.VALUE_AS_STRING, valueFormatter.format(cardinality));
        }
        return builder;
    }

}
