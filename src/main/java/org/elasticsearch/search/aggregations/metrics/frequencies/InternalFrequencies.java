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
import java.util.AbstractList;
import java.util.Iterator;
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

    private long[] frequencies;
    private boolean keyed;
    private CountMinSketch sketch;

    InternalFrequencies(String name, long[] frequencies, CountMinSketch sketch, boolean keyed, @Nullable ValueFormatter formatter, Map<String, Object> metaData) {
        super(name, metaData);
        this.sketch = sketch;
        this.frequencies = frequencies;
        this.keyed = keyed;
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
        keyed = in.readBoolean();
        valueFormatter = ValueFormatterStreams.readOptional(in);
        frequencies = in.readLongArray();
        if (in.readBoolean()) {
            sketch = CountMinSketch.readFrom(in, BigArrays.NON_RECYCLING_INSTANCE);
        } else {
            sketch = null;
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeBoolean(keyed);
        ValueFormatterStreams.writeOptional(valueFormatter, out);
        out.writeLongArray(frequencies);
        if (sketch != null) {
            out.writeBoolean(true);
            sketch.writeTo(0, out);
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
            if (frequencies.sketch != null) {
                if (reduced == null) {
                    reduced = new InternalFrequencies(name, this.frequencies, new CountMinSketch(
                            frequencies.sketch.d(),
                            frequencies.sketch.lgW(),
                            frequencies.sketch.lgMaxFreq(),
                            BigArrays.NON_RECYCLING_INSTANCE), keyed, this.valueFormatter, getMetaData());
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
        assert sketch != null && other != null;
        sketch.merge(0, other.sketch, 0);
    }

    public long value(long frequency) {
        return cardinality(frequency);
    }

    @Override
    public double value(String name) {
        return value(Long.parseLong(name));
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(CommonFields.VALUES);
            for(int i = 0; i < frequencies.length; ++i) {
                String key = String.valueOf(frequencies[i]);
                long value = value(frequencies[i]);
                builder.field(key, value);
                if (valueFormatter != null) {
                    builder.field(key + "_as_string", valueFormatter.format(value));
                }
            }
            builder.endObject();
        } else {
            builder.startArray(CommonFields.VALUES);
            for (int i = 0; i < frequencies.length; i++) {
                double value = value(frequencies[i]);
                builder.startObject();
                builder.field(CommonFields.KEY, frequencies[i]);
                builder.field(CommonFields.VALUE, value);
                if (valueFormatter != null) {
                    builder.field(CommonFields.VALUE_AS_STRING, valueFormatter.format(value));
                }
                builder.endObject();
            }
            builder.endArray();
        }
        return builder;
    }

    @Override
    public Iterator<Frequency> iterator() {
        return new AbstractList<Frequency>() {

            @Override
            public Frequency get(final int index) {
                return new Frequency() {

                    @Override
                    public long getMinFrequency() {
                        return frequencies[index];
                    }

                    @Override
                    public long getCardinality() {
                        return cardinality(getMinFrequency());
                    }

                };
            }

            @Override
            public int size() {
                return frequencies.length;
            }

        }.iterator();
    }

    @Override
    public long cardinality(long minFrequency) {
        return sketch == null ? 0L : sketch.cardinality(0, minFrequency);
    }

    @Override
    public String cardinalityAsString(long minFrequency) {
        ValueFormatter valueFormatter = this.valueFormatter;
        if (valueFormatter == null) {
            valueFormatter = ValueFormatter.RAW;
        }
        return valueFormatter.format(cardinality(minFrequency));
    }

}
