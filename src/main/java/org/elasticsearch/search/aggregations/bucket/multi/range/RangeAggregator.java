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

package org.elasticsearch.search.aggregations.bucket.multi.range;

import com.carrotsearch.hppc.IntArrayList;
import com.google.common.collect.Lists;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.OrdsAggregator;
import org.elasticsearch.search.aggregations.bucket.multi.MultiBucketAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.context.numeric.NumericValuesSource;
import org.elasticsearch.search.aggregations.context.numeric.ValueFormatter;
import org.elasticsearch.search.aggregations.context.numeric.ValueParser;
import org.elasticsearch.search.aggregations.factory.AggregatorFactories;
import org.elasticsearch.search.aggregations.factory.ValueSourceAggregatorFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
// nocommit range aggregations should use binary search to find the matching ranges
public class RangeAggregator extends MultiBucketAggregator {

    public static class Range {

        public String key;
        public double from = Double.NEGATIVE_INFINITY;
        String fromAsStr;
        public double to = Double.POSITIVE_INFINITY;
        String toAsStr;

        public Range(String key, double from, String fromAsStr, double to, String toAsStr) {
            this.key = key;
            this.from = from;
            this.fromAsStr = fromAsStr;
            this.to = to;
            this.toAsStr = toAsStr;
        }

        boolean matches(double value) {
            return value >= from && value < to;
        }

        @Override
        public String toString() {
            return "(" + from + " to " + to + "]";
        }

        public void process(ValueParser parser, AggregationContext aggregationContext) {
            if (fromAsStr != null) {
                from = parser != null ? parser.parseDouble(fromAsStr, aggregationContext.searchContext()) : Double.valueOf(fromAsStr);
            }
            if (toAsStr != null) {
                to = parser != null ? parser.parseDouble(toAsStr, aggregationContext.searchContext()) : Double.valueOf(toAsStr);
            }
        }
    }

    private final NumericValuesSource valuesSource;
    private final Range[] ranges;
    private final boolean keyed;
    private final AbstractRangeBase.Factory rangeFactory;

    private final BucketCollector[] bucketCollectors;

    public RangeAggregator(String name,
                           AggregatorFactories factories,
                           NumericValuesSource valuesSource,
                           AbstractRangeBase.Factory rangeFactory,
                           List<Range> ranges,
                           boolean keyed,
                           AggregationContext aggregationContext,
                           Aggregator parent) {

        super(name, factories, ranges.size(), aggregationContext, parent);
        this.valuesSource = valuesSource;
        this.keyed = keyed;
        this.rangeFactory = rangeFactory;
        bucketCollectors = new BucketCollector[ranges.size()];
        this.ranges = ranges.toArray(new Range[ranges.size()]);
        for (int i = 0; i < this.ranges.length; ++i) {
            final Range range = this.ranges[i];
            ValueParser parser = valuesSource != null ? valuesSource.parser() : null;
            range.process(parser, aggregationContext);
            bucketCollectors[i] = new BucketCollector(i, range, factories.createAggregators(this), ordsCollectors);
        }
    }

    @Override
    public Collector collector() {
        return valuesSource != null ? new Collector() : null;
    }

    @Override
    public InternalAggregation buildAggregation() {
        List<RangeBase.Bucket> buckets = Lists.newArrayListWithCapacity(bucketCollectors.length);
        for (int i = 0; i < bucketCollectors.length; i++) {
            Range range = bucketCollectors[i].range;
            RangeBase.Bucket bucket = rangeFactory.createBucket(range.key, range.from, range.to, bucketCollectors[i].docCount(),
                    bucketCollectors[i].buildAggregations(ordsAggregators), valuesSource.formatter());
            buckets.add(bucket);
        }

        // value source can be null in the case of unmapped fields
        ValueFormatter formatter = valuesSource != null ? valuesSource.formatter() : null;
        return rangeFactory.create(name, buckets, formatter, keyed);
    }

    class Collector implements Aggregator.Collector {

        final boolean[] matched;
        final IntArrayList matchedList;

        Collector() {
            matched = new boolean[ranges.length];
            matchedList = new IntArrayList();
        }

        @Override
        public void collect(int doc) throws IOException {
            final DoubleValues values = valuesSource.doubleValues();
            final int valuesCount = values.setDocument(doc);
            assert noMatchYet();
            for (int i = 0; i < valuesCount; ++i) {
                final double value = values.nextValue();
                collect(doc, value);
            }
            resetMatches();
        }

        private boolean noMatchYet() {
            for (int i = 0; i < ranges.length; ++i) {
                if (matched[i]) {
                    return false;
                }
            }
            return true;
        }

        private void resetMatches() {
            for (int i = 0; i < matchedList.size(); ++i) {
                matched[matchedList.get(i)] = false;
            }
            matchedList.clear();
        }

        private void collect(int doc, double value) throws IOException {
            for (int i = 0; i < ranges.length; ++i) {
                if (!matched[i] && ranges[i].matches(value)) {
                    matched[i] = true;
                    matchedList.add(i);
                    bucketCollectors[i].collect(doc);
                }
            }
        }

        @Override
        public void postCollection() {
            for (int i = 0; i < bucketCollectors.length; i++) {
                bucketCollectors[i].postCollection();
            }
        }
    }

    static class BucketCollector extends MultiBucketAggregator.BucketCollector {

        private final Range range;

        BucketCollector(int ord, Range range, Aggregator[] aggregators, OrdsAggregator.Collector[] ordsCollectors) {
            super(ord, aggregators, ordsCollectors);
            this.range = range;
        }
    }

    public static class Unmapped extends Aggregator {
        private final List<RangeAggregator.Range> ranges;
        private final boolean keyed;
        private final AbstractRangeBase.Factory factory;
        private final ValueFormatter formatter;
        private final ValueParser parser;

        public Unmapped(String name,
                        List<RangeAggregator.Range> ranges,
                        boolean keyed,
                        ValueFormatter formatter,
                        ValueParser parser,
                        AggregationContext aggregationContext,
                        Aggregator parent,
                        AbstractRangeBase.Factory factory) {

            super(name, AggregatorFactories.EMPTY, aggregationContext, parent);
            this.ranges = ranges;
            this.keyed = keyed;
            this.formatter = formatter;
            this.parser = parser;
            this.factory = factory;
        }

        @Override
        public Collector collector() {
            return null;
        }

        @Override
        public AbstractRangeBase buildAggregation() {
            List<RangeBase.Bucket> buckets = new ArrayList<RangeBase.Bucket>(ranges.size());
            for (RangeAggregator.Range range : ranges) {
                range.process(parser, context) ;
                buckets.add(factory.createBucket(range.key, range.from, range.to, 0, InternalAggregations.EMPTY, formatter));
            }
            return factory.create(name, buckets, formatter, keyed);
        }
    }

    public static class Factory extends ValueSourceAggregatorFactory.Normal<NumericValuesSource> {

        private final AbstractRangeBase.Factory rangeFactory;
        private final List<Range> ranges;
        private final boolean keyed;

        public Factory(String name, ValuesSourceConfig<NumericValuesSource> valueSourceConfig, AbstractRangeBase.Factory rangeFactory, List<Range> ranges, boolean keyed) {
            super(name, rangeFactory.type(), valueSourceConfig);
            this.rangeFactory = rangeFactory;
            this.ranges = ranges;
            this.keyed = keyed;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new Unmapped(name, ranges, keyed, valuesSourceConfig.formatter(), valuesSourceConfig.parser(), aggregationContext, parent, rangeFactory);
        }

        @Override
        protected Aggregator create(NumericValuesSource valuesSource, AggregationContext aggregationContext, Aggregator parent) {
            return new RangeAggregator(name, factories, valuesSource, rangeFactory, ranges, keyed, aggregationContext, parent);
        }
    }

}
