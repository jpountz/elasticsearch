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
import org.apache.lucene.util.InPlaceMergeSorter;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
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
public class RangeAggregator extends Aggregator {

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
            return "[" + from + " to " + to + ")";
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
    private final boolean keyed;
    private final AbstractRangeBase.Factory rangeFactory;

    private final BucketsCollector bucketsCollector;
    final boolean[] matched;
    final double[] maxTo;
    final IntArrayList matchedList;

    public RangeAggregator(String name,
                           AggregatorFactories factories,
                           NumericValuesSource valuesSource,
                           AbstractRangeBase.Factory rangeFactory,
                           List<Range> ranges,
                           boolean keyed,
                           AggregationContext aggregationContext,
                           Aggregator parent) {

        super(name, BucketAggregationMode.PER_BUCKET, factories, ranges.size(), aggregationContext, parent);
        this.valuesSource = valuesSource;
        this.keyed = keyed;
        this.rangeFactory = rangeFactory;

        bucketsCollector = new BucketsCollector(subAggregators, ranges.toArray(new Range[ranges.size()]));
        matched = new boolean[ranges.size()];
        maxTo = new double[matched.length];
        maxTo[0] = bucketsCollector.ranges[0].to;
        for (int i = 1; i < bucketsCollector.ranges.length; ++i) {
            maxTo[i] = Math.max(bucketsCollector.ranges[i].to,maxTo[i-1]);
        }
        matchedList = new IntArrayList();

    }

    @Override
    public boolean shouldCollect() {
        return valuesSource != null;
    }

    @Override
    public void collect(int doc, int owningBucketOrdinal) throws IOException {
        final DoubleValues values = valuesSource.doubleValues();
        final int valuesCount = values.setDocument(doc);
        assert noMatchYet();
        for (int i = 0; i < valuesCount; ++i) {
            final double value = values.nextValue();
            collect(doc, value);
        }
        resetMatches();
    }

    @Override
    public InternalAggregation buildAggregation(int owningBucketOrdinal) {
        // value source can be null in the case of unmapped fields
        ValueFormatter formatter = valuesSource != null ? valuesSource.formatter() : null;
        return rangeFactory.create(name, bucketsCollector.buildBuckets(), formatter, keyed);
    }

    private boolean noMatchYet() {
        for (int i = 0; i < matched.length; ++i) {
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
        int lo = 0, hi = bucketsCollector.ranges.length - 1; // all candidates are between these indexes
        int mid = (lo + hi) >>> 1;
        while (lo <= hi) {
            if (value < bucketsCollector.ranges[mid].from) {
                hi = mid - 1;
            } else if (value >= maxTo[mid]) {
                lo = mid + 1;
            } else {
                break;
            }
            mid = (lo + hi) >>> 1;
        }

        // binary search the lower bound
        int startLo = lo, startHi = mid;
        while (startLo <= startHi) {
            final int startMid = (startLo + startHi) >>> 1;
            if (value >= maxTo[startMid]) {
                startLo = startMid + 1;
            } else {
                startHi = startMid - 1;
            }
        }

        // binary search the upper bound
        int endLo = mid, endHi = hi;
        while (endLo <= endHi) {
            final int endMid = (endLo + endHi) >>> 1;
            if (value < bucketsCollector.ranges[endMid].from) {
                endHi = endMid - 1;
            } else {
                endLo = endMid + 1;
            }
        }

        assert startLo == 0 || value >= maxTo[startLo - 1];
        assert endHi == bucketsCollector.ranges.length - 1 || value < bucketsCollector.ranges[endHi + 1].from;

        for (int i = startLo; i <= endHi; ++i) {
            if (!matched[i] && bucketsCollector.ranges[i].matches(value)) {
                matched[i] = true;
                matchedList.add(i);
                bucketsCollector.collect(doc, i);
            }
        }
    }

    class BucketsCollector extends org.elasticsearch.search.aggregations.bucket.BucketsCollector {

        private final Range[] ranges;

        BucketsCollector(Aggregator[] aggregators, Range[] ranges) {
            super(aggregators, ranges.length);
            this.ranges = ranges;
            for (int i = 0; i < ranges.length; i++) {
                ranges[i].process(valuesSource.parser(), context);
            }
            sortRanges(this.ranges);
        }

        @Override
        protected boolean onDoc(int doc, int bucketOrd) throws IOException {
            return true;
        }

        List<RangeBase.Bucket> buildBuckets() {
            List<RangeBase.Bucket> buckets = Lists.newArrayListWithCapacity(ranges.length);
            for (int i = 0; i < ranges.length; i++) {
                Range range = ranges[i];
                RangeBase.Bucket bucket = rangeFactory.createBucket(range.key, range.from, range.to, docCounts[i],
                        buildAggregations(i), valuesSource.formatter());
                buckets.add(bucket);
            }
            return buckets;
        }

    }

    private static final void sortRanges(final Range[] ranges) {
        new InPlaceMergeSorter() {

            @Override
            protected void swap(int i, int j) {
                final Range tmp = ranges[i];
                ranges[i] = ranges[j];
                ranges[j] = tmp;
            }

            @Override
            protected int compare(int i, int j) {
                int cmp = Double.compare(ranges[i].from, ranges[j].from);
                if (cmp == 0) {
                    cmp = Double.compare(ranges[i].to, ranges[j].to);
                }
                return cmp;
            }
        };
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

            super(name, BucketAggregationMode.PER_BUCKET, AggregatorFactories.EMPTY, 0, aggregationContext, parent);
            this.ranges = ranges;
            for (Range range : this.ranges) {
                range.process(parser, context);
            }
            this.keyed = keyed;
            this.formatter = formatter;
            this.parser = parser;
            this.factory = factory;
        }

        @Override
        public boolean shouldCollect() {
            return false;
        }

        @Override
        public void collect(int doc, int owningBucketOrdinal) throws IOException {
        }

        @Override
        public AbstractRangeBase buildAggregation(int owningBucketOrdinal) {
            List<RangeBase.Bucket> buckets = new ArrayList<RangeBase.Bucket>(ranges.size());
            for (RangeAggregator.Range range : ranges) {
                buckets.add(factory.createBucket(range.key, range.from, range.to, 0, InternalAggregations.EMPTY, formatter));
            }
            return factory.create(name, buckets, formatter, keyed);
        }
    }

    public static class Factory extends ValueSourceAggregatorFactory<NumericValuesSource> {

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
        public BucketAggregationMode bucketMode() {
            return BucketAggregationMode.PER_BUCKET;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new Unmapped(name, ranges, keyed, valuesSourceConfig.formatter(), valuesSourceConfig.parser(), aggregationContext, parent, rangeFactory);
        }

        @Override
        protected Aggregator create(NumericValuesSource valuesSource, int expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            return new RangeAggregator(name, factories, valuesSource, rangeFactory, ranges, keyed, aggregationContext, parent);
        }
    }

}
