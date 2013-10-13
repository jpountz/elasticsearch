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

import com.google.common.collect.Lists;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.DoubleBucketsAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValueSpace;
import org.elasticsearch.search.aggregations.context.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.context.numeric.NumericValuesSource;
import org.elasticsearch.search.aggregations.context.numeric.ValueFormatter;
import org.elasticsearch.search.aggregations.context.numeric.ValueParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.aggregations.bucket.BucketsAggregator.buildAggregations;
import static org.elasticsearch.search.aggregations.bucket.BucketsAggregator.createSubAggregators;

/**
 *
 */
public class RangeAggregator extends DoubleBucketsAggregator {

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

    private final boolean keyed;
    private final AbstractRangeBase.Factory rangeFactory;
    BucketCollector[] bucketCollectors;

    public RangeAggregator(String name,
                           List<Aggregator.Factory> factories,
                           NumericValuesSource valuesSource,
                           AbstractRangeBase.Factory rangeFactory,
                           List<Range> ranges,
                           boolean keyed,
                           AggregationContext aggregationContext,
                           Aggregator parent) {

        super(name, valuesSource, aggregationContext, parent);
        this.keyed = keyed;
        this.rangeFactory = rangeFactory;
        bucketCollectors = new BucketCollector[ranges.size()];
        int i = 0;
        for (Range range : ranges) {
            ValueParser parser = valuesSource != null ? valuesSource.parser() : null;
            range.process(parser, aggregationContext);
            bucketCollectors[i++] = new BucketCollector(range, valuesSource, createSubAggregators(factories, this), this);
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
            buckets.add(bucketCollectors[i].buildBucket(rangeFactory));
        }

        // value source can be null in the case of unmapped fields
        ValueFormatter formatter = valuesSource != null ? valuesSource.formatter() : null;
        return rangeFactory.create(name, buckets, valuesSource.formatter(), keyed);
    }

    class Collector implements Aggregator.Collector {

        @Override
        public void collect(int doc, ValueSpace valueSpace) throws IOException {
            for (int i = 0; i < bucketCollectors.length; i++) {
                bucketCollectors[i].collect(doc, valueSpace);
            }
        }

        @Override
        public void postCollection() {
            for (int i = 0; i < bucketCollectors.length; i++) {
                bucketCollectors[i].postCollection();
            }
        }
    }

    static class BucketCollector extends DoubleBucketsAggregator.BucketCollector {

        private final Range range;

        long docCount;

        BucketCollector(Range range, NumericValuesSource valuesSource, Aggregator[] aggregators, Aggregator aggregator) {
            super(valuesSource, aggregators, aggregator);
            this.range = range;
        }

        @Override
        protected boolean onDoc(int doc, DoubleValues values, ValueSpace valueSpace) throws IOException {
            if (matches(doc, values, valueSpace)) {
                docCount++;
                return true;
            }
            return false;
        }

        private boolean matches(int doc, DoubleValues values, ValueSpace context) {
            if (!values.hasValue(doc)) {
                return false;
            }

            Object valueSourceKey = valuesSource.key();
            if (!values.isMultiValued()) {
                double value = values.getValue(doc);
                return context.accept(valueSourceKey, value) && range.matches(value);
            }

            for (DoubleValues.Iter iter = values.getIter(doc); iter.hasNext();) {
                double value = iter.next();
                if (context.accept(valueSourceKey, value) && range.matches(value)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean accept(double value) {
            return range.matches(value);
        }

        RangeBase.Bucket buildBucket(AbstractRangeBase.Factory factory) {
            return factory.createBucket(range.key, range.from, range.to, docCount, buildAggregations(subAggregators), valuesSource.formatter());
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

            super(name, aggregationContext, parent);
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

    public static class Factory extends CompoundFactory<NumericValuesSource> {

        private final AbstractRangeBase.Factory rangeFactory;
        private final List<Range> ranges;
        private final boolean keyed;

        public Factory(String name, ValuesSourceConfig<NumericValuesSource> valueSourceConfig, AbstractRangeBase.Factory rangeFactory, List<Range> ranges, boolean keyed) {
            super(name, valueSourceConfig);
            this.rangeFactory = rangeFactory;
            this.ranges = ranges;
            this.keyed = keyed;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new Unmapped(name, ranges, keyed, valueSourceConfig.formatter(), valueSourceConfig.parser(), aggregationContext, parent, rangeFactory);
        }

        @Override
        protected Aggregator create(NumericValuesSource valuesSource, AggregationContext aggregationContext, Aggregator parent) {
            return new RangeAggregator(name, factories, valuesSource, rangeFactory, ranges, keyed, aggregationContext, parent);
        }
    }

}
