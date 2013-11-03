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

package org.elasticsearch.search.aggregations.bucket.multi.geo.distance;

import com.carrotsearch.hppc.IntArrayList;
import com.google.common.collect.Lists;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.OrdsAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.context.geopoints.GeoPointValuesSource;
import org.elasticsearch.search.aggregations.factory.AggregatorFactories;
import org.elasticsearch.search.aggregations.factory.ValueSourceAggregatorFactory;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class GeoDistanceAggregator extends Aggregator {

    static class DistanceRange {

        String key;
        GeoPoint origin;
        double from;
        double to;
        DistanceUnit unit;
        org.elasticsearch.common.geo.GeoDistance distanceType;

        DistanceRange(String key, double from, double to) {
            this.from = from;
            this.to = to;
            this.key = key(key, from, to);
        }

        boolean matches(GeoPoint target) {
            double distance = distanceType.calculate(origin.getLat(), origin.getLon(), target.getLat(), target.getLon(), unit);
            return distance >= from && distance < to;
        }

        private static String key(String key, double from, double to) {
            if (key != null) {
                return key;
            }
            StringBuilder sb = new StringBuilder();
            sb.append(from == 0 ? "*" : from);
            sb.append("-");
            sb.append(Double.isInfinite(to) ? "*" : to);
            return sb.toString();
        }
    }

    private final BucketCollector[] bucketCollectors;

    private final GeoPointValuesSource valuesSource;

    private final Collector collector;

    public GeoDistanceAggregator(String name, GeoPointValuesSource valuesSource, AggregatorFactories factories,
                                 List<DistanceRange> ranges, AggregationContext aggregationContext, Aggregator parent) {
        super(name, factories, ranges.size(), aggregationContext, parent);
        this.valuesSource = valuesSource;
        bucketCollectors = new BucketCollector[ranges.size()];
        int i = 0;
        for (DistanceRange range : ranges) {
            bucketCollectors[i++] = new BucketCollector(i, range, factories.createAggregators(this), ordsAggregators);
        }
        collector = valuesSource == null ? null : new Collector();
    }

    @Override
    public boolean shouldCollect() {
        return collector != null;
    }

    @Override
    public void collect(int doc) throws IOException {
        collector.collect(doc);
    }

    @Override
    public void postCollection() {
        collector.postCollection();
    }

    @Override
    public InternalAggregation buildAggregation() {
        List<GeoDistance.Bucket> buckets = Lists.newArrayListWithCapacity(bucketCollectors.length);
        for (BucketCollector collector : bucketCollectors) {
            InternalAggregations aggregations = collector.buildAggregations();
            DistanceRange range = collector.range;
            InternalGeoDistance.Bucket bucket = new InternalGeoDistance.Bucket(range.key, range.unit, range.from, range.to, collector.docCount(), aggregations);
            buckets.add(bucket);
        }
        return new InternalGeoDistance(name, buckets);
    }

    class Collector {

        final boolean[] matched;
        final IntArrayList matchedList;

        Collector() {
            matched = new boolean[bucketCollectors.length];
            matchedList = new IntArrayList(matched.length);
        }

        public void collect(int doc) throws IOException {
            final GeoPointValues values = valuesSource.values();
            final int valuesCount = values.setDocument(doc);
            assert noMatchYet();
            for (int i = 0; i < valuesCount; ++i) {
                final GeoPoint value = values.nextValue();
                collect(doc, value);
            }
            resetMatches();
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

        private void collect(int doc, GeoPoint value) throws IOException {
            for (int i = 0; i < bucketCollectors.length; ++i) {
                if (!matched[i] && bucketCollectors[i].range.matches(value)) {
                    matched[i] = true;
                    matchedList.add(i);
                    bucketCollectors[i].collect(doc);
                }
            }
        }

        public void postCollection() {
            for (BucketCollector collector : bucketCollectors) {
                collector.postCollection();
            }
        }
    }

    static class BucketCollector extends org.elasticsearch.search.aggregations.bucket.BucketCollector {

        private final DistanceRange range;

        BucketCollector(int ord, DistanceRange range, Aggregator[] aggregators, OrdsAggregator[] ordsAggregators) {
            super(ord, aggregators, ordsAggregators);
            this.range = range;
        }

        @Override
        protected boolean onDoc(int doc) throws IOException {
            return true;
        }
    }

    public static class Factory extends ValueSourceAggregatorFactory.Normal<GeoPointValuesSource> {

        private final List<DistanceRange> ranges;

        public Factory(String name, ValuesSourceConfig<GeoPointValuesSource> valueSourceConfig, List<DistanceRange> ranges) {
            super(name, InternalGeoDistance.TYPE.name(), valueSourceConfig);
            this.ranges = ranges;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new GeoDistanceAggregator(name, null, factories, ranges, aggregationContext, parent);
        }

        @Override
        protected Aggregator create(GeoPointValuesSource valuesSource, AggregationContext aggregationContext, Aggregator parent) {
            return new GeoDistanceAggregator(name, valuesSource, factories, ranges, aggregationContext, parent);
        }

    }

}
