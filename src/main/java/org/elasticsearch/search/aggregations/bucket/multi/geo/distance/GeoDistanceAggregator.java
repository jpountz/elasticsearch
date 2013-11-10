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

    private final GeoPointValuesSource valuesSource;

    private final BucketsCollector bucketsCollector;
    final boolean[] matched;
    final IntArrayList matchedList;


    public GeoDistanceAggregator(String name, GeoPointValuesSource valuesSource, AggregatorFactories factories,
                                 List<DistanceRange> ranges, AggregationContext aggregationContext, Aggregator parent) {
        super(name, BucketAggregationMode.PER_BUCKET, factories, ranges.size(), aggregationContext, parent);
        this.valuesSource = valuesSource;
        bucketsCollector = new BucketsCollector(subAggregators, ranges);
        matched = new boolean[ranges.size()];
        matchedList = new IntArrayList(matched.length);
    }

    @Override
    public boolean shouldCollect() {
        return valuesSource != null;
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
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
        for (int i = 0; i < bucketsCollector.ranges.length; i++) {
            if (!matched[i] && bucketsCollector.ranges[i].matches(value)) {
                matched[i] = bucketsCollector.collect(doc, i);
                matchedList.add(i);
            }
        }
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        return new InternalGeoDistance(name, bucketsCollector.buildBuckets());
    }

    private class BucketsCollector extends org.elasticsearch.search.aggregations.bucket.BucketsCollector {

        private DistanceRange[] ranges;

        BucketsCollector(Aggregator[] aggregators, List<DistanceRange> ranges) {
            super(aggregators, ranges.size());
            this.ranges = ranges.toArray(new DistanceRange[ranges.size()]);
        }

        @Override
        protected boolean onDoc(int doc, long bucketOrd) throws IOException {
            return true;
        }

        List<GeoDistance.Bucket> buildBuckets() {
            List<GeoDistance.Bucket> buckets = Lists.newArrayListWithCapacity(ranges.length);
            for (int i = 0; i < ranges.length; i++) {
                InternalAggregations aggregations = buildSubAggregations(i);
                DistanceRange range = ranges[i];
                InternalGeoDistance.Bucket bucket = new InternalGeoDistance.Bucket(range.key, range.unit, range.from, range.to, docCount(i), aggregations);
                buckets.add(bucket);
            }
            return buckets;
        }
    }

    public static class Factory extends ValueSourceAggregatorFactory<GeoPointValuesSource> {

        private final List<DistanceRange> ranges;

        public Factory(String name, ValuesSourceConfig<GeoPointValuesSource> valueSourceConfig, List<DistanceRange> ranges) {
            super(name, InternalGeoDistance.TYPE.name(), valueSourceConfig);
            this.ranges = ranges;
        }

        @Override
        public BucketAggregationMode bucketMode() {
            return BucketAggregationMode.PER_BUCKET;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new GeoDistanceAggregator(name, null, factories, ranges, aggregationContext, parent);
        }

        @Override
        protected Aggregator create(GeoPointValuesSource valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            return new GeoDistanceAggregator(name, valuesSource, factories, ranges, aggregationContext, parent);
        }

    }

}
