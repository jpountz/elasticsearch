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

import com.google.common.collect.Lists;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.GeoPointBucketsAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValueSpace;
import org.elasticsearch.search.aggregations.context.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.context.geopoints.GeoPointValuesSource;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.search.aggregations.bucket.BucketsAggregator.buildAggregations;

/**
 *
 */
public class GeoDistanceAggregator extends GeoPointBucketsAggregator {

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

    private final BucketCollector[] collectors;

    public GeoDistanceAggregator(String name, GeoPointValuesSource valuesSource, List<Aggregator.Factory> factories,
                                 List<DistanceRange> ranges, AggregationContext aggregationContext, Aggregator parent) {
        super(name, valuesSource, aggregationContext, parent);
        collectors = new BucketCollector[ranges.size()];
        int i = 0;
        for (DistanceRange range : ranges) {
            collectors[i++] = new BucketCollector(range, valuesSource, BucketsAggregator.createSubAggregators(factories, this), this);
        }
    }

    @Override
    public Collector collector() {
        return valuesSource != null ? new Collector() : null;
    }

    @Override
    public InternalAggregation buildAggregation() {
        List<GeoDistance.Bucket> buckets = Lists.newArrayListWithCapacity(collectors.length);
        for (BucketCollector collector : collectors) {
            buckets.add(collector.buildBucket());
        }
        return new InternalGeoDistance(name, buckets);
    }

    class Collector implements Aggregator.Collector {

        @Override
        public void collect(int doc, ValueSpace valueSpace) throws IOException {
            for (BucketCollector collector : collectors) {
                collector.collect(doc, valueSpace);
            }
        }

        @Override
        public void postCollection() {
            for (BucketCollector collector : collectors) {
                collector.postCollection();
            }
        }
    }

    static class BucketCollector extends GeoPointBucketsAggregator.BucketCollector {

        private final DistanceRange range;

        long docCount;

        BucketCollector(DistanceRange range, GeoPointValuesSource valuesSource, Aggregator[] subAggregators, Aggregator aggregator) {
            super(valuesSource, subAggregators, aggregator);
            this.range = range;
        }

        @Override
        protected boolean onDoc(int doc, GeoPointValues values, ValueSpace valueSpace) throws IOException {
            if (matches(doc, valuesSource.key(), values, valueSpace)) {
                docCount++;
                return true;
            }
            return false;
        }

        private boolean matches(int doc, Object valuesSourceKey, GeoPointValues values, ValueSpace context) {
            if (!values.hasValue(doc)) {
                return false;
            }
            if (!values.isMultiValued()) {
                return range.matches(values.getValue(doc)) && context.accept(valuesSourceKey, values.getValue(doc));
            }
            for (GeoPointValues.Iter iter = values.getIter(doc); iter.hasNext();) {
                GeoPoint point = iter.next();
                if (range.matches(point) && context.accept(valuesSourceKey, point)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean accept(GeoPoint value) {
            return range.matches(value);
        }

        InternalGeoDistance.Bucket buildBucket() {
            return new InternalGeoDistance.Bucket(range.key, range.unit, range.from, range.to, docCount, buildAggregations(subAggregators));
        }
    }

    public static class Factory extends CompoundFactory<GeoPointValuesSource> {

        private final List<DistanceRange> ranges;

        public Factory(String name, ValuesSourceConfig<GeoPointValuesSource> valueSourceConfig, List<DistanceRange> ranges) {
            super(name, valueSourceConfig);
            this.ranges = ranges;
        }

        @Override
        protected GeoDistanceAggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new GeoDistanceAggregator(name, null, factories, ranges, aggregationContext, parent);
        }

        @Override
        protected GeoDistanceAggregator create(GeoPointValuesSource valuesSource, AggregationContext aggregationContext, Aggregator parent) {
            return new GeoDistanceAggregator(name, valuesSource, factories, ranges, aggregationContext, parent);
        }
    }

}
