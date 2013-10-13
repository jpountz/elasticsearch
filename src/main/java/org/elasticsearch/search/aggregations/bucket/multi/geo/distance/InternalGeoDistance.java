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
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.io.IOException;
import java.util.*;

/**
 *
 */
public class InternalGeoDistance extends InternalAggregation implements GeoDistance, ToXContent, Streamable {

    public static final Type TYPE = new Type("geo_distance", "gdist");

    public static final AggregationStreams.Stream<InternalGeoDistance> STREAM = new AggregationStreams.Stream<InternalGeoDistance>() {
        @Override
        public InternalGeoDistance readResult(StreamInput in) throws IOException {
            InternalGeoDistance geoDistance = new InternalGeoDistance();
            geoDistance.readFrom(in);
            return geoDistance;
        }
    };

    public static void registerStream() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    static class Bucket implements GeoDistance.Bucket {

        private String key;
        private DistanceUnit unit;
        private double from;
        private double to;
        private long docCount;
        private InternalAggregations aggregations;

        Bucket(String key, DistanceUnit unit, double from, double to, long docCount, List<InternalAggregation> aggregations) {
            this(key, unit, from, to, docCount, new InternalAggregations(aggregations));
        }

        Bucket(String key, DistanceUnit unit, double from, double to, long docCount, InternalAggregations aggregations) {
            this.key = key;
            this.unit = unit;
            this.from = from;
            this.to = to;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public DistanceUnit getUnit() {
            return unit;
        }

        @Override
        public double getFrom() {
            return from;
        }

        @Override
        public double getTo() {
            return to;
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        Bucket reduce(List<Bucket> buckets, CacheRecycler cacheRecycler) {
            if (buckets.size() == 1) {
                return buckets.get(0);
            }
            Bucket reduced = null;
            List<InternalAggregations> aggregationsList = Lists.newArrayListWithCapacity(buckets.size());
            for (Bucket bucket : buckets) {
                if (reduced == null) {
                    reduced = bucket;
                } else {
                    reduced.docCount += bucket.docCount;
                }
                aggregationsList.add(bucket.aggregations);
            }
            reduced.aggregations = InternalAggregations.reduce(aggregationsList, cacheRecycler);
            return reduced;
        }
    }

    private List<GeoDistance.Bucket> buckets;
    private Map<String, GeoDistance.Bucket> bucketMap;

    InternalGeoDistance() {} // for serialization

    public InternalGeoDistance(String name, List<GeoDistance.Bucket> buckets) {
        super(name);
        this.buckets = buckets;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalAggregation reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        if (aggregations.size() == 1) {
            return aggregations.get(0);
        }

        List<List<Bucket>> distancesList = null;
        for (InternalAggregation aggregation : aggregations) {
            InternalGeoDistance distances = (InternalGeoDistance) aggregation;
            if (distancesList == null) {
                distancesList = new ArrayList<List<Bucket>>(distances.buckets.size());
                for (GeoDistance.Bucket bucket : distances.buckets) {
                    List<Bucket> sameDistanceList = new ArrayList<Bucket>(aggregations.size());
                    sameDistanceList.add((Bucket) bucket);
                    distancesList.add(sameDistanceList);
                }
            } else {
                int i = 0;
                for (GeoDistance.Bucket bucket : distances.buckets) {
                    distancesList.get(i++).add((Bucket) bucket);
                }
            }
        }

        InternalGeoDistance reduced = (InternalGeoDistance) aggregations.get(0);
        int i = 0;
        for (List<Bucket> sameDistanceList : distancesList) {
            reduced.buckets.set(i++, sameDistanceList.get(0).reduce(sameDistanceList, reduceContext.cacheRecycler()));
        }
        return reduced;
    }

    @Override
    public Iterator<GeoDistance.Bucket> iterator() {
        return buckets.iterator();
    }

    @Override
    public List<GeoDistance.Bucket> buckets() {
        return buckets;
    }

    @Override
    public GeoDistance.Bucket getByKey(String key) {
        if (bucketMap == null) {
            bucketMap = new HashMap<String, GeoDistance.Bucket>(buckets.size());
            for (GeoDistance.Bucket bucket : buckets) {
                bucketMap.put(bucket.getKey(), bucket);
            }
        }
        return bucketMap.get(key);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        int size = in.readVInt();
        List<GeoDistance.Bucket> buckets = Lists.newArrayListWithCapacity(size);
        for (int i = 0; i < size; i++) {
            buckets.add(new Bucket(in.readOptionalString(), DistanceUnit.readDistanceUnit(in), in.readDouble(), in.readDouble(), in.readVLong(), InternalAggregations.readAggregations(in)));
        }
        this.bucketMap = null;
        this.buckets = buckets;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVInt(buckets.size());
        for (GeoDistance.Bucket bucket : buckets) {
            out.writeOptionalString(((Bucket) bucket).key);
            DistanceUnit.writeDistanceUnit(out, ((Bucket) bucket).unit);
            out.writeDouble(((Bucket) bucket).from);
            out.writeDouble(((Bucket) bucket).to);
            out.writeVLong(((Bucket) bucket).docCount);
            ((Bucket) bucket).aggregations.writeTo(out);
        }
    }


    public static class Fields {
        public static final XContentBuilderString UNIT = new XContentBuilderString("unit");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(name);
        for (GeoDistance.Bucket bucket : buckets) {
            builder.startObject();
            if (((Bucket) bucket).key != null) {
                builder.field(CommonFields.KEY, ((Bucket) bucket).key);
            }
            builder.field(Fields.UNIT, ((Bucket) bucket).unit.toString());
            if (!Double.isInfinite(((Bucket) bucket).from)) {
                builder.field(CommonFields.FROM, ((Bucket) bucket).from);
            }
            if (!Double.isInfinite(((Bucket) bucket).to)) {
                builder.field(CommonFields.TO, ((Bucket) bucket).to);
            }
            builder.field(CommonFields.DOC_COUNT, ((Bucket) bucket).docCount);
            ((Bucket) bucket).aggregations.toXContentInternal(builder, params);
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }
}
