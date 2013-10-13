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

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.search.aggregations.bucket.multi.geo.distance.InternalGeoDistance;
import org.elasticsearch.search.aggregations.bucket.multi.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.bucket.multi.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.multi.range.InternalRange;
import org.elasticsearch.search.aggregations.bucket.multi.range.date.InternalDateRange;
import org.elasticsearch.search.aggregations.bucket.multi.range.ip4v.InternalIPv4Range;
import org.elasticsearch.search.aggregations.bucket.multi.terms.DoubleTerms;
import org.elasticsearch.search.aggregations.bucket.multi.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.multi.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.multi.terms.UnmappedTerms;
import org.elasticsearch.search.aggregations.bucket.single.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.single.global.InternalGlobal;
import org.elasticsearch.search.aggregations.bucket.single.missing.InternalMissing;
import org.elasticsearch.search.aggregations.bucket.single.nested.InternalNested;
import org.elasticsearch.search.aggregations.calc.bytes.count.InternalCount;
import org.elasticsearch.search.aggregations.calc.numeric.avg.InternalAvg;
import org.elasticsearch.search.aggregations.calc.numeric.max.InternalMax;
import org.elasticsearch.search.aggregations.calc.numeric.min.InternalMin;
import org.elasticsearch.search.aggregations.calc.numeric.stats.InternalExtendedStats;
import org.elasticsearch.search.aggregations.calc.numeric.stats.InternalStats;
import org.elasticsearch.search.aggregations.calc.numeric.sum.InternalSum;

/**
 * A module that registers all the transport streams for the addAggregation
 */
public class TransportAggregationModule extends AbstractModule {

    @Override
    protected void configure() {

        // calcs
        InternalAvg.registerStreams();
        InternalSum.registerStreams();
        InternalMin.registerStreams();
        InternalMax.registerStreams();
        InternalStats.registerStreams();
        InternalExtendedStats.registerStreams();
        InternalCount.registerStreams();

        // buckets
        InternalGlobal.registerStreams();
        InternalFilter.registerStreams();
        InternalMissing.registerStreams();
        StringTerms.registerStreams();
        LongTerms.registerStreams();
        DoubleTerms.registerStreams();
        UnmappedTerms.registerStreams();
        InternalRange.registerStream();
        InternalDateRange.registerStream();
        InternalIPv4Range.registerStream();
        InternalHistogram.registerStream();
        InternalDateHistogram.registerStream();
        InternalGeoDistance.registerStream();
        InternalNested.registerStream();
    }
}
