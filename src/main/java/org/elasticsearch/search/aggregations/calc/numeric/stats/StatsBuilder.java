package org.elasticsearch.search.aggregations.calc.numeric.stats;

import org.elasticsearch.search.aggregations.calc.numeric.NumericAggregationBuilder;

/**
 *
 */
public class StatsBuilder extends NumericAggregationBuilder<StatsBuilder> {

    public StatsBuilder(String name) {
        super(name, InternalStats.TYPE.name());
    }
}
