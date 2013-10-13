package org.elasticsearch.search.aggregations.calc.numeric.avg;

import org.elasticsearch.search.aggregations.calc.numeric.NumericAggregationBuilder;

/**
 *
 */
public class AvgBuilder extends NumericAggregationBuilder<AvgBuilder> {

    public AvgBuilder(String name) {
        super(name, InternalAvg.TYPE.name());
    }
}
