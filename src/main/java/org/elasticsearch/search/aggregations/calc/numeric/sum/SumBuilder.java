package org.elasticsearch.search.aggregations.calc.numeric.sum;

import org.elasticsearch.search.aggregations.calc.numeric.NumericAggregationBuilder;

/**
 *
 */
public class SumBuilder extends NumericAggregationBuilder<SumBuilder> {

    public SumBuilder(String name) {
        super(name, InternalSum.TYPE.name());
    }
}
