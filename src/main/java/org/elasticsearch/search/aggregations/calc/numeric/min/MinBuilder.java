package org.elasticsearch.search.aggregations.calc.numeric.min;

import org.elasticsearch.search.aggregations.calc.numeric.NumericAggregationBuilder;

/**
 *
 */
public class MinBuilder extends NumericAggregationBuilder<MinBuilder> {

    public MinBuilder(String name) {
        super(name, InternalMin.TYPE.name());
    }
}
