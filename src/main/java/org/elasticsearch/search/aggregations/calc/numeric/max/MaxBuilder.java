package org.elasticsearch.search.aggregations.calc.numeric.max;

import org.elasticsearch.search.aggregations.calc.numeric.NumericAggregationBuilder;

/**
 *
 */
public class MaxBuilder extends NumericAggregationBuilder<MaxBuilder> {

    public MaxBuilder(String name) {
        super(name, InternalMax.TYPE.name());
    }
}
