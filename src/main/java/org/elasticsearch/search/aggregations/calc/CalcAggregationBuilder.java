package org.elasticsearch.search.aggregations.calc;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import java.io.IOException;

/**
 *
 */
public abstract class CalcAggregationBuilder<B extends CalcAggregationBuilder> extends AggregationBuilder {

    public CalcAggregationBuilder(String name, String type) {
        super(name, type);
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name).startObject(type);
        internalXContent(builder, params);
        return builder.endObject().endObject();
    }

    protected abstract void internalXContent(XContentBuilder builder, Params params) throws IOException;
}
