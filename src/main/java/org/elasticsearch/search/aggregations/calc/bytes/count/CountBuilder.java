package org.elasticsearch.search.aggregations.calc.bytes.count;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.calc.CalcAggregationBuilder;

import java.io.IOException;

/**
 *
 */
public class CountBuilder extends CalcAggregationBuilder<CountBuilder> {

    private String field;

    public CountBuilder(String name) {
        super(name, InternalCount.TYPE.name());
    }

    public CountBuilder field(String field) {
        this.field = field;
        return this;
    }

    @Override
    protected void internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (field != null) {
            builder.field("field", field);
        }
    }
}
