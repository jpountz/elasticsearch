package org.elasticsearch.search.aggregations.bucket.single.global;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.bucket.BucketAggregationBuilder;

import java.io.IOException;

/**
 *
 */
public class GlobalBuilder extends BucketAggregationBuilder<GlobalBuilder> {

    public GlobalBuilder(String name) {
        super(name, InternalGlobal.TYPE.name());
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().endObject();
    }
}
