package org.elasticsearch.search.aggregations.calc.numeric;

import com.google.common.collect.Maps;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.calc.CalcAggregationBuilder;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public abstract class NumericAggregationBuilder<B extends NumericAggregationBuilder> extends CalcAggregationBuilder<B> {

    private String field;
    private String script;
    private String scriptLang;
    private Map<String, Object> params;
    private Boolean multiValued;

    protected NumericAggregationBuilder(String name, String type) {
        super(name, type);
    }

    public B field(String field) {
        this.field = field;
        return (B) this;
    }

    public B script(String script) {
        this.script = script;
        return (B) this;
    }

    public B scriptLang(String scriptLang) {
        this.scriptLang = scriptLang;
        return (B) this;
    }

    public B params(Map<String, Object> params) {
        if (this.params == null) {
            this.params = params;
        } else {
            this.params.putAll(params);
        }
        return (B) this;
    }

    public B param(String name, Object value) {
        if (this.params == null) {
            this.params = Maps.newHashMap();
        }
        this.params.put(name, value);
        return (B) this;
    }

    public B multiValued(boolean multiValued) {
        this.multiValued = multiValued;
        return (B) this;
    }

    @Override
    protected void internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (field != null) {
            builder.field("field", field);
        }

        if (script != null) {
            builder.field("script", script);
        }

        if (scriptLang != null) {
            builder.field("script_lang", scriptLang);
        }

        if (this.params != null && !this.params.isEmpty()) {
            builder.field("params").map(this.params);
        }

        if (multiValued != null) {
            builder.field("multi_valued", multiValued);
        }
    }
}
