package org.elasticsearch.search.aggregations.context;

import org.elasticsearch.search.aggregations.context.bytes.BytesValuesSource;
import org.elasticsearch.search.aggregations.context.numeric.NumericValuesSource;

/**
 *
 */
public enum ScriptValueType {

    STRING(BytesValuesSource.class),
    LONG(NumericValuesSource.class),
    DOUBLE(NumericValuesSource.class);

    final Class<? extends ValuesSource> valuesSourceType;

    private ScriptValueType(Class<? extends ValuesSource> valuesSourceType) {
        this.valuesSourceType = valuesSourceType;
    }

    public Class<? extends ValuesSource> getValuesSourceType() {
        return valuesSourceType;
    }

    public boolean isNumeric() {
        return this != STRING;
    }

    public boolean isFloatingPoint() {
        return this == DOUBLE;
    }
}
