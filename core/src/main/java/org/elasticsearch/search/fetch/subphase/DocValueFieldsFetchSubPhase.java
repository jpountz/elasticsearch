/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Query sub phase which pulls data from doc values
 *
 * Specifying {@code "docvalue_fields": ["field1", "field2"]}
 */
public final class DocValueFieldsFetchSubPhase implements FetchSubPhase {

    // TODO: Remove in 7.0
    private static final String USE_DEFAULT_FORMAT = "use_field_mapping";
    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(Loggers.getLogger(DocValueFieldsFetchSubPhase.class));

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) {
        if (context.collapse() != null) {
            // retrieve the `doc_value` associated with the collapse field
            String name = context.collapse().getFieldType().name();
            if (context.docValueFieldsContext() == null) {
                context.docValueFieldsContext(new DocValueFieldsContext(
                        Collections.singletonList(new DocValueFieldsContext.Field(name, null))));
            } else if (context.docValueFieldsContext().fields().contains(name) == false) {
                context.docValueFieldsContext().fields().add(new DocValueFieldsContext.Field(name, null));
            }
        }
        if (context.docValueFieldsContext() == null) {
            return;
        }
        for (DocValueFieldsContext.Field field : context.docValueFieldsContext().fields()) {
            if (hitContext.hit().fieldsOrNull() == null) {
                hitContext.hit().fields(new HashMap<>(2));
            }
            SearchHitField hitField = hitContext.hit().getFields().get(field.getName());
            if (hitField == null) {
                hitField = new SearchHitField(field.getName(), new ArrayList<>(2));
                hitContext.hit().getFields().put(field.getName(), hitField);
            }
            MappedFieldType fieldType = context.mapperService().fullName(field.getName());
            if (fieldType != null) {
                final List<Object> values;
                String formatName = field.getFormat();
                if (USE_DEFAULT_FORMAT.equals(formatName)) {
                    // 5.0 and 5.1 did not format doc values fields, so we exposed the ability to
                    // use the format associated with the field with a special format name
                    // `use_field_mapping`, which we are keeping in 6.x to ease the transition from
                    // 5.x to 6.x
                    DEPRECATION_LOGGER.deprecated("Format [{}] is deprecated, just omit the format or set it to null in order to use "
                            + "the field defaults", USE_DEFAULT_FORMAT);
                    formatName = null;
                }
                final DocValueFormat format = fieldType.docValueFormat(formatName, null);
                final IndexFieldData<?> fieldData = context.fieldData().getForField(fieldType);
                if (fieldData instanceof IndexNumericFieldData) {
                    IndexNumericFieldData numericFieldData = (IndexNumericFieldData) fieldData;
                    if (numericFieldData.getNumericType().isFloatingPoint()) {
                        SortedNumericDoubleValues dv = numericFieldData.load(hitContext.readerContext()).getDoubleValues();
                        dv.setDocument(hitContext.docId());
                        final int count = dv.count();
                        values = new ArrayList<>(count);
                        for (int i = 0; i < count; ++i) {
                            values.add(format.format(dv.valueAt(i)));
                        }
                    } else {
                        SortedNumericDocValues dv = numericFieldData.load(hitContext.readerContext()).getLongValues();
                        dv.setDocument(hitContext.docId());
                        final int count = dv.count();
                        values = new ArrayList<>(count);
                        for (int i = 0; i < count; ++i) {
                            values.add(format.format(dv.valueAt(i)));
                        }
                    }
                } else {
                    SortedBinaryDocValues dv = fieldData.load(hitContext.readerContext()).getBytesValues();
                    dv.setDocument(hitContext.docId());
                    final int count = dv.count();
                    values = new ArrayList<>(count);
                    for (int i = 0; i < count; ++i) {
                        values.add(format.format(dv.valueAt(i)));
                    }
                }

                hitField.values().addAll(values);
            }
        }
    }
}
