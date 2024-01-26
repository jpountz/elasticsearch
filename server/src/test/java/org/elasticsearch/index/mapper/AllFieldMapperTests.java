/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class AllFieldMapperTests extends MetadataMapperTestCase {

    public void testPostParseEnabled() throws IOException {
        DocumentMapper docMapper = createDocumentMapper(allFieldMapping(true, b -> {
            b.startObject("field1");
            b.field("type", "keyword");
            b.endObject();
        }));

        ParsedDocument doc = docMapper.parse(source(b -> b.field("field1", "value1")));
        assertThat(doc.rootDoc().getFields(AllFieldMapper.NAME).size(), equalTo(1));
        assertThat(doc.rootDoc().getFields(AllFieldMapper.NAME).get(0).binaryValue(), equalTo(new BytesRef("value1\0field1")));

        Query query = docMapper.mappers().getFieldType("field1").termQuery("value1", null);
        assertThat(query, instanceOf(TermQuery.class));
        TermQuery termQuery = (TermQuery) query;
        assertThat(termQuery.getTerm().field(), equalTo(AllFieldMapper.NAME));
        assertThat(termQuery.getTerm().bytes(), equalTo(new BytesRef("value1\0field1")));
    }

    public void testPostParseDisabed() throws IOException {
        DocumentMapper docMapper = createDocumentMapper(allFieldMapping(false, b -> {
            b.startObject("field1");
            b.field("type", "keyword");
            b.endObject();
        }));

        ParsedDocument doc = docMapper.parse(source(b -> b.field("field1", "value1")));
        assertThat(doc.rootDoc().getFields(AllFieldMapper.NAME).size(), equalTo(0));

        Query query = docMapper.mappers().getFieldType("field1").termQuery("value1", null);
        assertThat(query, instanceOf(TermQuery.class));
        TermQuery termQuery = (TermQuery) query;
        assertThat(termQuery.getTerm().field(), equalTo("field1"));
        assertThat(termQuery.getTerm().bytes(), equalTo(new BytesRef("value1")));
    }

    @Override
    protected String fieldName() {
        return AllFieldMapper.NAME;
    }

    @Override
    protected boolean isConfigurable() {
        return true;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck(
            "enabled",
            allFieldMapping(true, b -> b.startObject("field1").field("type", "keyword").endObject()),
            allFieldMapping(false, b -> b.startObject("field1").field("type", "keyword").endObject())
        );
        checker.registerConflictCheck(
            "enabled",
            allFieldMapping(false, b -> b.startObject("field1").field("type", "keyword").endObject()),
            allFieldMapping(true, b -> b.startObject("field1").field("type", "keyword").endObject())
        );
    }

    private static XContentBuilder allFieldMapping(boolean enabled, CheckedConsumer<XContentBuilder, IOException> propertiesBuilder)
        throws IOException {
        return topMapping(b -> {
            b.startObject(AllFieldMapper.NAME).field("enabled", enabled).endObject();
            b.startObject("properties");
            propertiesBuilder.accept(b);
            b.endObject();
        });
    }
}
