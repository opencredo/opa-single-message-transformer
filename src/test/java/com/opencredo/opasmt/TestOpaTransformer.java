package com.opencredo.opasmt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class TestOpaTransformer {

    @Test
    public void testOpaTransformerFiltersOut() {
        OpaTransformer<SourceRecord> transformer = buildTransformer();

        Schema valueSchema = SchemaBuilder.struct().name("whatever").field("personal", Schema.BOOLEAN_SCHEMA).field("name", Schema.STRING_SCHEMA).build();

        var value = new Struct(valueSchema);
        value.put("name", "person1");
        value.put("personal", true);
        var record = new SourceRecord(Map.of(), Map.of(), "topic", valueSchema, value);

        var actual = transformer.apply(record);
        Assert.assertNull(actual);
    }

    @Test
    public void testOpaTransformerDoesntFilter() {
        OpaTransformer<SourceRecord> transformer = buildTransformer();

        Schema valueSchema = SchemaBuilder.struct().name("whatever").field("personal", Schema.BOOLEAN_SCHEMA).field("name", Schema.STRING_SCHEMA).build();

        var value = new Struct(valueSchema);
        value.put("name", "person1");
        value.put("personal", false);
        var record = new SourceRecord(Map.of(), Map.of(), "topic", valueSchema, value);

        var actual = transformer.apply(record);
        Assert.assertEquals(actual, record);
    }

    private OpaTransformer<SourceRecord> buildTransformer() {
        var properties = Map.of(OpaTransformer.OPA_BUNDLE_PATH_FIELD, "bundle.tar.gz");

        var transformer = new OpaTransformer<SourceRecord>();
        transformer.configure(properties);
        return transformer;
    }


}
