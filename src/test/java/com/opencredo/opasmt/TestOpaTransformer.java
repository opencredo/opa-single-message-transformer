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

        Schema valueSchema = SchemaBuilder.struct().name("test schema").field("personal", Schema.BOOLEAN_SCHEMA).field("name", Schema.STRING_SCHEMA).build();

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

        Schema valueSchema = SchemaBuilder.struct().name("test schema").field("personal", Schema.BOOLEAN_SCHEMA).field("name", Schema.STRING_SCHEMA).build();

        var value = new Struct(valueSchema);
        value.put("name", "person1");
        value.put("personal", false);
        var record = new SourceRecord(Map.of(), Map.of(), "topic", valueSchema, value);

        var actual = transformer.apply(record);
        Assert.assertEquals(actual, record);
    }

    @Test
    public void testFieldMasking() {
        OpaTransformer<SourceRecord> transformer = buildTransformer();

        Schema valueSchema = SchemaBuilder.struct().name("test schema").field("phone", Schema.STRING_SCHEMA).field("personal", Schema.BOOLEAN_SCHEMA).field("name", Schema.STRING_SCHEMA).build();

        var value = new Struct(valueSchema);
        value.put("name", "person1");
        value.put("personal", false);
        value.put("phone", "020 8765 4321");
        var record = new SourceRecord(Map.of(), Map.of(), "topic", valueSchema, value);

        var actual = transformer.apply(record);
        Assert.assertEquals("person1", ((Struct)actual.value()).get("name"));
        Assert.assertEquals(false, ((Struct)actual.value()).get("personal"));
        Assert.assertEquals("000 0000 0000", ((Struct)actual.value()).get("phone"));
    }

    @Test
    public void testObjectFieldMasking() {
        OpaTransformer<SourceRecord> transformer = buildTransformer();

        Schema addressSchema = SchemaBuilder.struct().name("test schema")
                .field("building", Schema.INT32_SCHEMA)
                .field("street", Schema.STRING_SCHEMA)
                .field("city", Schema.STRING_SCHEMA)
                .build();

        Schema valueSchema = SchemaBuilder.struct().name("test schema")
                .field("phone", Schema.STRING_SCHEMA)
                .field("personal", Schema.BOOLEAN_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("address", addressSchema)
                .build();

        var address = new Struct(addressSchema);
        address.put("building", 131);
        address.put("street", "Hope Street");
        address.put("city", "london");

        var value = new Struct(valueSchema);
        value.put("name", "person1");
        value.put("personal", false);
        value.put("phone", "020 8765 4321");
        value.put("address", address);
        var record = new SourceRecord(Map.of(), Map.of(), "topic", valueSchema, value);

        var actual = transformer.apply(record);
        Struct actualValue = (Struct) actual.value();
        Assert.assertEquals("person1", actualValue.get("name"));
        Assert.assertEquals(false, actualValue.get("personal"));
        Assert.assertEquals("000 0000 0000", actualValue.get("phone"));

        Struct actualAddress = (Struct) actualValue.get("address");
        Assert.assertEquals(131, actualAddress.get("building"));
        Assert.assertEquals("Hope Street", actualAddress.get("street"));
        Assert.assertEquals("anon city", actualAddress.get("city"));
    }

    private OpaTransformer<SourceRecord> buildTransformer() {
        var properties = Map.of(
                OpaTransformer.BUNDLE_PATH_FIELD_CONFIG, "example/bundle.tar.gz",
                OpaTransformer.FILTERING_ENTRYPOINT_CONFIG, "kafka/filter",
                OpaTransformer.MASKING_ENTRYPOINT_CONFIG, "kafka/maskingConfig"
        );

        var transformer = new OpaTransformer<SourceRecord>();
        transformer.configure(properties);
        return transformer;
    }


}
