package com.opencredo.opasmt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.opencredo.opasmt.OPATest.buildTransformer;

public class TestOpaTransformer {

    @Test
    public void testFilteringRecordsOut() {
        OpaTransformer<SourceRecord> transformer = buildTransformer(SIMPLE_TEST_REGO);

        Schema valueSchema = SchemaBuilder.struct().name("test schema").field("personal", Schema.BOOLEAN_SCHEMA).field("name", Schema.STRING_SCHEMA).build();

        var value = new Struct(valueSchema);
        value.put("name", "person1");
        value.put("personal", true);
        var record = new SourceRecord(Map.of(), Map.of(), "topic", valueSchema, value);

        var actual = transformer.apply(record);
        Assert.assertNull(actual);
    }

    @Test
    public void testNotFilteringRecordsOut() {
        OpaTransformer<SourceRecord> transformer = buildTransformer(SIMPLE_TEST_REGO);

        Schema valueSchema = SchemaBuilder.struct().name("test schema").field("personal", Schema.BOOLEAN_SCHEMA).field("name", Schema.STRING_SCHEMA).build();

        var value = new Struct(valueSchema);
        value.put("name", "person1");
        value.put("personal", false);
        var record = new SourceRecord(Map.of(), Map.of(), "topic", valueSchema, value);

        var actual = transformer.apply(record);
        Assert.assertEquals(actual, record);
    }

    @Test
    public void testFilteringBasedOnRecordsOnNestedFields() {
        OpaTransformer<SourceRecord> transformer = buildTransformer(BUNDLE_WITH_NESTED_OBJECT_FILTERING);

        Schema addressSchema = SchemaBuilder.struct().name("test schema")
                .field("building", Schema.INT32_SCHEMA)
                .field("street", Schema.STRING_SCHEMA)
                .field("city", Schema.STRING_SCHEMA)
                .field("personal", Schema.BOOLEAN_SCHEMA)
                .build();

        Schema valueSchema = SchemaBuilder.struct().name("test schema")
                .field("phone", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("address", addressSchema)
                .build();

        var address = new Struct(addressSchema);
        address.put("building", 131);
        address.put("street", "Hope Street");
        address.put("city", "london");
        address.put("personal", true);

        var value = new Struct(valueSchema);
        value.put("name", "person1");
        value.put("phone", "020 8765 4321");
        value.put("address", address);
        var record = new SourceRecord(Map.of(), Map.of(), "topic", valueSchema, value);

        var actual = transformer.apply(record);
        Assert.assertNull(actual);
    }

    @Test
    public void testNotFilteringBasedOnRecordsOnNestedFields() {
        OpaTransformer<SourceRecord> transformer = buildTransformer(BUNDLE_WITH_NESTED_OBJECT_FILTERING);

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
        Assert.assertEquals(actual, record);
    }

    @Test
    public void testFieldMasking() {
        OpaTransformer<SourceRecord> transformer = buildTransformer(SIMPLE_TEST_REGO);

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
        OpaTransformer<SourceRecord> transformer = buildTransformer(SIMPLE_TEST_REGO);

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

    @Test
    public void testMapFieldMasking() {
        OpaTransformer<SourceRecord> transformer = buildTransformer(SIMPLE_TEST_REGO);

        Schema addressSchema = SchemaBuilder.struct()
                .field("building", Schema.INT32_SCHEMA)
                .field("street", Schema.STRING_SCHEMA)
                .build();

        Schema valueSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, addressSchema);

        var bobAddress = new Struct(addressSchema);
        bobAddress.put("building", 131);
        bobAddress.put("street", "Hope Street");

        var stuartAddress = new Struct(addressSchema);
        stuartAddress.put("building", 222);
        stuartAddress.put("street", "Glasgow Street");

        var value = Map.of("bob", bobAddress, "stuart", stuartAddress);

        var record = new SourceRecord(Map.of(), Map.of(), "topic", valueSchema, value);

        var actual = transformer.apply(record);

        Map<String, Struct>  map = (Map<String, Struct>) actual.value();
        Struct bobActual = map.get("bob");
        Assert.assertEquals(bobActual.get("building"), 131);
        Assert.assertEquals(bobActual.get("street"), "a secret street");

        Struct stuartActual = map.get("stuart");
        Assert.assertEquals(stuartActual.get("building"), 222);
        Assert.assertEquals(stuartActual.get("street"), "Glasgow Street");
        // TODO actually test masking
    }


    @Test
    public void testArrayFieldMasking() {
        OpaTransformer<SourceRecord> transformer = buildTransformer(SIMPLE_TEST_REGO);

        Schema petSchema = SchemaBuilder.struct().name("test schema")
                .field("name", Schema.STRING_SCHEMA)
                .field("species", Schema.STRING_SCHEMA)
                .field("colour", Schema.STRING_SCHEMA)
                .build();

        Schema valueSchema = SchemaBuilder.struct().name("test schema")
                .field("phone", Schema.STRING_SCHEMA)
                .field("personal", Schema.BOOLEAN_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("pets", SchemaBuilder.array(petSchema).build())
                .build();

        var pet1 = new Struct(petSchema);
        pet1.put("name", "Simon");
        pet1.put("species", "Dog");
        pet1.put("colour", "Brown");

        var pet2 = new Struct(petSchema);
        pet2.put("name", "Sally");
        pet2.put("species", "Cat");
        pet2.put("colour", "Cream");

        var value = new Struct(valueSchema);
        value.put("name", "person1");
        value.put("personal", false);
        value.put("phone", "020 8765 4321");
        value.put("pets", Arrays.asList(pet1, pet2));
        var record = new SourceRecord(Map.of(), Map.of(), "topic", valueSchema, value);

        var actual = transformer.apply(record);
        Struct actualValue = (Struct) actual.value();
        Assert.assertEquals("person1", actualValue.get("name"));
        Assert.assertEquals(false, actualValue.get("personal"));
        Assert.assertEquals("000 0000 0000", actualValue.get("phone"));

        List<Struct> pets = (List<Struct>) actualValue.get("pets");

        Assert.assertEquals("Simon", pets.get(0).get("name"));
        Assert.assertEquals("* * * *", pets.get(0).get("species"));
        Assert.assertEquals("Brown", pets.get(0).get("colour"));

        Assert.assertEquals("Sally", pets.get(1).get("name"));
        Assert.assertEquals("* * * *", pets.get(1).get("species"));
        Assert.assertEquals("Cream", pets.get(1).get("colour"));
    }


    private static final String SIMPLE_TEST_REGO = "src/test/resources/testRego/bundle.tar.gz";
    private static final String BUNDLE_WITH_NESTED_OBJECT_FILTERING = "src/test/resources/nestedFilterRego/bundle.tar.gz";

}
