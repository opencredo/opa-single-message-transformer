package com.opencredo.opasmt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class OpaTransformer<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String BUNDLE_PATH_FIELD_CONFIG = "bundlePath";
    public static final String FILTERING_ENTRYPOINT_CONFIG = "filteringEntrypoint";
    public static final String MASKING_ENTRYPOINT_CONFIG = "maskingEntrypoint";

    public static final ConfigDef CONFIG =
            new ConfigDef()
                    .define(BUNDLE_PATH_FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,"Path to the OPA policy bundle")
                    .define(FILTERING_ENTRYPOINT_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Entrypoint specifying whether to filter a record")
                    .define(MASKING_ENTRYPOINT_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Entrypoint specifying whether to mask a field");

    private OpaClient opaClient;

    // The key should be a JSONPath?
    private final Map<String, Optional<String>> fieldToMask = new HashMap<>();

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG, props);
        opaClient = new OpaClient(config.getString(BUNDLE_PATH_FIELD_CONFIG), config.getString(FILTERING_ENTRYPOINT_CONFIG), config.getString(MASKING_ENTRYPOINT_CONFIG));
    }

    @Override
    public R apply(R record) {
        if(opaClient.shouldFilterOut(record)) {
            return null;
        }

        return applyMasking(record);
    }

    private R applyMasking(R record) {
        Object maskedValue = maskInternal(record.valueSchema(), record.value(), "");
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), maskedValue, null);
    }

    private Object maskInternal(Schema valueSchema, Object value, String prefixThenDot) {
        System.out.println("maskInternal called with prefixThenDot "+prefixThenDot);
        final Struct maskedObject = new Struct(valueSchema);
        for (Field field : valueSchema.fields()) {
            if(field.schema().type()== Schema.Type.STRUCT) {
                maskedObject.put(field.name(), maskInternal(field.schema(), getValue(value, field), prefixThenDot + field.name()+"."));
            } else {
                Optional<String> mask = getMask(prefixThenDot + field.name());
                System.out.println("Mask for field " + (prefixThenDot + field.name()) + " is " + mask);
                if (mask.isPresent()) {
                    maskedObject.put(field.name(), mask.get());
                } else {
                    maskedObject.put(field.name(), getValue(value, field));
                }
            }
        }
        System.out.println("maskInternal returning "+maskedObject);
        return maskedObject;
    }

    // None means "do not mask this field"
    private Optional<String> getMask(String fieldName) {
        Optional<String> mask = fieldToMask.get(fieldName);
        if (mask!=null) {
            return mask;
        }

        Optional<String> masking = opaClient.getMaskingReplacement(fieldName);
        fieldToMask.put(fieldName, masking);
        return masking;
    }

    private Object getValue(Object value, Field field) {
        if(value instanceof Map r) {
            return r.get(field.name());
        }
        if(value instanceof Struct r) {
            return r.get(field);
        }
        throw new IllegalArgumentException("Unable to get a value from record of type "+ value.getClass().getName());
    }

    @Override
    public ConfigDef config() {
        return CONFIG;
    }

    @Override
    public void close() {
        opaClient.close();
    }

}