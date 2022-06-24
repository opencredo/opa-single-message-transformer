package com.opencredo.opasmt;

import io.github.sangkeon.opa.wasm.Bundle;
import io.github.sangkeon.opa.wasm.BundleUtil;
import io.github.sangkeon.opa.wasm.OPAModule;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class OpaTransformer<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String BUNDLE_PATH_FIELD_CONFIG = "bundlePath";
    public static final String FILTERING_ENTRYPOINT_CONFIG = "filteringEntrypoint";
    public static final String MASKING_ENTRYPOINT_CONFIG = "maskingEntrypoint";

    public static final ConfigDef CONFIG =
            new ConfigDef()
                    .define(BUNDLE_PATH_FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,"Path to the OPA policy bundle")
                    .define(FILTERING_ENTRYPOINT_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Entrypoint specifying whether to filter a record")
                    .define(MASKING_ENTRYPOINT_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Entrypoint specifying whether to mask a field");

    private OPAModule opaModule;
    private String opaFilteringEntrypoint;
    private String maskingEntrypoint;

    // The key should be a JSONPath?
    private final Map<String, Optional<String>> fieldToMask = new HashMap<>();

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG, props);
        String opaBundlePath = config.getString(BUNDLE_PATH_FIELD_CONFIG);
        opaFilteringEntrypoint = config.getString(FILTERING_ENTRYPOINT_CONFIG);
        maskingEntrypoint = config.getString(MASKING_ENTRYPOINT_CONFIG);

        try {
            System.out.println("Configuring OPATransformer against bundle path: " + opaBundlePath);
            Bundle bundle = BundleUtil.extractBundle(opaBundlePath);
            opaModule = new OPAModule(bundle);
        } catch (IOException e) {
            throw new RuntimeException("Error configuring OPATransformer", e);
        }

        System.out.println("OPA entrypoints available: " + opaModule.getEntrypoints());
    }

    @Override
    public R apply(R record) {
        String opaInput = recordToJson(record);
        System.out.println("** OPA filter input: " + opaInput);
        String opaResponse = opaModule.evaluate(opaInput, opaFilteringEntrypoint);
        System.out.println("** OPA filter response: " + opaResponse);

        if(shouldFilterOut(opaResponse)) {
            return null;
        }

        return applyMasking(record);
    }

    private boolean shouldFilterOut(String opaResponse) {
        // Try to avoid having to parse the String
        if(opaResponse.equals("[{\"result\":true}]")) {
            return true;
        }
        if(opaResponse.equals("[{\"result\":false}]")) {
            return false;
        }

        return OpaResultParser.parseBooleanResult(opaResponse);
    }

    private R applyMasking(R record) {
        final Struct updatedValue = new Struct(record.valueSchema());

        for (Field field : record.valueSchema().fields()) {
            Optional<String> mask = getMask(field);
            if(mask.isPresent()) {
                updatedValue.put(field.name(), mask.get());
            } else {
                updatedValue.put(field.name(), getValue(record, field));
            }
        }

        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), updatedValue, null);
    }

    // None means "do not mask this field"
    private Optional<String> getMask(Field field) {
        Optional<String> mask = fieldToMask.get(field.name());
        if (mask!=null) {
            return mask;
        }

        String requestJson = "{ \"fieldName\": \"" +field.name()+ "\" }";

        String maskingRawResp = opaModule.evaluate(requestJson, maskingEntrypoint);

        System.out.println("OPA masking response: " + maskingRawResp);
        Optional<String> masking = Optional.ofNullable(OpaResultParser.parseStringResult(maskingRawResp));

        fieldToMask.put(field.name(), masking);
        return masking;
    }

    private Object getValue(R record, Field field) {
        if(record.value() instanceof Map r) {
            return r.get(field.name());
        }
        if(record.value() instanceof Struct r) {
            return r.get(field);
        }
        throw new IllegalArgumentException("Unable to get a value from record of type "+ record.getClass().getName());
    }

    private String recordToJson(R record) {
        var value = record.value();
        System.out.println("recordToJson: " + value.getClass().getName() + ": " + value);
        Struct valueStruct = (Struct) value;

        var valueSchema = record.valueSchema();
        for(Field field : valueSchema.fields()) {
            System.out.println(field.name() + " type: " + field.schema().getClass().getName() +  " " + field.schema().type() + " val: "+ valueStruct.get(field));
        }

        var fields = valueSchema.fields().stream().map( field -> {
            StringBuilder fieldString = new StringBuilder();
            fieldString.append('\"').append(field.name()).append("\": ");

            if (field.schema().type() == Schema.Type.STRING) {
                fieldString.append('"').append(valueStruct.get(field)).append('"');
            } else {
                fieldString.append(valueStruct.get(field));
            }
            return fieldString.toString();
        }).collect(Collectors.joining(", "));

        return "{ " + fields + " }";
    }

    @Override
    public ConfigDef config() {
        return CONFIG;
    }

    @Override public void close() {
        opaModule.close();
    }

}