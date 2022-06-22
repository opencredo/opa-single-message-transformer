package com.opencredo.opasmt;

import io.github.sangkeon.opa.wasm.Bundle;
import io.github.sangkeon.opa.wasm.BundleUtil;
import io.github.sangkeon.opa.wasm.OPAModule;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

public class OpaTransformer<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OPA_BUNDLE_PATH_FIELD = "opaBundlePath";

    public static final ConfigDef CONFIG =
            new ConfigDef().define(OPA_BUNDLE_PATH_FIELD, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                    "Path to the OPA policy bundle");

    private OPAModule opaModule;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG, props);
        String opaBundlePath = config.getString(OPA_BUNDLE_PATH_FIELD);

        try {
            System.out.println("**Loading bundle from " + opaBundlePath);
            Bundle bundle = BundleUtil.extractBundle(opaBundlePath);
            opaModule = new OPAModule(bundle);
            System.out.println("** opaModule set to "+opaModule);
            System.out.println("Entrypoints: " + opaModule.getEntrypoints().keySet());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public R apply(R record) {
        String opaInput = recordToJson(record);
        System.out.println("** OPA input: " + opaInput);
        String opaResponse = opaModule.evaluate(opaInput, "kafka/filter");
        System.out.println("** OPA response: " + opaResponse);

        var falseStr = "[{\"result\":false}]";
        var trueStr = "[{\"result\":true}]";
        if(opaResponse.equals(falseStr)) {
            System.out.println("returning record");
            return record;
        }
        if(opaResponse.equals(trueStr)) {
            System.out.println("returning null");
            return null;
        }
        throw new IllegalStateException();
    }

    private String recordToJson(R record) {
        // TODO understand what this purpose field is as the 2nd argument
        //var values = requireMap(record, "Filtering objects");
        SourceRecord sourceRecord =(SourceRecord)record;
        Object value = sourceRecord.value();
        System.out.println("recordToJson: " + value.getClass().getName() + ": " + value);
        Struct valueStruct = (Struct) value;

        var valueSchema = record.valueSchema();
        for(Field field : valueSchema.fields()) {
            System.out.println(field.name() + " type: " + field.schema().getClass().getName() +  " " + field.schema().type() + " val: "+ valueStruct.get(field));
        }

        StringBuilder ret = new StringBuilder();

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

        ret.append("{ ");
        ret.append(fields);
        ret.append("}");
        return ret.toString();
    }


    @Override
    public ConfigDef config() {
        return CONFIG;
    }

    @Override public void close() {}

}