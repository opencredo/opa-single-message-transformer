package com.opencredo.opasmt;

import io.github.sangkeon.opa.wasm.Bundle;
import io.github.sangkeon.opa.wasm.BundleUtil;
import io.github.sangkeon.opa.wasm.OPAModule;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.Collectors;

public class OpaClient {

    private final OPAModule opaModule;
    private final String opaFilteringEntrypoint;
    private final String maskingEntrypoint;

    public OpaClient(String opaBundlePath, String opaFilteringEntrypoint, String maskingEntrypoint) {
        this.opaFilteringEntrypoint = opaFilteringEntrypoint;
        this.maskingEntrypoint = maskingEntrypoint;

        try {
            System.out.println("Configuring OPATransformer against bundle path: " + opaBundlePath);
            Bundle bundle = BundleUtil.extractBundle(opaBundlePath);
            opaModule = new OPAModule(bundle);
        } catch (IOException e) {
            throw new RuntimeException("Error configuring OPATransformer", e);
        }

        System.out.println("OPA entrypoints available: " + opaModule.getEntrypoints());
    }


    public  boolean shouldFilterOut(ConnectRecord<?> record) {
        String opaInput = recordToJson(record);
        System.out.println("** OPA filter input: " + opaInput);
        String opaResponse = opaModule.evaluate(opaInput, opaFilteringEntrypoint);
        System.out.println("** OPA filter response: " + opaResponse);
        return OpaResultParser.parseBooleanResult(opaResponse);
    }

    public Optional<String> getMaskingReplacement(String fieldName) {
        String requestJson = "{ \"fieldName\": \"" +fieldName+ "\" }";
        String maskingRawResp = opaModule.evaluate(requestJson, maskingEntrypoint);

        System.out.println("OPA masking response: " + maskingRawResp + " for request "+requestJson);
        return Optional.ofNullable(OpaResultParser.parseStringResult(maskingRawResp));
    }

    private String recordToJson(ConnectRecord<?> record) {
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

    public void close() {
        opaModule.close();
    }
}
