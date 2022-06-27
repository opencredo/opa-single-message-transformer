package com.opencredo.opasmt;

import io.github.sangkeon.opa.wasm.Bundle;
import io.github.sangkeon.opa.wasm.BundleUtil;
import io.github.sangkeon.opa.wasm.OPAModule;
import org.apache.kafka.connect.connector.ConnectRecord;

import java.io.IOException;
import java.util.Optional;

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
        String opaInput = ConnectRecordToJson.convertRecord(record);
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



    public void close() {
        opaModule.close();
    }
}
