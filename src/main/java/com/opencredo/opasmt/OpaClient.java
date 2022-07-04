package com.opencredo.opasmt;

import com.opencredo.opasmt.bundlesource.BundleChangeListener;
import com.opencredo.opasmt.bundlesource.BundleSource;
import io.github.sangkeon.opa.wasm.OPAModule;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class OpaClient implements BundleChangeListener {

    Logger logger = LoggerFactory.getLogger(OpaClient.class);

    private final BundleSource bundleSource;
    private final String opaFilteringEntrypoint;
    private final String maskingEntrypoint;

    private final Object lock = new Object();
    // state protected by the lock
    private OPAModule opaModule;
    private final Map<String, Optional<String>> fieldPathToOptionalMaskCache = new HashMap<>();

    public OpaClient(BundleSource bundleSource, String opaFilteringEntrypoint, String maskingEntrypoint) throws IOException {
        this.bundleSource = bundleSource;
        this.opaFilteringEntrypoint = opaFilteringEntrypoint;
        this.maskingEntrypoint = maskingEntrypoint;

        restartWithNewBundleContents();

        logger.info("OPA entrypoints available: " + opaModule.getEntrypoints());
    }

    private void restartWithNewBundleContents() throws IOException {
        synchronized (lock) {
            fieldPathToOptionalMaskCache.clear();
            opaModule = new OPAModule(bundleSource.getBundle());
        }
    }

    @Override
    public void bundleChanged() {
        logger.info("Reloading bundle file");
        try {
            restartWithNewBundleContents();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public  boolean shouldFilterOut(ConnectRecord<?> record) {
        String opaInput = ConnectRecordToJson.convertRecord(record);
        logger.debug("OPA filter input: " + opaInput);

        String opaResponse;
        synchronized (lock) {
            opaResponse = opaModule.evaluate(opaInput, opaFilteringEntrypoint);
        }

        logger.debug("OPA filter response: " + opaResponse);
        return OpaResultParser.parseBooleanResult(opaResponse);
    }

    public Optional<String> getMaskingReplacement(String fieldName) {
        synchronized (lock) {
            Optional<String> mask = fieldPathToOptionalMaskCache.get(fieldName);
            if (mask != null) {
                return mask;
            }

            String requestJson = "{ \"fieldName\": \"" + fieldName + "\" }";
            String maskingRawResult = opaModule.evaluate(requestJson, maskingEntrypoint);
            Optional<String> masking = Optional.ofNullable(OpaResultParser.parseStringResult(maskingRawResult));
            fieldPathToOptionalMaskCache.put(fieldName, masking);

            logger.debug("OPA masking response: " + masking + " for request " + requestJson);
            return masking;
        }
    }

    public void close() {
        synchronized (lock) {
            opaModule.close();
        }
    }
}
