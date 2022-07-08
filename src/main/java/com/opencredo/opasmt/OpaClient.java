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
    private Map<String, String> fieldPathToOptionalMaskCache = new HashMap<>();

    public OpaClient(BundleSource bundleSource, String opaFilteringEntrypoint, String maskingEntrypoint) throws IOException {
        this.bundleSource = bundleSource;
        this.opaFilteringEntrypoint = opaFilteringEntrypoint;
        this.maskingEntrypoint = maskingEntrypoint;

        restartWithNewBundleContents();

        logger.info("OPA entrypoints available: " + opaModule.getEntrypoints());
    }

    private void restartWithNewBundleContents() throws IOException {
        synchronized (lock) {
            opaModule = new OPAModule(bundleSource.getBundle());
            fieldPathToOptionalMaskCache = collectFieldMaskingConfig();
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

    private Map<String,String> collectFieldMaskingConfig() {
        String maskingRawResult = opaModule.evaluate("{}", maskingEntrypoint);
        Map<String, Object> stringObjectMap = OpaResultParser.parseMap(maskingRawResult);

        Map<String, String> fieldMaskingConfig = new HashMap<>();

        for(Map.Entry<String,Object> entry : stringObjectMap.entrySet()) {
            String fieldName = entry.getKey();
            Object maskingForField = entry.getValue();

            if (maskingForField instanceof String s) {
                fieldMaskingConfig.put(fieldName, s);
            } else {
                throw new IllegalArgumentException("Field " + fieldName + " is of type " + maskingForField.getClass().getName() + " when it must be a String");
            }
        }
        return fieldMaskingConfig;
    }

    public boolean shouldFilterOut(ConnectRecord<?> record) {
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
            return Optional.ofNullable(fieldPathToOptionalMaskCache.get(fieldName));
        }
    }

    public void close() {
        synchronized (lock) {
            opaModule.close();
        }
    }
}
