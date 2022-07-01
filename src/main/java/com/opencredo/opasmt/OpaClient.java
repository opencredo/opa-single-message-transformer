package com.opencredo.opasmt;

import io.github.sangkeon.opa.wasm.BundleUtil;
import io.github.sangkeon.opa.wasm.OPAModule;
import org.apache.kafka.connect.connector.ConnectRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

public class OpaClient {

    private File opaBundleFile;
    private final String opaFilteringEntrypoint;
    private final String maskingEntrypoint;

    private final Object lock = new Object();
    // state protected by the lock
    private OPAModule opaModule;
    private final Map<String, Optional<String>> fieldPathToOptionalMaskCache = new HashMap<>();
    private volatile boolean closed = false;

    public OpaClient(String opaBundlePath, String opaFilteringEntrypoint, String maskingEntrypoint) throws IOException {
        this.opaBundleFile = new File(opaBundlePath);
        this.opaFilteringEntrypoint = opaFilteringEntrypoint;
        this.maskingEntrypoint = maskingEntrypoint;

        restartWithNewBundleContents();
        listenForFileSystemChanges();

        System.out.println("OPATransformer running against bundle path: " + opaBundlePath);
        System.out.println("OPA entrypoints available: " + opaModule.getEntrypoints());
    }

    private void restartWithNewBundleContents() throws IOException {
        synchronized (lock) {
            fieldPathToOptionalMaskCache.clear();
            opaModule = new OPAModule(BundleUtil.extractBundle(opaBundleFile.getAbsolutePath()));
        }
    }

    private void listenForFileSystemChanges() {
        new Thread(runnable).start();
    }

    private final Runnable runnable = () -> {
        try {
            var bundleFileParentDir = opaBundleFile.toPath().getParent();
            WatchService watchService = FileSystems.getDefault().newWatchService();
            bundleFileParentDir.register(watchService, ENTRY_MODIFY);

            while (!closed) {
                WatchKey key = watchService.take();
                for (WatchEvent<?> event : key.pollEvents()) {
                    Path changedFilePath = (Path) event.context();
                    if(changedFilePath.getFileName().toString().equals(opaBundleFile.getName())) {
                        reloadBundleFile();
                    }
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    };

    private void reloadBundleFile() {
        System.out.println("Reloading bundle file");
        try {
            restartWithNewBundleContents();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public  boolean shouldFilterOut(ConnectRecord<?> record) {
        String opaInput = ConnectRecordToJson.convertRecord(record);
//        System.out.println("** OPA filter input: " + opaInput);

        String opaResponse;
        synchronized (lock) {
            opaResponse = opaModule.evaluate(opaInput, opaFilteringEntrypoint);
        }

//        System.out.println("** OPA filter response: " + opaResponse);
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

            System.out.println("OPA masking response: " + masking + " for request " + requestJson);
            return masking;
        }
    }

    public void close() {
        synchronized (lock) {
            opaModule.close();
            closed = true;
        }
    }
}
