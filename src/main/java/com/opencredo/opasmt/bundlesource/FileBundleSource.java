package com.opencredo.opasmt.bundlesource;

import io.github.sangkeon.opa.wasm.Bundle;
import io.github.sangkeon.opa.wasm.BundleUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

public class FileBundleSource implements BundleSource {

    Logger logger = LoggerFactory.getLogger(FileBundleSource.class);

    private File opaBundleFile;
    private final List<BundleChangeListener> bundleChangeListeners = new ArrayList<>();

    public FileBundleSource(File file) {
        logger.info("OPATransformer running against bundle path: " + file.getAbsolutePath());
        this.opaBundleFile = file;
        new Thread(runnable).start();
    }

    @Override
    public Bundle getBundle() throws IOException {
        return BundleUtil.extractBundle(opaBundleFile.getAbsolutePath());
    }

    @Override
    public void addBundleChangeListener(BundleChangeListener listener) {
        bundleChangeListeners.add(listener);
    }

    private final Runnable runnable = () -> {
        try {
            var bundleFileParentDir = opaBundleFile.toPath().getParent();
            WatchService watchService = FileSystems.getDefault().newWatchService();
            bundleFileParentDir.register(watchService, ENTRY_MODIFY);

            while (true) {
                WatchKey key = watchService.take();
                for (WatchEvent<?> event : key.pollEvents()) {
                    Path changedFilePath = (Path) event.context();
                    if(changedFilePath.getFileName().toString().equals(opaBundleFile.getName())) {
                        for(BundleChangeListener listener : bundleChangeListeners) {
                            listener.bundleChanged();
                        }
                    }
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    };

}
