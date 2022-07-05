package com.opencredo.opasmt.bundlesource;

import io.github.sangkeon.opa.wasm.Bundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;


public class URIBundleSource implements BundleSource {

    private static final Logger logger = LoggerFactory.getLogger(URIBundleSource.class);

    private final URI bundleUri;
    private final Timer timer = new Timer();

    private final Object lock = new Object();
    private final List<BundleChangeListener> listeners = new ArrayList<>();
    private Bundle bundle;


    public URIBundleSource(String bundleUri, int pollFrequencyInSeconds) throws IOException, URISyntaxException{
        this.bundleUri = new URI(bundleUri);

        synchronized (lock) {
            bundle = readBundleFromHost();
        }

        if(pollFrequencyInSeconds>0) {
            long pollFrequencyInMs = pollFrequencyInSeconds * 1000L;
            timer.schedule(updateBundleAndNotifyListenersTask, pollFrequencyInMs, pollFrequencyInMs);
            logger.info("Scheduled polling task every " + pollFrequencyInSeconds + " seconds");
        }
    }

    @Override
    public Bundle getBundle() {
        synchronized (lock) {
            return bundle;
        }
    }

    @Override
    public void addBundleChangeListener(BundleChangeListener listener) {
        synchronized (lock) {
            listeners.add(listener);
        }
    }

    private Bundle readBundleFromHost() throws IOException {
        logger.info("Reading bundle from " + bundleUri);
        return RemoteBundleFetcher.extractBundle(bundleUri);
    }

    private final TimerTask updateBundleAndNotifyListenersTask = new TimerTask() {
        public void run() {
            try {
                Bundle oldBundle = bundle;
                Bundle newBundle = readBundleFromHost();

                synchronized (lock) {
                    bundle = newBundle;
                    if (!bundlesAreEqual(oldBundle, bundle)) {
                        for (BundleChangeListener listener : listeners) {
                            listener.bundleChanged();
                        }
                    }
                }
            } catch (IOException e) {
                logger.error("Error polling for bundle change", e);
            }
        }
    };

    private static boolean bundlesAreEqual(Bundle a, Bundle b) {
        return a.getData().equals(b.getData()) && Arrays.equals(a.getPolicy(), b.getPolicy());
    }
}
