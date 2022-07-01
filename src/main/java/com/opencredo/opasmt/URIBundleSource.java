package com.opencredo.opasmt;

import io.github.sangkeon.opa.wasm.Bundle;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

// TODO listen to bundle changes - by polling?
public class URIBundleSource implements BundleSource {

    private final String bundleUri;

    public URIBundleSource(String bundleUri) {
        this.bundleUri = bundleUri;
    }

    @Override
    public Bundle getBundle() throws IOException {
        try {
            return RemoteBundleFetcher.extractBundle(new URI(bundleUri));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addBundleChangeListener(BundleChangeListener listener) {
    }
}
