package com.opencredo.opasmt.bundlesource;

import io.github.sangkeon.opa.wasm.Bundle;

import java.io.IOException;

public interface BundleSource {
    Bundle getBundle() throws IOException;

    void addBundleChangeListener(BundleChangeListener listener);
}
