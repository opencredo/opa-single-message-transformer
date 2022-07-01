package com.opencredo.opasmt;

import io.github.sangkeon.opa.wasm.Bundle;

import java.io.IOException;

public interface BundleSource {
    Bundle getBundle() throws IOException;

    void addBundleChangeListener(BundleChangeListener listener);
}
