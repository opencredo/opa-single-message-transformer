package com.opencredo.opasmt;

import io.github.sangkeon.opa.wasm.Bundle;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class RemoteBundleFetcher {

    public static Bundle extractBundle(URI uri) throws IOException {
        HttpClient client = HttpClient.newHttpClient();

        HttpRequest request = HttpRequest.newBuilder().uri(uri).build();
        HttpResponse<byte[]> resp = client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                .join();
        byte[] body = resp.body();

        Bundle bundle = new Bundle();

        try (InputStream fi = new ByteArrayInputStream(body);
             InputStream bi = new BufferedInputStream(fi);
             InputStream gzi = new GzipCompressorInputStream(bi);
             ArchiveInputStream i = new TarArchiveInputStream(gzi)
        ) {
            ArchiveEntry entry;
            while ((entry = i.getNextEntry()) != null) {
                if (!i.canReadEntryData(entry)) {
                    continue;
                }

                if("/policy.wasm".equals(entry.getName())) {
                    bundle.setPolicy(IOUtils.toByteArray(i));
                } else if("/data.json".equals(entry.getName())) {
                    bundle.setData(new String(IOUtils.toByteArray(i)));
                }
            }
        }

        return bundle;
    }

}
