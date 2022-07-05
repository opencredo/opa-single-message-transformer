package com.opencredo.opasmt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.util.resource.PathResource;
import org.eclipse.jetty.util.resource.Resource;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;

import static com.opencredo.opasmt.OPATest.buildTransformer;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

public class TestOpaTransformerWithRemoteBundle {

    private static final String REGO_V1 = "src/test/resources/filterPersonalMaskNothing/bundle.tar.gz";
    private static final String REGO_V2 = "src/test/resources/filterNothingMaskNothing/bundle.tar.gz";

    @Rule
    public TemporaryFolder folder= new TemporaryFolder(new File("target"));

    @Test
    public void testRemoteBundle() throws Exception {
        folder.create();
        File tempFolder = folder.getRoot();

        Path controllerBundlePath = tempFolder.toPath().resolve("bundle.tar.gz");

        copyRegoFileToTestLocation(REGO_V1, controllerBundlePath);

        Server server = createWebserver(9999, new PathResource(tempFolder));
        server.start();

        try {
            String uri = "http://localhost:9999/bundle.tar.gz";
            OpaTransformer<SourceRecord> transformer = buildTransformer(new URI(uri));
            Schema valueSchema = SchemaBuilder.struct().name("test schema").field("personal", Schema.BOOLEAN_SCHEMA).field("name", Schema.STRING_SCHEMA).build();

            var value = new Struct(valueSchema);
            value.put("name", "person1");
            value.put("personal", true);
            var record = new SourceRecord(Map.of(), Map.of(), "topic", valueSchema, value);

            var actual = transformer.apply(record);
            Assert.assertNull(actual);

            copyRegoFileToTestLocation(REGO_V2, controllerBundlePath);

            await().atMost(10, SECONDS).until(() -> {
                SourceRecord transformedWithPolicyV2 = transformer.apply(record);
                return transformedWithPolicyV2 != null;
            });
        } finally {
            server.stop();
        }
    }

    private static Server createWebserver(int port, Resource baseResource) {
        Server server = new Server(port);
        ResourceHandler resourceHandler = new ResourceHandler();
        resourceHandler.setBaseResource(baseResource);
        resourceHandler.setDirAllowed(true);
        resourceHandler.setDirectoriesListed(true);
        server.setHandler(new HandlerList(resourceHandler, new DefaultHandler()));
        return server;
    }

    private void copyRegoFileToTestLocation(String nameOfFileToCopy, Path destinationFilePath) throws IOException {
        Path simpleTestRegoPath = new File(nameOfFileToCopy).toPath();
        Files.copy(simpleTestRegoPath, destinationFilePath, StandardCopyOption.REPLACE_EXISTING);
    }
}
