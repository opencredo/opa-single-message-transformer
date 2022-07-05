package com.opencredo.opasmt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;

import static com.opencredo.opasmt.OPATest.buildTransformer;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

public class TestBundleUpdateHandling {

    private static final String REGO_V1 = "src/test/resources/testBundleUpdateHandling/v1/bundle.tar.gz";
    private static final String REGO_V2 = "src/test/resources/testBundleUpdateHandling/v2/bundle.tar.gz";

    @Rule
    public TemporaryFolder folder= new TemporaryFolder();

    @Test
    public void testBundleUpdateHandling() throws Exception {
        File tempFolder = folder.newFolder();
        Path controllerBundlePath = tempFolder.toPath().resolve("bundle.tar.gz");

        copyRegoFileToTestLocation(REGO_V1, controllerBundlePath);

        OpaTransformer<SourceRecord> transformer = buildTransformer(controllerBundlePath.toString());

        final var schema = SchemaBuilder.struct().field("version", Schema.STRING_SCHEMA).build();
        final var struct = new Struct(schema);
        struct.put("version", "I have not been masked");
        final var record = new SourceRecord(Map.of(), Map.of(), "topic", schema, struct);

        final var transformedWithPolicyV1 = transformer.apply(record);
        Assert.assertEquals("v1", ((Struct) transformedWithPolicyV1.value()).get("version"));

        copyRegoFileToTestLocation(REGO_V2, controllerBundlePath);

        await().atMost(20, SECONDS).until(() -> {
            SourceRecord transformedWithPolicyV2 = transformer.apply(record);
            return "v2".equals(((Struct) transformedWithPolicyV2.value()).get("version"));
        });
    }

    private void copyRegoFileToTestLocation(String nameOfFileToCopy, Path destinationFilePath) throws IOException {
        Path simpleTestRegoPath = new File(nameOfFileToCopy).toPath();
        Files.copy(simpleTestRegoPath, destinationFilePath, StandardCopyOption.REPLACE_EXISTING);
    }

}
