package com.opencredo.opasmt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.util.resource.PathResource;
import org.eclipse.jetty.util.resource.Resource;
import org.junit.Assert;
import org.junit.Test;
import java.io.File;
import java.net.URI;
import java.util.Map;

import static com.opencredo.opasmt.OPATest.buildTransformer;

public class TestOpaTransformerWithRemoteBundle {

    @Test
    public void testRemoteBundle() throws Exception {
        Server server = createWebserver(9999, new PathResource(new File("src/test/resources/")));
        server.start();
        try {
            OpaTransformer<SourceRecord> transformer = buildTransformer(new URI("http://localhost:9999/testRego/bundle.tar.gz"));

            Schema valueSchema = SchemaBuilder.struct().name("test schema").field("personal", Schema.BOOLEAN_SCHEMA).field("name", Schema.STRING_SCHEMA).build();

            var value = new Struct(valueSchema);
            value.put("name", "person1");
            value.put("personal", true);
            var record = new SourceRecord(Map.of(), Map.of(), "topic", valueSchema, value);

            var actual = transformer.apply(record);
            Assert.assertNull(actual);
        } finally {
            server.stop();
        }
    }

    private static Server createWebserver(int port, Resource baseResource) {
        Server server = new Server(port);
        ResourceHandler resourceHandler = new ResourceHandler();
        resourceHandler.setDirectoriesListed(true);
        resourceHandler.setWelcomeFiles(new String[]{"index.html"});
        resourceHandler.setBaseResource(baseResource);
        HandlerList handlers = new HandlerList();
        handlers.setHandlers(new Handler[]{resourceHandler, new DefaultHandler()});
        server.setHandler(handlers);

        return server;
    }
}
