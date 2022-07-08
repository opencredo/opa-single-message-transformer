package com.opencredo.opasmt;

import org.apache.kafka.connect.source.SourceRecord;

import java.net.URI;
import java.util.Map;

public class OPATest {

    public static OpaTransformer<SourceRecord> buildTransformer(String testBundle) {
        var properties = Map.of(
                OpaTransformer.BUNDLE_FILE_FIELD_CONFIG, testBundle,
                OpaTransformer.FILTERING_ENTRYPOINT_CONFIG, "kafka/filter",
                OpaTransformer.MASKING_ENTRYPOINT_CONFIG, "kafka/maskingByField"
        );

        var transformer = new OpaTransformer<SourceRecord>();
        transformer.configure(properties);
        return transformer;
    }

    public static OpaTransformer<SourceRecord> buildTransformer(URI testBundle) {
        var properties = Map.of(
                OpaTransformer.BUNDLE_URI_FIELD_CONFIG, testBundle.toString(),
                OpaTransformer.FILTERING_ENTRYPOINT_CONFIG, "kafka/filter",
                OpaTransformer.MASKING_ENTRYPOINT_CONFIG, "kafka/maskingByField",
                OpaTransformer.POLL_BUNDLE_URI_FREQUENCY_SECONDS, 2
        );

        var transformer = new OpaTransformer<SourceRecord>();
        transformer.configure(properties);
        return transformer;
    }
}
