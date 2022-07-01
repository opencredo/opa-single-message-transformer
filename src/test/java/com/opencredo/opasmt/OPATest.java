package com.opencredo.opasmt;

import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;

public class OPATest {

    public static OpaTransformer<SourceRecord> buildTransformer(String testBundle) {
        var properties = Map.of(
                OpaTransformer.BUNDLE_PATH_FIELD_CONFIG, testBundle,
                OpaTransformer.FILTERING_ENTRYPOINT_CONFIG, "kafka/filter",
                OpaTransformer.MASKING_ENTRYPOINT_CONFIG, "kafka/maskingConfig"
        );

        var transformer = new OpaTransformer<SourceRecord>();
        transformer.configure(properties);
        return transformer;
    }
}
