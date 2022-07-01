package com.opencredo.opasmt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

public class OpaTransformer<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String BUNDLE_FILE_FIELD_CONFIG = "bundlePath";
    public static final String BUNDLE_URI_FIELD_CONFIG = "bundleUri";
    public static final String FILTERING_ENTRYPOINT_CONFIG = "filteringEntrypoint";
    public static final String MASKING_ENTRYPOINT_CONFIG = "maskingEntrypoint";

    public static final ConfigDef CONFIG =
            new ConfigDef()
                    .define(BUNDLE_FILE_FIELD_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "File path of the OPA policy bundle")
                    .define(BUNDLE_URI_FIELD_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "URI of the OPA policy bundle")
                    .define(FILTERING_ENTRYPOINT_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Entrypoint specifying whether to filter a record")
                    .define(MASKING_ENTRYPOINT_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Entrypoint specifying whether to mask a field");

    private OpaClient opaClient;

    @Override
    public void configure(Map<String, ?> props) {
        var config = new SimpleConfig(CONFIG, props);
        String bundleFile = config.getString(BUNDLE_FILE_FIELD_CONFIG);
        String bundleUri = config.getString(BUNDLE_URI_FIELD_CONFIG);
        if (bundleFile==null && bundleUri==null) {
            throw new IllegalArgumentException("Either the "+BUNDLE_FILE_FIELD_CONFIG+" or "+BUNDLE_URI_FIELD_CONFIG+" parameter must be provided.");
        }
        if (bundleFile!=null && bundleUri!=null) {
            throw new IllegalArgumentException("Both the "+BUNDLE_FILE_FIELD_CONFIG+" and "+BUNDLE_URI_FIELD_CONFIG+" parameters cannot be provided simultaneously.");
        }

        try {
            BundleSource bundleSource;
            if(bundleFile!=null) {
                bundleSource = new FileBundleSource(new File(bundleFile));
            } else {
                bundleSource = new URIBundleSource(bundleUri);
            }

            opaClient = new OpaClient(bundleSource, config.getString(FILTERING_ENTRYPOINT_CONFIG), config.getString(MASKING_ENTRYPOINT_CONFIG));
            bundleSource.addBundleChangeListener(opaClient);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public R apply(R record) {
        if(opaClient.shouldFilterOut(record)) {
            return null;
        }

        return applyMasking(record);
    }

    private R applyMasking(R record) {
        Object maskedValue = maskRecursively(record.valueSchema(), record.value(), "");
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), maskedValue, null);
    }

    private Object maskRecursively(Schema valueSchema, Object value, String prefix) {
        if (valueSchema.type() == Schema.Type.STRUCT) {
            final Struct maskedObject = new Struct(valueSchema);
            for (Field field : valueSchema.fields()) {
                maskedObject.put(field.name(), maskRecursively(field.schema(), getValue(value, field), (prefix.length() == 0 ? "" : prefix + ".") + field.name()));
            }
            return maskedObject;
        } else if (valueSchema.type().equals(Schema.Type.ARRAY)) {
            List<Object> unmasked = (List<Object>) value;
            return unmasked.stream()
                    .map(u -> maskRecursively(valueSchema.valueSchema(), u, prefix + "[*]"))
                    .collect(Collectors.toList());
        } else if (valueSchema.type() == Schema.Type.MAP) {
            Map<String,Object> unmasked = (Map<String,Object>) value;
            return unmasked.entrySet().stream()
                    .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), maskRecursively(valueSchema.valueSchema(), entry.getValue(), prefix+"['"+entry.getKey()+"']")))
                    .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
        } else {
            Optional<String> mask = getMask(prefix);
            if (mask.isPresent()) {
                return mask.get();
            } else {
                return value;
            }
        }
    }

    private Optional<String> getMask(String fieldName) {
        return opaClient.getMaskingReplacement(fieldName);
    }

    private Object getValue(Object value, Field field) {
        if(value instanceof Map r) {
            return r.get(field.name());
        }
        if(value instanceof Struct r) {
            return r.get(field);
        }
        throw new IllegalArgumentException("Unable to get a value from record of type "+ value.getClass().getName());
    }

    @Override
    public ConfigDef config() {
        return CONFIG;
    }

    @Override
    public void close() {
        opaClient.close();
    }

}