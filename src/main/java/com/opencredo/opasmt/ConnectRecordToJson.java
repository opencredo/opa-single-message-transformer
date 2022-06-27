package com.opencredo.opasmt;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

public class ConnectRecordToJson {

    public static String convert(ConnectRecord<?> record) {
        var value = record.value();
        JSONObject ret = structToJson(record.valueSchema(), (Struct) value);
        System.out.println("recordToJson: " + value + " transformed to "+ret);
        return ret.toString();
    }

    private static JSONObject structToJson(Schema schema, Struct value) {
        Map<String, Object> fieldsMap = new HashMap<>();

        for(Field field : schema.fields()) {
            if (field.schema().type() == Schema.Type.STRUCT) {
                fieldsMap.put(field.name(), structToJson(field.schema(), (Struct) value.get(field)));
            } else {
                fieldsMap.put(field.name(), value.get(field));
            }
        }

        return new JSONObject(fieldsMap);
    }

}
