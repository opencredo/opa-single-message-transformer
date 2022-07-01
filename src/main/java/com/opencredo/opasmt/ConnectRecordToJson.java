package com.opencredo.opasmt;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ConnectRecordToJson {

    public static String convertRecord(ConnectRecord<?> record) {
        var value = record.value();
        Object ret = convert(record.valueSchema(), value);
//        System.out.println("recordToJson: " + value + " transformed to "+ret);
        return ret.toString();
    }

    private static Object convert(Schema schema, Object value) {
        if (schema.type() == Schema.Type.STRUCT) {
            Map<String, Object> fieldsMap = new HashMap<>();
            for(Field field : schema.fields()) {
                fieldsMap.put(field.name(), convert(field.schema(), ((Struct) value).get(field)));
            }
            return new JSONObject(fieldsMap);
        } else if (schema.type() == Schema.Type.ARRAY) {
            List<Object> in = (List) value;
            JSONArray array = new JSONArray(in.stream().map(i -> convert(schema.valueSchema(), i)).collect(Collectors.toList()));
            return array;
        } else {
            return value;
        }
    }

}
