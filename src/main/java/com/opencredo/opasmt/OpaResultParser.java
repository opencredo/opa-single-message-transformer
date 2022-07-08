package com.opencredo.opasmt;

import org.json.JSONArray;

import java.util.Map;

public class OpaResultParser {

    public static String parseStringResult(String in) {
        JSONArray arr = new JSONArray(in);
        var obj = arr.getJSONObject(0);
        var resultArray = obj.getJSONArray("result");
        if(resultArray.length()==0) {
            return null;
        }
        return resultArray.getString(0);
    }

    public static boolean parseBooleanResult(String in) {
        JSONArray arr = new JSONArray(in);
        var obj = arr.getJSONObject(0);
        return obj.getBoolean("result");
    }

    // "[{\"result\":{\"address.city\":\"anon city\",\"['bob'].street\":\"a secret street\",\"pets[*].species\":\"* * * *\",\"pii\":\"****\",\"phone\":\"000 0000 0000\"}}]"
    public static Map<String, Object> parseMap(String in) {
        JSONArray arr = new JSONArray(in);
        var obj = arr.getJSONObject(0);
        var resulObject = obj.getJSONObject("result");
        return resulObject.toMap();
    }
}
