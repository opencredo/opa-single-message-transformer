package com.opencredo.opasmt;

import org.json.JSONArray;

public class OpaResultParser {

    //  example: [{"result":["000 0000 0000"]}]
    public static String parseStringResult(String in) {
        JSONArray arr = new JSONArray(in);
        var obj = arr.getJSONObject(0);
        var resultArray = obj.getJSONArray("result");
        if(resultArray.length()==0) {
            return null;
        }
        return resultArray.getString(0);
    }

    //  example: [{"result":[false]}]
    public static boolean parseBooleanResult(String in) {
        JSONArray arr = new JSONArray(in);
        var obj = arr.getJSONObject(0);
        return obj.getBoolean("result");
    }
}
