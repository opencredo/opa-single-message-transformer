package com.opencredo.opasmt;

import org.junit.Assert;
import org.junit.Test;

public class TestOpaResultParsing {

    @Test
    public void parseTrueResponse() {
        String in = "[{\"result\":true}]";
        Assert.assertTrue(OpaResultParser.parseBooleanResult(in));
    }

    @Test
    public void parseFalseResponse() {
        String in = "[{\"result\":false}]";
        Assert.assertFalse(OpaResultParser.parseBooleanResult(in));
    }

    @Test
    public void parseStringResponse() {
        String in = "[{\"result\":[\"000 0000 0000\"]}]";
        Assert.assertEquals("000 0000 0000", OpaResultParser.parseStringResult(in));
    }

}
