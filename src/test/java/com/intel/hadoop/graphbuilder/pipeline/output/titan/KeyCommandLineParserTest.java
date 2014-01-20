package com.intel.hadoop.graphbuilder.pipeline.output.titan;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KeyCommandLineParserTest {

    @Test
    public void testParseKeyCommandLine_Empty() throws Exception {

        // invoke method under test
        List<GBTitanKey> gbTitanKeyList = new KeyCommandLineParser().parse("");

        assertEquals(0, gbTitanKeyList.size());
    }

    @Test
    public void testParseKeyCommandLine() throws Exception {

        // invoke method under test
        List<GBTitanKey> list = new KeyCommandLineParser().parse("cf:userId;String;U;V,cf:eventId;E;Long");

        assertEquals(2, list.size());

        GBTitanKey userKey = list.get(0);
        assertEquals("cf:userId", userKey.getName());
        assertEquals(String.class, userKey.getDataType());
        assertTrue(userKey.isVertexIndex());
        assertFalse(userKey.isEdgeIndex());
        assertTrue(userKey.isUnique());

        GBTitanKey eventKey = list.get(1);
        assertEquals("cf:eventId", eventKey.getName());
        assertEquals(Long.class, eventKey.getDataType());
        assertFalse(eventKey.isVertexIndex());
        assertTrue(eventKey.isEdgeIndex());
        assertFalse(eventKey.isUnique());

    }
}
