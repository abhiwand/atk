package com.intel.pig.udf.flatten;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class StringSplitOptionsTest {

    @Test
    public void StringSplitOptions_requires_delimiter() throws Exception {
        try {
            new StringSplitOptions(null, null, null, false);
        }
        catch(IllegalArgumentException e) {
            assertEquals("delimiter can't be empty", e.getMessage());
        }
    }

    @Test
    public void StringSplitOptions_and_getters_work() {
        String delimiter = "delimiter";
        String start = "start";

        StringSplitOptions options = new StringSplitOptions(delimiter, start, null, true);

        assertEquals(delimiter, options.getDelimiter());
        assertEquals(start, options.getTrimStart());
        assertNull(options.getTrimEnd());
        assertTrue(options.isTrimWhitespace());
    }

    @Test
    public void StringSplitOptions_and_getters_work_other_args() {
        String delimiter = "delimiter";
        String end = "end";

        StringSplitOptions options = new StringSplitOptions(delimiter, null, end, false);

        assertEquals(delimiter, options.getDelimiter());
        assertNull(options.getTrimStart());
        assertEquals(end, options.getTrimEnd());
        assertFalse(options.isTrimWhitespace());
    }

}
