package com.intel.pig.udf.flatten;

import static org.junit.Assert.assertArrayEquals;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;

public class StringSplitterTest {

    @Test
    public void split_with_comma_and_start_and_end() throws Exception {
        StringSplitter splitter = new StringSplitter(new StringSplitOptions(",", "(", ")", false));
        assertArrayEquals(new String[]{"1", "2", "3"}, splitter.split("(1,2,3)"));
    }

    @Test
    public void split_where_start_and_end_not_found() throws Exception {
        StringSplitter splitter = new StringSplitter(new StringSplitOptions(",", "(", ")", false));
        assertArrayEquals(new String[]{"1", "2", "3"}, splitter.split("1,2,3"));
    }

    @Test
    public void split_should_work_with_other_delimiters() throws Exception {
        StringSplitter splitter = new StringSplitter(new StringSplitOptions("#", null, null, false));
        assertArrayEquals(new String[]{"1", "2", "3"}, splitter.split("1#2#3"));
    }

    @Test
    public void split_with_null_start_or_end() throws Exception {
        StringSplitter splitter = new StringSplitter(new StringSplitOptions("|", null, null, false));
        assertArrayEquals(new String[]{"1", "2", "3"}, splitter.split("1|2|3"));
    }


    @Test
    public void split_can_trim_long_start_and_end() throws Exception {
        StringSplitter splitter = new StringSplitter(new StringSplitOptions("|", "--<[", "]>--", false));
        assertArrayEquals(new String[]{"1", "2", "3"}, splitter.split("--<[1|2|3]>--"));
    }

    @Test
    public void split_can_handle_trimming_whitespace() throws Exception {
        StringSplitter splitter = new StringSplitter(new StringSplitOptions(",", "(", ")", true));
        assertArrayEquals(new String[]{"1", "2", "3"}, splitter.split("( 1, 2, 3 )"));
    }

    @Test
    public void split_can_handle_leaving_whitespace() throws Exception {
        StringSplitter splitter = new StringSplitter(new StringSplitOptions(",", "(", ")", false));
        assertArrayEquals(new String[]{" 1", " 2", " 3 "}, splitter.split("( 1, 2, 3 )"));
    }

    @Test
    public void split_can_handle_whitespace_in_parts() throws Exception {
        StringSplitter splitter = new StringSplitter(new StringSplitOptions(",", "(", ")", true));
        assertArrayEquals(new String[]{"le gateau", "le pain"}, splitter.split("( le gateau, le pain )"));
    }

    @Test
    public void split_can_handle_null() throws Exception {
        StringSplitter splitter = new StringSplitter(new StringSplitOptions(",", "(", ")", true));
        assertArrayEquals(ArrayUtils.EMPTY_STRING_ARRAY, splitter.split(null));
    }

    @Test
    public void split_can_handle_empty() throws Exception {
        StringSplitter splitter = new StringSplitter(new StringSplitOptions(",", "(", ")", true));
        assertArrayEquals(ArrayUtils.EMPTY_STRING_ARRAY, splitter.split(""));
    }

    @Test
    public void split_can_handle_blank() throws Exception {
        StringSplitter splitter = new StringSplitter(new StringSplitOptions(",", "(", ")", true));
        assertArrayEquals(ArrayUtils.EMPTY_STRING_ARRAY, splitter.split("   "));
    }

    @Test
    public void split_can_handle_zero_parts() throws Exception {
        StringSplitter splitter = new StringSplitter(new StringSplitOptions(",", "(", ")", true));
        assertArrayEquals(ArrayUtils.EMPTY_STRING_ARRAY, splitter.split("(  )"));
    }

}
