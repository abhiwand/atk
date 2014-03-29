package com.intel.pig.udf.flatten;

import org.apache.commons.lang.StringUtils;

/**
 * Options for how to split a delimited string into a list.
 *
 * Extra options are included like removing a start and end String, as well as trimming whitespace.
 */
public class StringSplitOptions {

    private final String delimiter;
    private final String trimStart;
    private final String trimEnd;
    private final boolean trimWhitespace;

    /**
     * Options for splitting a String
     * @param delimiter the delimiter to split on
     * @param trimStart String.startsWith("") characters to remove if they exist
     * @param trimEnd String.endsWith("") characters to remove if they exist
     * @param trimWhitespace true if whitespace should also be used as part of split expression
     */
    public StringSplitOptions(String delimiter, String trimStart, String trimEnd, boolean trimWhitespace) {
        if (StringUtils.isEmpty(delimiter)) {
            throw new IllegalArgumentException("delimiter can't be empty");
        }

        this.delimiter = delimiter;
        this.trimStart = trimStart;
        this.trimEnd = trimEnd;
        this.trimWhitespace = trimWhitespace;
    }

    public String getDelimiter() {
        return delimiter;
    }

    /**
     * Characters to remove from beginning of String, e.g. "(" for splitting "(1,2,3)" into { "1" "2" "3" }
     */
    public String getTrimStart() {
        return trimStart;
    }

    /**
     * Characters to remove from end of String, e.g. ")" for splitting "(1,2,3)" into { "1" "2" "3" }
     */
    public String getTrimEnd() {
        return trimEnd;
    }


    public boolean isTrimWhitespace() {
        return trimWhitespace;
    }
}
