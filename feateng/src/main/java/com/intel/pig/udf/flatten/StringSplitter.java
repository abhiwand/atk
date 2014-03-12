package com.intel.pig.udf.flatten;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * A slightly fancy String splitter that can handle extra options like trimming whitespace, and removing start and end
 * characters.
 */
public class StringSplitter {

    private StringSplitOptions options;

    public StringSplitter(StringSplitOptions options) {
        this.options = options;
    }

    /**
     * Split string according to options
     * @param s the string to split
     * @return Empty Array or the parts
     */
    public String[] split(String s) {
        if (StringUtils.isEmpty(s) ) {
            return ArrayUtils.EMPTY_STRING_ARRAY;
        }

        String trimmed = trimStart(trimEnd(s));
        return StringUtils.split(trimmed, options.getSplitChars());
    }

    private String trimStart(String s) {
        if (needsTrimStart(s)) {
            return s.substring(options.getTrimStart().length());
        }
        return s;
    }

    private String trimEnd(String s) {
        if (needsTrimEnd(s)) {
            return s.substring(0, s.length() - options.getTrimStart().length());
        }
        return s;
    }

    private boolean needsTrimStart(String s) {
        return options.hasTrimStart()
                && StringUtils.startsWith(s, options.getTrimStart());
    }

    private boolean needsTrimEnd(String s) {
        return options.hasTrimEnd()
                && StringUtils.endsWith(s, options.getTrimEnd());
    }
}
