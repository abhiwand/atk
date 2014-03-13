package com.intel.pig.udf.flatten;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

        String trimmed = trimStartAndEnd(s);
        String[] parts = StringUtils.split(trimmed, options.getDelimiter());
        if (options.isTrimWhitespace()) {
            parts = trim(parts);
        }
        return parts;
    }

    private String trimStartAndEnd(String s) {
        String trimmed = StringUtils.removeStart(s, options.getTrimStart());
        return StringUtils.removeEnd(trimmed, options.getTrimEnd());
    }

    /**
     * Create a new array of trimmed strings discarding nulls and empties
     */
    private String[] trim(String[] strings) {
        List<String> list = new ArrayList<String>(strings.length);
        for(String s : strings) {
            String trimmed = StringUtils.trim(s);
            if(StringUtils.isNotEmpty(trimmed)) {
                list.add(trimmed);
            }
        }
        return list.toArray(new String[list.size()] );
    }

}
