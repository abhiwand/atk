/* Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * For more about this software visit:
 *      http://www.01.org/GraphBuilder
 */
package com.intel.pig.udf.util;

import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;

/**
 * This is a helper for converting Strings to booleans.
 *
 * (Pig needs constructor args to be strings).
 */
public class BooleanUtils {

    private static final List<String> TRUE_BOOLEAN_VALUES = Arrays.asList( "1", "true", "TRUE", "True" );
    private static final List<String> FALSE_BOOLEAN_VALUES = Arrays.asList("0", "false", "FALSE", "False");
    private static final List<String> ALL_BOOLEAN_VALUES = ListUtils.union(TRUE_BOOLEAN_VALUES, FALSE_BOOLEAN_VALUES);

    /**
     * True values: "1", "true", "TRUE", "True",
     * False values: "0", "false", "FALSE", "False"
     * @throws IllegalArgumentException for any other value
     */
    public static void validateBooleanString(String booleanString) {
        if (!ALL_BOOLEAN_VALUES.contains(booleanString)) {
            throw new IllegalArgumentException(
                    booleanString + " is not a valid argument. " +
                            "Use one of: " + StringUtils.join(ALL_BOOLEAN_VALUES, ", "));
        }
    }

    /**
     * True values: "1", "true", "TRUE", "True",
     * False values: "0", "false", "FALSE", "False"
     * @throws IllegalArgumentException for any other value
     */
    public static boolean toBoolean(String booleanString) {
        validateBooleanString(booleanString);
        return TRUE_BOOLEAN_VALUES.contains(booleanString);
    }
}
