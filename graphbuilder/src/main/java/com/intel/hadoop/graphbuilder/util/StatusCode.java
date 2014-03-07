/**
 * Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more about this software visit:
 *     http://www.01.org/GraphBuilder
 */
package com.intel.hadoop.graphbuilder.util;

/**
 * Code numbers with <code>String</code> messages for reporting abnormal terminations.
 */
public enum StatusCode {
    SUCCESS(0, "GRAPHBUILDER_SUCCESS"),
    BAD_COMMAND_LINE(1, "GRAPHBUILDER_ERROR: bad command line"),
    UNABLE_TO_LOAD_INPUT_FILE(2, "GRAPHBUILDER_ERROR: unable to load input file"),
    UNHANDLED_IO_EXCEPTION(3, "GRAPHBUILDER_ERROR: unhandled IO exception"),
    MISSING_HBASE_TABLE(4, "GRAPHBUILDER_ERROR: missing hbase table"),
    HADOOP_REPORTED_ERROR(5, "GRAPHBUILDER_ERROR: hadoop reported exception"),
    INTERNAL_PARSER_ERROR(6, "GRAPHBUILDER_ERROR: internal parser error"),
    UNABLE_TO_CONNECT_TO_HBASE(7, "GRAPHBUILDER_ERROR: unable to connect to hbase"),
    CLASS_INSTANTIATION_ERROR(8, "GRAPHBUILDER_ERROR: class instantiation error"),
    INDESCRIBABLE_FAILURE(9, "GRAPHBUILDER_ERROR: failure"),
    HBASE_ERROR(10, "GRAPHBUILDER_ERROR: hbase error"),
    TITAN_ERROR(11, "GRAPHBUILDER_ERROR: Titan error"),
    CANNOT_FIND_CONFIG_FILE(12, "GRAPHBUILDER_ERROR: cannot locate config file"),
    CANNOT_FIND_DEPENDENCIES(13, "GRAPHBUILDER_ERROR: cannot find dependencies"),
    UNKNOWN_CLASS_IN_GRAPHSCHEMA(14, "GRAPHBUILDER_ERROR: Class not found exception for graph schema");

    private final int status;
    private final String message;

    /**
     * Constructor.
     *
     * @param status  Status code number.
     * @param message String message to user and or logger.
     */
    StatusCode(int status, String message) {
        this.status = status;
        this.message = message;
    }

    /**
     * Obtain the numerical status code from this <code>StatusCode</code> object.
     *
     * @return integer status code
     */
    public int getStatus() {
        return status;
    }

    /**
     * Obtain the human-readable error message from this <code>StatusCode</code> object.
     *
     * @return A string containing a brief error message.
     */
    public String getMessage() {
        return message;
    }
}
