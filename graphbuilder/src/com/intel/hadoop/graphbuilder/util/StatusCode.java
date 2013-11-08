package com.intel.hadoop.graphbuilder.util;

public enum StatusCode {
    SUCCESS(0,                    "GRAPHBUILDER: success"),
    BAD_COMMAND_LINE(1,           "GRAPHBUILDER: bad command line"),
    UNABLE_TO_LOAD_INPUT_FILE(2,  "GRAPHBUILDER: unable to load input file"),
    UNHANDLED_IO_EXCEPTION(3,     "GRAPHBUILDER: unhandled IO exception"),
    MISSING_HBASE_TABLE(4,        "GRAPHBUILDER: missing hbase table"),
    HADOOP_REPORTED_ERROR(5,      "GRAPHBUILDER: hadoop reported exception"),
    INTERNAL_PARSER_ERROR(6,      "GRAPHBUILDER: internal parser error"),
    UNABLE_TO_CONNECT_TO_HBASE(7, "GRAPHBUILDER: unable to connect to hbase"),
    CLASS_INSTANTIATION_ERROR(8,  "GRAPHBUILDER: class instantiation error"),
    INDESCRIBABLE_FAILURE(9,      "GRAPHBUILDER: failure"),
    HBASE_ERROR(10,               "GRAPHBUILDER: hbase error"),
    TITAN_ERROR(11,               "GRAPHBUILDER: Titan error"),
    CANNOT_FIND_CONFIG_FILE(12,   "GRAPHBUILDER: cannot locate config file");

    private final int    status;
    private final String message;

    StatusCode(int status, String message) {
        this.status  = status;
        this.message = message;
    }

    public int getStatus(){
        return status;
    }

    public String getMessage() {
        return message;
    }
}
