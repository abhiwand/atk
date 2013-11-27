package com.intel.hadoop.graphbuilder.pipeline.input.hbase;

/**
 * hold all the command line long options names. These should not be referenced directly but through CommonCommandLineOptions.
 * if any new options names are added they should also be added to CommonCommandLineOptions
 *
 * @see com.intel.hadoop.graphbuilder.util.CommonCommandLineOptions
 */
public class HBaseCommandLineOptions {
    public static final String CMD_EDGES_OPTION_NAME = "edges";
    public static final String CMD_DIRECTED_EDGES_OPTION_NAME = "directedEdges";
    public static final String CMD_TABLE_OPTION_NAME = "tablename";
    public static final String CMD_VERTICES_OPTION_NAME = "vertices";
    public static final String FLATTEN_LISTS_OPTION_NAME = "flattenlists";
}
