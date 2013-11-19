package com.intel.hadoop.graphbuilder.util;


import com.intel.hadoop.graphbuilder.pipeline.input.hbase.HBaseCommandLineOptions;
import com.intel.hadoop.graphbuilder.pipeline.output.titan.TitanCommandLineOptions;
import org.apache.commons.cli.Option;

/**
 * one stop shop for all the command line option long names (ie tablename for -t) that gets referenced in
 * many places from tokenizers to configs.
 * Pull all the options into one central place to make it easier to change down the road and easier to find.
 *
 */
public class CommonCommandLineOptions {

    //titan command line options
    private static final String APPEND = TitanCommandLineOptions.APPEND;

    //hbase command line options
    private static final String CMD_EDGES_OPTION_NAME = HBaseCommandLineOptions.CMD_EDGES_OPTION_NAME;
    private static final String CMD_DIRECTED_EDGES_OPTION_NAME = HBaseCommandLineOptions.CMD_DIRECTED_EDGES_OPTION_NAME;
    private static final String CMD_TABLE_OPTION_NAME = HBaseCommandLineOptions.CMD_TABLE_OPTION_NAME;
    private static final String CMD_VERTICES_OPTION_NAME = HBaseCommandLineOptions.CMD_VERTICES_OPTION_NAME;
    private static final String FLATTEN_LISTS_OPTION_NAME = HBaseCommandLineOptions.FLATTEN_LISTS_OPTION_NAME;

    public enum Option{
        titanAppend(APPEND), table(CMD_TABLE_OPTION_NAME), edges(CMD_EDGES_OPTION_NAME),
        directedEdges(CMD_DIRECTED_EDGES_OPTION_NAME), vertices(CMD_VERTICES_OPTION_NAME),
        flatten(FLATTEN_LISTS_OPTION_NAME);

        private final String longName;
        Option(String longName){this.longName = longName;}
        public String get(){return this.longName;}
    }
}
