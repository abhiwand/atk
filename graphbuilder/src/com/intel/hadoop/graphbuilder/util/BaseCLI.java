package com.intel.hadoop.graphbuilder.util;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;


/**
 * A nice wrapper to grab all the commonly used CLI options. Easily referenced by the enum so it's easy to get the list of
 * options in any code editor.
 *
 * Usage:
 *
 * BaseCLI.Options.inputPath.get()
 *
 * @see CommandLineInterface
 */
public class BaseCLI {

    private static final String CMD_EDGES_OPTION_NAME = CommonCommandLineOptions.Option.edges.get();
    private static final String CMD_DIRECTED_EDGES_OPTION_NAME = CommonCommandLineOptions.Option.directedEdges.get();
    private static final String CMD_TABLE_OPTION_NAME = CommonCommandLineOptions.Option.table.get();
    private static final String CMD_VERTICES_OPTION_NAME = CommonCommandLineOptions.Option.vertices.get();
    private static final String FLATTEN_LISTS_OPTION_NAME = CommonCommandLineOptions.Option.flatten.get();

    private static final String TITAN_APPEND = CommonCommandLineOptions.Option.titanAppend.get();

    public enum Options{
        hbaseTable(CLI_HBASE_TABLE_NAME_OPTION), vertex(CLI_VERTEX_OPTION), edge(CLI_EDGE_OPTION),
        directedEdge(CLI_DIRECTED_EDGE_OPTION), flattenList(CLI_FLATTEN_LIST_OPTION),
        titanAppend(CLI_TITAN_APPEND_OPTION), outputPath(CLI_OUTPUT_PATH_OPTION), inputPath(CLI_INPUT_PATH_OPTION);

        private final Option option;
        Options(Option option){this.option = option;}
        public Option get(){return this.option;}
    }

    //shared options amongst the demo apps no reason duplicate these configs all over the place
    private static final Option CLI_FLATTEN_LIST_OPTION = OptionBuilder.withLongOpt(FLATTEN_LISTS_OPTION_NAME)
            .withDescription("Flag that expends lists into multiple items. " )
            .create("F");

    private static final Option CLI_TITAN_APPEND_OPTION= OptionBuilder.withLongOpt(TITAN_APPEND)
            .withDescription("Append Graph to Current Graph at Specified Titan Table")
            .create("a");

    private static final Option CLI_OUTPUT_PATH_OPTION = OptionBuilder.withLongOpt("out").withDescription("output path")
            .hasArg().create("o");

    private static final Option CLI_INPUT_PATH_OPTION = OptionBuilder.withLongOpt("in")
            .withDescription("input path")
            .hasArgs()
            .isRequired()
            .withArgName("input path")
            .create("i");

    private static final Option CLI_HBASE_TABLE_NAME_OPTION = OptionBuilder.withLongOpt(CMD_TABLE_OPTION_NAME)
            .withDescription("HBase table name")
            .hasArgs()
            .isRequired()
            .withArgName("HBase table name")
            .create("t");

    private static final Option CLI_VERTEX_OPTION  = OptionBuilder.withLongOpt(CMD_VERTICES_OPTION_NAME)
            .withDescription("Specify the columns which are vertex tokens and vertex properties" +
                    "Example: --" + CMD_VERTICES_OPTION_NAME + "\"<vertex_col>=[<vertex_prop1>,...]\"")
            .hasArgs()
            .isRequired()
            .withArgName("Vertex-Column-Name")
            .create("v");

    private static final Option CLI_EDGE_OPTION = OptionBuilder.withLongOpt(CMD_EDGES_OPTION_NAME)
            .withDescription("Specify the HTable columns which are undirected edge tokens; " +
                    "Example: --" + CMD_EDGES_OPTION_NAME + "\"<src_vertex_col>,<dest_vertex_col>,<label>,[edge_property_col,...]\"..." +
                    "Note: Edge labels must be unique")
            .hasArgs()
            .withArgName("Edge-Column-Name")
            .create("e");

    private static final Option CLI_DIRECTED_EDGE_OPTION = OptionBuilder.withLongOpt(CMD_DIRECTED_EDGES_OPTION_NAME)
            .withDescription("Specify the columns which are directed edge tokens; " +
                    "Example: --" + CMD_DIRECTED_EDGES_OPTION_NAME + "\"<src_vertex_col>,<dest_vertex_col>,<label>,[edge_property_col,...]\"..." +
                    "Note: Edge labels must be unique")
            .hasArgs()
            .withArgName("Edge-Column-Name")
            .create("d");
}
