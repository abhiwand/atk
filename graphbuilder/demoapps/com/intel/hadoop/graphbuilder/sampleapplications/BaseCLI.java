package com.intel.hadoop.graphbuilder.sampleapplications;

import com.intel.hadoop.graphbuilder.pipeline.input.hbase.GBHTableConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.output.titan.TitanCommandLineOptions;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;

public class BaseCLI {

    public static final String CMD_EDGES_OPTNAME = GBHTableConfiguration.CMD_EDGES_OPTNAME;
    public static final String CMD_DIRECTED_EDGES_OPTNAME = GBHTableConfiguration.CMD_DIRECTED_EDGES_OPTNAME;
    public static final String CMD_TABLE_OPTNAME = GBHTableConfiguration.CMD_TABLE_OPTNAME;
    public static final String CMD_VERTICES_OPTNAME = GBHTableConfiguration.CMD_VERTICES_OPTNAME;
    public static final String FLATTEN_LISTS_OPTNAME = GBHTableConfiguration.FLATTEN_LISTS_OPTNAME;
    public static final String TITAN_APPEND = TitanCommandLineOptions.APPEND;

    public enum Options{
        hbaseTable(CLI_HBASE_TABLE_NAME_OPTION), vertex(CLI_VERTEX_OPTION), edge(CLI_EDGE_OPTION),
        directedEdge(CLI_DIRECTED_EDGE_OPTION), flattenList(CLI_FLATTEN_LIST_OPTION),
        titanAppend(CLI_TITAN_APPEND_OPTION), outputPath(CLI_OUTPUT_PATH_OPTION), inputPath(CLI_INPUT_PATH_OPTION);

        private final Option option;
        Options(Option option){this.option = option;}
        public Option get(){return this.option;}
    }

    //shared options amongst the demo apps no reason duplicate these configs all over the place
    public static final Option CLI_FLATTEN_LIST_OPTION = OptionBuilder.withLongOpt(FLATTEN_LISTS_OPTNAME)
            .withDescription("Flag that expends lists into multiple items. " )
            .create("F");

    public static final Option CLI_TITAN_APPEND_OPTION= OptionBuilder.withLongOpt(TITAN_APPEND)
            .withDescription("Append Graph to Current Graph at Specified Titan Table")
            .create("a");

    public static final Option CLI_OUTPUT_PATH_OPTION = OptionBuilder.withLongOpt("out").withDescription("output path")
            .hasArg().create("o");

    public static final Option CLI_INPUT_PATH_OPTION = OptionBuilder.withLongOpt("in")
            .withDescription("input path")
            .hasArgs()
            .isRequired()
            .withArgName("input path")
            .create("i");

    public static final Option CLI_HBASE_TABLE_NAME_OPTION = OptionBuilder.withLongOpt(CMD_TABLE_OPTNAME)
            .withDescription("HBase table name")
            .hasArgs()
            .isRequired()
            .withArgName("HBase table name")
            .create("t");

    public static final Option CLI_VERTEX_OPTION  = OptionBuilder.withLongOpt(CMD_VERTICES_OPTNAME)
            .withDescription("Specify the columns which are vertex tokens and vertex properties" +
                    "Example: --" + CMD_VERTICES_OPTNAME + "\"<vertex_col>=[<vertex_prop1>,...]\"")
            .hasArgs()
            .isRequired()
            .withArgName("Vertex-Column-Name")
            .create("v");

    public static final Option CLI_EDGE_OPTION = OptionBuilder.withLongOpt(CMD_EDGES_OPTNAME)
            .withDescription("Specify the HTable columns which are undirected edge tokens; " +
                    "Example: --" + CMD_EDGES_OPTNAME + "\"<src_vertex_col>,<dest_vertex_col>,<label>,[edge_property_col,...]\"..." +
                    "Note: Edge labels must be unique")
            .hasArgs()
            .withArgName("Edge-Column-Name")
            .create("e");

    public static final Option CLI_DIRECTED_EDGE_OPTION = OptionBuilder.withLongOpt(CMD_DIRECTED_EDGES_OPTNAME)
            .withDescription("Specify the columns which are directed edge tokens; " +
                    "Example: --" + CMD_DIRECTED_EDGES_OPTNAME + "\"<src_vertex_col>,<dest_vertex_col>,<label>,[edge_property_col,...]\"..." +
                    "Note: Edge labels must be unique")
            .hasArgs()
            .withArgName("Edge-Column-Name")
            .create("d");
}
