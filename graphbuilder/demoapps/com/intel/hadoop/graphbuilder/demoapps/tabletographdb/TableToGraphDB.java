

package com.intel.hadoop.graphbuilder.demoapps.tabletographdb;

import com.intel.hadoop.graphbuilder.demoapps.tabletotextgraph.BasicHBaseTokenizer;
import com.intel.hadoop.graphbuilder.graphconstruction.outputconfiguration.TitanOutputConfiguration;
import com.intel.hadoop.graphbuilder.graphconstruction.inputconfiguration.HBaseInputConfiguration;
import com.intel.hadoop.graphbuilder.graphconstruction.inputmappers.GBHTableConfig;
import com.intel.hadoop.graphbuilder.job.AbstractCreateGraphJob;
import com.intel.hadoop.graphbuilder.util.CommandLineInterface;
import com.intel.hadoop.graphbuilder.util.GraphbuilderExit;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import com.intel.hadoop.graphbuilder.util.Timer;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

/**
 * TableToGraphDB
 * a demonstration/testing application showing how to generate graphs from big tables and load
 * them into a database
 */

public class TableToGraphDB {

    private static final Logger LOG = Logger.getLogger(TableToGraphDB.class);

    private static CommandLineInterface commandLineInterface = new CommandLineInterface();
    static {
        Options options = new Options();
        options.addOption("h", "help", false, "");
        options.addOption("o", "out",  true, "output path");

        options.addOption(OptionBuilder.withLongOpt(GBHTableConfig.config.getProperty("CMD_TABLE_OPTNAME"))
                .withDescription("HBase table name")
                .hasArgs()
                .isRequired()
                .withArgName("HBase table name")
                .create("t"));

        options.addOption(OptionBuilder.withLongOpt(GBHTableConfig.config.getProperty("CMD_VERTICES_OPTNAME"))
                .withDescription("Specify the HBase columns which are vertex tokens and vertex properties" +
                        "Example: --" + GBHTableConfig.config.getProperty("CMD_VERTICES_OPTNAME") + "\"<vertex_col>=[<vertex_prop1>,...]\"")
                .hasArgs()
                .isRequired()
                .withArgName("Vertex-Column-Name")
                .create("v"));

        options.addOption(OptionBuilder.withLongOpt(GBHTableConfig.config.getProperty("CMD_EDGES_OPTNAME"))
                .withDescription("Specify the HTable columns which are edge tokens; " +
                        "Example: --" + GBHTableConfig.config.getProperty("CMD_EDGES_OPTNAME") + "\"<src_vertex_col>,<dest_vertex_col>,<label>,[edge_property_col,...]\"..." +
                        "Note: Edge labels must be unique")
                .hasArgs()
                .isRequired()
                .withArgName("Edge-Column-Name")
                .create("e"));

        commandLineInterface.setOptions(options);
    }

    public class Job extends AbstractCreateGraphJob {
        @Override
        public boolean cleanBidirectionalEdge() {
            return false;
        }

        @Override
        public boolean usesHBase() {
            return true;
        }
    }

    /**
     * Main method for feature table to graph database construction
     *
     * @param args Command line arguments
     * @throws Exception
     */

    public static void main(String[] args)  {

        Timer timer = new Timer();

        commandLineInterface.checkCli(args);
        if (null == commandLineInterface.getCmd()) {
            commandLineInterface.showHelp("Error parsing command line options");
            GraphbuilderExit.graphbuilderFatalExitNoException(StatusCode.BAD_COMMAND_LINE,
                    "Error parsing command line options", LOG);
        }

        Job                      job                 = new TableToGraphDB().new Job();
        job = (Job) commandLineInterface.getRuntimeConfig().addConfig(job);

        HBaseInputConfiguration  inputConfiguration  = new HBaseInputConfiguration();
        BasicHBaseTokenizer      tokenizer           = new BasicHBaseTokenizer();
        TitanOutputConfiguration outputConfiguration = new TitanOutputConfiguration();

        LOG.info("============= Creating graph from feature table ==================");
        timer.start();
        job.run(inputConfiguration, tokenizer, outputConfiguration, commandLineInterface.getCmd());
        LOG.info("========== Done creating graph from feature table ================");
        LOG.info("Time elapsed : " + timer.current_time() + " seconds");
    }
}