

package com.intel.hadoop.graphbuilder.demoapps.tabletotextgraph;

import com.intel.hadoop.graphbuilder.graphconstruction.outputconfiguration.TextGraphOutputConfiguration;
import com.intel.hadoop.graphbuilder.graphconstruction.inputconfiguration.HBaseInputConfiguration;
import com.intel.hadoop.graphbuilder.graphconstruction.inputmappers.GBHTableConfig;
import com.intel.hadoop.graphbuilder.job.AbstractCreateGraphJob;
import com.intel.hadoop.graphbuilder.util.Timer;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

/**
 * TableToTextGraph
 *
 * Read  a big tables and generate a graph in the TextGraph format,
 * This is two text files, one a vertex list, and the other an edge list text files.
 */

public class TableToTextGraph {

    private static final Logger LOG = Logger.getLogger(TableToTextGraph.class);

    private static Options createOptions() {

        Options options = new Options();
        options.addOption("h", "help", false, "");
        options.addOption("o", "out", true, "output path");
        options.addOption("t", GBHTableConfig.config.getProperty("CMD_TABLE_OPTNAME"), true, "HBase table name");

        options.addOption(OptionBuilder.withLongOpt(GBHTableConfig.config.getProperty("CMD_VERTICES_OPTNAME"))
                .withDescription("Specify the columns which are vertex tokens and vertex properties" +
                        "Example: --" + GBHTableConfig.config.getProperty("CMD_VERTICES_OPTNAME") + "\"<vertex_col>=[<vertex_prop1>,...]\"")
                .hasArgs()
                .isRequired()
                .withArgName("Vertex-Column-Name")
                .create("v"));

        options.addOption(OptionBuilder.withLongOpt(GBHTableConfig.config.getProperty("CMD_EDGES_OPTNAME"))
                .withDescription("Specify the columns which are edge tokens; " +
                        "Example: --" + GBHTableConfig.config.getProperty("CMD_EDGES_OPTNAME") + "\"<src_vertex_col>,<dest_vertex_col>,<label>,[edge_property_col,...]\"..." +
                        "Note: Edge labels must be unique")
                .hasArgs()
                .isRequired()
                .withArgName("Edge-Column-Name")
                .create("e"));

        options.addOption(OptionBuilder.withLongOpt(GBHTableConfig.config.getProperty("FLATTEN_LISTS_OPTNAME"))
                .withDescription("Flag that expends lists into multiple items. " )
                .create("F"));

        return options;
    }

    private static void showHelp(Options options) {
        HelpFormatter h = new HelpFormatter();
        h.printHelp("TableToTextGraph", options);
    }

    /**
     * This function checks whether required tablename, vertices, vertex properties
     * edges and edge properties are specified as command line arguments
     *
     * @param args Command line parameters
     */
    private static CommandLine checkCli(String[] args) {

        Options options          = createOptions();
        String      srcTableName = null;
        String      outTableName = null;
        CommandLine cmd          = null;

        try {
            CommandLineParser parser = new PosixParser();
            cmd                      = parser.parse(options, args);

            if (cmd.hasOption(GBHTableConfig.config.getProperty("CMD_TABLE_OPTNAME"))) {
                srcTableName = cmd.getOptionValue(GBHTableConfig.config.getProperty("CMD_TABLE_OPTNAME"));
                LOG.info("Table Name: " + srcTableName);
            } else {
                LOG.fatal("A table name is required");
                showHelp(options);
                System.exit(1);
            }

            if (cmd.hasOption("o")) {
                outTableName = cmd.getOptionValue("o");
                LOG.info("Output path: " + outTableName);
            } else {
                LOG.fatal("An output path is required");
                showHelp(options);
                System.exit(1);
            }

            if (cmd.hasOption(GBHTableConfig.config.getProperty("CMD_VERTICES_OPTNAME"))) {
                for (String v : cmd.getOptionValues(GBHTableConfig.config.getProperty("CMD_VERTICES_OPTNAME"))) {
                    LOG.info("Vertices: " + v);
                }
            } else {
                LOG.fatal("Please add column family and names for vertices and vertex properties");
                showHelp(options);
                System.exit(1);
            }

            if (cmd.hasOption(GBHTableConfig.config.getProperty("CMD_EDGES_OPTNAME"))) {
                for (String e : cmd.getOptionValues(GBHTableConfig.config.getProperty("CMD_EDGES_OPTNAME"))) {
                    LOG.info("Edges: " + e);
                }
            } else {
                LOG.fatal("Please add column family and names for edges and edge properties");
                showHelp(options);
                System.exit(1);
            }

        } catch (Exception e) {
            e.printStackTrace();
            showHelp(options);
            System.exit(1);
        }

        assert(cmd != null);
        return cmd;
    }

    /**
     * Encapsulation of the job setup process.
     */
    public class Job extends AbstractCreateGraphJob {
        /**
         * This method allows bidirectional edges (do not clean them).
         * @return  false
         */
        @Override
        public boolean shouldCleanBiDirectionalEdges() {
            return false;
        }

        /**
         * This method uses hbase.
         * @return  true
         */
        @Override
        public boolean shouldUseHBase() {
            return true;
        }
    }

    /**
     * Main method for feature table to text graph construction
     *
     * @param args Command line arguments
     * @throws Exception
     */

    public static void main(String[] args) throws  Exception {

        Timer timer = new Timer();

        CommandLine cmd = checkCli(args);

        Job                                   job                 = new TableToTextGraph().new Job();
        HBaseInputConfiguration               inputConfiguration  = new HBaseInputConfiguration();
        BasicHBaseGraphBuildingRule           buildingRule        = new BasicHBaseGraphBuildingRule(cmd);
        TextGraphOutputConfiguration outputConfiguration = new TextGraphOutputConfiguration();


        LOG.info("============= Creating graph from hbase ==================");
        timer.start();
        job.run( inputConfiguration,buildingRule, outputConfiguration, cmd);
        LOG.info("========== Done creating graph from hbase ================");
        LOG.info("Time elapsed : " + timer.current_time() + " seconds");
    }
}