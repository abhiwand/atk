

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
 * <p>
 * Read  a big tables and generate a graph in the TextGraph format,
 * that is, two text files, one a vertex list, and the other an edge list text files.
 * </p>
 *
 *  * <p>
 *     Path Arguments:
 *     <ul>
 *         <li> <code>-t</code> specifies the HBase table from which to read</li>
 *         <li> <code>-conf</code> specifies configuration file</li>
 *         <li><code>-a</code> an option that tells Titan it can append the newly generated graph to an existing
 *         one in the same table. Default behavior is to abort if you try to use an existing Titan table name</li>
 *     </ul>
 *     The Titan table name is specified in the configuration file in the property
 *     <code>graphbuilder.titan.storage_tablename</code>
 * </p>
 *
 * <p>TO SPECIFY EDGES:
 * Edges are specified by a sequence of "edge rules" following the flag <code>-e</code> (for undirected edges) or
 * <code>-d</code> (for directed edges). The rules for edge construction are the same for both directed and
 * undirected edges.
 * The first three attributes in the edge rule are source vertex column, destination
 * vertex column and the string label. </p>
 * <code> -e src_col,dest_col>,label,edge_property_col1,...edge_property_coln </code>
 * </p>
 * <p> <code> -d src_col,dest_col>,label,edge_property_col1,...edge_property_coln </code></p>
 * <p>
 * <p>TO SPECIFY VERTICES: The first attribute in the string is the vertex ID column. Subsequent attributes
 * denote vertex properties
 * and are separated from the first by an equals sign:</p>
 * <code> -v vertex_id_column=vertex_prop1_column,... vertex_propn_column </code>
 * <p>or in the case there are no properties associated with the vertex id:
 * <code> vertex_id_column </code>
 *
 * </p>
 *  Because the endpoints of an edge must be vertices, all endpoints of edges are declared to be vertices.
 *  (The declaration is implicit, but the vertices really end up in the graph database.)
 * <p>
 *     EXAMPLES:
 *     <p>
 *<code>-conf /home/user/conf.xml -t my_hbase_table -v "cf:name=cf:age"  -d "cf:name,cf:dept,worksAt,cf:seniority"</code>
 *     </p>
 *     This generates a vertex for each employee annotated by their age, a vertex for each department with at least
 *     one employee, and a directed edge labeled "worksAt" between each employee and their department, annotated by their
 *     seniority in that department.
 * </p>
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
                .withDescription("Specify the columns which are undirected edge tokens; " +
                        "Example: --" + GBHTableConfig.config.getProperty("CMD_EDGES_OPTNAME") + "\"<src_vertex_col>,<dest_vertex_col>,<label>,[edge_property_col,...]\"..." +
                        "Note: Edge labels must be unique")
                .hasArgs()
                .withArgName("Edge-Column-Name")
                .create("e"));

        options.addOption(OptionBuilder.withLongOpt(GBHTableConfig.config.getProperty("CMD_DIRECTED_EDGES_OPTNAME"))
                .withDescription("Specify the columns which are directed edge tokens; " +
                        "Example: --" + GBHTableConfig.config.getProperty("CMD_DIRECTED_EDGES_OPTNAME") + "\"<src_vertex_col>,<dest_vertex_col>,<label>,[edge_property_col,...]\"..." +
                        "Note: Edge labels must be unique")
                .hasArgs()
                .withArgName("Edge-Column-Name")
                .create("d"));
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
            }

            if (cmd.hasOption(GBHTableConfig.config.getProperty("CMD_DIRECTED_EDGES_OPTNAME"))) {
                for (String e : cmd.getOptionValues(GBHTableConfig.config.getProperty("CMD_DIRECTED_EDGES_OPTNAME"))) {
                    LOG.info("Edges: " + e);
                }
            }

            if (!(cmd.hasOption(GBHTableConfig.config.getProperty("CMD_EDGES_OPTNAME"))) &&
                    !(cmd.hasOption(GBHTableConfig.config.getProperty("CMD_DIRECTED_EDGES_OPTNAME")))) {
                LOG.fatal("Please add column family and names for (directed) edges and (directed) edge properties");
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
        String srcTableName = cmd.getOptionValue(GBHTableConfig.config.getProperty("CMD_TABLE_OPTNAME"));

        Job                          job                 = new TableToTextGraph().new Job();
        HBaseInputConfiguration      inputConfiguration  = new HBaseInputConfiguration(srcTableName);
        BasicHBaseGraphBuildingRule  buildingRule        = new BasicHBaseGraphBuildingRule(cmd);
        TextGraphOutputConfiguration outputConfiguration = new TextGraphOutputConfiguration();


        LOG.info("============= Creating graph from hbase ==================");
        timer.start();
        job.run( inputConfiguration,buildingRule, outputConfiguration, cmd);
        LOG.info("========== Done creating graph from hbase ================");
        LOG.info("Time elapsed : " + timer.current_time() + " seconds");
    }
}