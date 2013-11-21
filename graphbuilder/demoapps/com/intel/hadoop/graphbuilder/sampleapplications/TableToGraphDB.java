

package com.intel.hadoop.graphbuilder.sampleapplications;

import com.intel.hadoop.graphbuilder.pipeline.input.hbase.GBHTableConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseGraphBuildingRule;
import com.intel.hadoop.graphbuilder.pipeline.output.titan.TitanCommandLineOptions;
import com.intel.hadoop.graphbuilder.pipeline.output.titan.TitanOutputConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.input.hbase.HBaseInputConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.GraphConstructionPipeline;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseGraphBuildingRuleCommandLineOptions;
import com.intel.hadoop.graphbuilder.util.CommandLineInterface;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import com.intel.hadoop.graphbuilder.util.Timer;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

/**
 * Generate a graph from rows of a big table, store in a graph database.
 * <p>
 *    <ul>
 *        <li>At present, only Hbase is supported for the big table</li>
 *        <li>At present, only Titan is supported for the graph database</li>
 *    </ul>
 * </p>
 *
 * <p>
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
 * <p>
 *     The option <code>-F</code> (for "flatten lists") specifies that when a cell containing a JSon list is read as a vertex ID, it is to be
 *     expanded into one vertex for each entry in the list. This applies to the source and destination columns for
 *     edges as well. It does not apply to properties.
 * </p>
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
 *
 */

public class TableToGraphDB {

    private static final Logger LOG = Logger.getLogger(TableToGraphDB.class);

    private static CommandLineInterface commandLineInterface = new CommandLineInterface();
    static {
        Options options = new Options();
        options.addOption("h", "help", false, "");

        options.addOption(OptionBuilder.withLongOpt(TitanCommandLineOptions.APPEND)
                .withDescription("Append Graph to Current Graph at Specified Titan Table")
                .create("a"));

        options.addOption(OptionBuilder.withLongOpt(GBHTableConfiguration.config.getProperty("CMD_TABLE_OPTNAME"))
                .withDescription("HBase table name")
                .hasArgs()
                .isRequired()
                .withArgName("HBase table name")
                .create("t"));

        options.addOption(OptionBuilder.withLongOpt(GBHTableConfiguration.config.getProperty("CMD_VERTICES_OPTNAME"))
                .withDescription("Specify the HBase columns which are vertex tokens and vertex properties" +
                        "Example: --" + GBHTableConfiguration.config.getProperty("CMD_VERTICES_OPTNAME") + "\"<vertex_col>=[<vertex_prop1>,...]\"")
                .hasArgs()
                .isRequired()
                .withArgName("Vertex-Column-Name")
                .create("v"));

        options.addOption(OptionBuilder.withLongOpt(GBHTableConfiguration.config.getProperty("CMD_EDGES_OPTNAME"))
                .withDescription("Specify the HTable columns which are undirected edge tokens; " +
                        "Example: --" + GBHTableConfiguration.config.getProperty("CMD_EDGES_OPTNAME") + "\"<src_vertex_col>,<dest_vertex_col>,<label>,[edge_property_col,...]\"..." +
                        "Note: Edge labels must be unique")
                .hasArgs()
                .withArgName("Edge-Column-Name")
                .create("e"));

        options.addOption(OptionBuilder.withLongOpt(HBaseGraphBuildingRuleCommandLineOptions.FLATTEN_LISTS_OPTNAME)
                .withDescription("Flag that expends lists into multiple items. " )
                .create("F"));
        options.addOption(OptionBuilder.withLongOpt(GBHTableConfiguration.config.getProperty("CMD_DIRECTED_EDGES_OPTNAME"))
                .withDescription("Specify the columns which are directed edge tokens; " +
                        "Example: --" + GBHTableConfiguration.config.getProperty("CMD_DIRECTED_EDGES_OPTNAME") + "\"<src_vertex_col>,<dest_vertex_col>,<label>,[edge_property_col,...]\"..." +
                        "Note: Edge labels must be unique")
                .hasArgs()
                .withArgName("Edge-Column-Name")
                .create("d"));


        commandLineInterface.setOptions(options);
    }

    /**
     * Encapsulation of the job setup process.
     */
    public class ConstructionPipeline extends GraphConstructionPipeline {

        /**
         * Should bidirectional edges be removed?
         *
         * @return   false:  this graph construction method allows bidirectional edges
         */
        @Override
        public boolean shouldCleanBiDirectionalEdges() {
            return false;
        }

        /**
         * Does this graph construction method use hbase?
         *
         * @return  true: this graph construction method uses hbase
         */
        @Override
        public boolean shouldUseHBase() {
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
            GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.BAD_COMMAND_LINE,
                    "Error parsing command line options", LOG);
        }

        CommandLine cmd = commandLineInterface.getCmd();

        ConstructionPipeline job                 = new TableToGraphDB().new ConstructionPipeline();
        job = (ConstructionPipeline) commandLineInterface.getRuntimeConfig().addConfig(job);

        String srcTableName = cmd.getOptionValue(GBHTableConfiguration.config.getProperty("CMD_TABLE_OPTNAME"));
        HBaseInputConfiguration  inputConfiguration  = new HBaseInputConfiguration(srcTableName);


        HBaseGraphBuildingRule buildingRule = new HBaseGraphBuildingRule(cmd);
        buildingRule.setFlattenLists(cmd.hasOption(HBaseGraphBuildingRuleCommandLineOptions.FLATTEN_LISTS_OPTNAME));

        TitanOutputConfiguration outputConfiguration = new TitanOutputConfiguration();

        LOG.info("============= Creating graph from feature table ==================");
        timer.start();
        job.run(inputConfiguration, buildingRule, outputConfiguration, cmd);
        LOG.info("========== Done creating graph from feature table ================");
        LOG.info("Time elapsed : " + timer.current_time() + " seconds");
    }
}