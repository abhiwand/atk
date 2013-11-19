

package com.intel.hadoop.graphbuilder.sampleapplications;

import com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseGraphBuildingRule;
import com.intel.hadoop.graphbuilder.pipeline.output.titan.TitanOutputConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.input.hbase.HBaseInputConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.GraphConstructionPipeline;
import com.intel.hadoop.graphbuilder.util.*;
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

        options.addOption(BaseCLI.Options.titanAppend.get());

        options.addOption(BaseCLI.Options.flattenList.get());

        options.addOption(BaseCLI.Options.hbaseTable.get());

        options.addOption(BaseCLI.Options.vertex.get());

        options.addOption(BaseCLI.Options.edge.get());

        options.addOption(BaseCLI.Options.directedEdge.get());

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
     */

    public static void main(String[] args)  {

        Timer timer = new Timer();

        CommandLine cmd = commandLineInterface.checkCli(args);

        ConstructionPipeline job                 = new TableToGraphDB().new ConstructionPipeline();
        job = (ConstructionPipeline) commandLineInterface.addConfig(job);

        String srcTableName = cmd.getOptionValue(CommonCommandLineOptions.Option.table.get());

        HBaseInputConfiguration  inputConfiguration  = new HBaseInputConfiguration(srcTableName);
        HBaseGraphBuildingRule buildingRule     = new HBaseGraphBuildingRule(cmd);
        TitanOutputConfiguration outputConfiguration = new TitanOutputConfiguration();

        LOG.info("============= Creating graph from feature table ==================");
        timer.start();
        job.run(inputConfiguration, buildingRule, outputConfiguration, cmd);
        LOG.info("========== Done creating graph from feature table ================");
        LOG.info("Time elapsed : " + timer.current_time() + " seconds");
    }
}