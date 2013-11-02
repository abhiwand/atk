package com.intel.hadoop.graphbuilder.demoapps.tabletotextgraph;

import com.intel.hadoop.graphbuilder.graphconstruction.inputmappers.GBHTableConfig;
import com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema.EdgeSchema;
import com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema.PropertyGraphSchema;
import com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema.PropertySchema;
import com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema.VertexSchema;
import com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.GraphBuildingRule;
import com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.GraphTokenizer;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.HBaseUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * This class handles the configuration time aspects of the graph construction rule (graph tokenizer).
 *
 * It is primarily responsible for:
 * - the parsing of the graph specification rule
 * -- including validation of the rules
 * -- including proving static parsing methods for use by the mapper-time graph construction routines
 *
 * - generating the property graph schema that this graph construction rule will result in  (so it can be used by
 *   the @code GraphGenerationMRJob , if necessary
 *
 * - at job setup time, populating the configuration with information required by the graph construction routine
 *  at mapper time
 *
 * @see GraphBuildingRule
 *
 *  The rules for specifying a graph are, at present, as follows:
 *
 *  EDGES:
 *  The first three attributes in the edge string are source vertex column, destination
 *  vertex column and the string label e.g.
 *  <src_vertex_col>,<dest_vertex_col>,<label>,[<edge_property_col1>,<edge_property_col2>,...]
 *
 *  VERTICES:
 *      Parse the column names of vertices and properties from command line prompt
 *      <vertex_col1>=[<vertex_prop1>,...] [<vertex_col2>=[<vertex_prop1>,...]]
 */


public class BasicHBaseGraphBuildingRule implements GraphBuildingRule {

    private static final Logger LOG = Logger.getLogger(BasicHBaseGraphBuildingRule.class);

    private PropertyGraphSchema graphSchema;
    private HBaseUtils          hBaseUtils;
    private String              srcTableName;
    private String[]            vertexRules;
    private String[]            edgeRules;

    public BasicHBaseGraphBuildingRule(CommandLine cmd) {

        graphSchema = new PropertyGraphSchema();
        hBaseUtils  = HBaseUtils.getInstance();

        srcTableName = cmd.getOptionValue(GBHTableConfig.config.getProperty("CMD_TABLE_OPTNAME"));
        vertexRules  = cmd.getOptionValues(GBHTableConfig.config.getProperty("CMD_VERTICES_OPTNAME"));
        edgeRules    = cmd.getOptionValues(GBHTableConfig.config.getProperty("CMD_EDGES_OPTNAME"));

        checkSyntaxOfVertexRules();
        checkSyntaxOfEdgeRules();

        validateVertexRuleColumnFamilies();
        validateEdgeRuleColumnFamilies();

        generateEdgeSchemata();
        generateVertexSchemata();

    }

    /**
     * Vertex rules actually have a minimum syntax, since they can be a lone column identifier specifying
     * a vertex ID
     */
    public void checkSyntaxOfVertexRules() {
        return;
    }

    /**
     * Verify that the edge rules are all syntactically correct
     *
     */
    private void checkSyntaxOfEdgeRules() {
         for (String edgeRule : edgeRules) {
             if (edgeRule.split("\\,").length < 3) {
                 LOG.fatal("Edge rule too short; does not specify <source>,<destination>,<label>");
                 LOG.fatal("The fatal rule: " + edgeRule);
                 System.exit(1);
             }
         }
    }

    /**
     * Check the vertex generation rules for legal column families.
     * @return  true iff the supplied vertex rules all have column families valid for the table
     *
     * Note: because hbase allows different rows to contains different columns under each column family,
     *       we cannot meaningfully validate the full column name against an Hbase table
     */
    private boolean validateVertexRuleColumnFamilies(){

        boolean returnValue = true;

        for (String vertexRule : vertexRules) {

                String vidColumn = BasicHBaseGraphBuildingRule.getVidColNameFromVertexRule(vertexRule);

                returnValue &= hBaseUtils.columnHasValidFamily(vidColumn, srcTableName);

                if (returnValue == false) {
                    LOG.fatal("FAILURE: attempt to generate graph using column family name not in specified hbase table");
                    LOG.fatal("colum name: " + vidColumn);
                    LOG.fatal("hbase table name: " + srcTableName);
                    System.exit(1);
                }

                String[] vertexPropertiesColumnNames =
                        BasicHBaseGraphBuildingRule.getVertexPropertyColumnsFromVertexRule(vertexRule);

                for (String columnName : vertexPropertiesColumnNames) {
                    returnValue &= hBaseUtils.columnHasValidFamily(columnName, srcTableName);
                    if (returnValue == false) {
                        LOG.fatal("FAILURE: attempt to generate graph using column family name not in specified hbase table");
                        LOG.fatal("colum name: " + columnName);
                        LOG.fatal("hbase table name: " + srcTableName);
                        System.exit(1);
                    }
                }

        }

        return returnValue;
    }

    /**
     * Check the vertex generation for legal column families.
     * @return  true iff the supplied vertex rules all have column families valid for the table
     *
     * Note: because hbase allows different rows to contains different columns under each column family,
     *       we cannot meaningfully validate the full column name against an Hbase table
     */

    private boolean validateEdgeRuleColumnFamilies(){

        boolean returnValue = true;

        for (String edgeRule : edgeRules) {

            String   srcVertexColName = BasicHBaseGraphBuildingRule.getSrcColNameFromEdgeRule(edgeRule);
            String   tgtVertexColName = BasicHBaseGraphBuildingRule.getDstColNameFromEdgeRule(edgeRule);

            List<String> propertyColNames = BasicHBaseGraphBuildingRule.getEdgePropertyColumnNamesFromEdgeRule(edgeRule);

            returnValue &= hBaseUtils.columnHasValidFamily(srcVertexColName, srcTableName);
            if (returnValue == false) {
                LOG.fatal("FAILURE: attempt to generate graph using column family name not in specified hbase table");
                LOG.fatal("colum name: " + srcVertexColName);
                LOG.fatal("hbase table name: " + srcTableName);
                System.exit(1);
            }

            returnValue &= hBaseUtils.columnHasValidFamily(tgtVertexColName, srcTableName);
            if (returnValue == false) {
                LOG.fatal("FAILURE: attempt to generate graph using column family name not in specified hbase table");
                LOG.fatal("colum name: " + tgtVertexColName);
                LOG.fatal("hbase table name: " + srcTableName);
                System.exit(1);
            }

            for (String propertyColName : propertyColNames) {
                returnValue &= hBaseUtils.columnHasValidFamily(propertyColName, srcTableName);
                if (returnValue == false) {
                    LOG.fatal("FAILURE: attempt to generate graph using column family name not in specified hbase table");
                    LOG.fatal("colum name: " + propertyColName);
                    LOG.fatal("hbase table name: " + srcTableName);
                    System.exit(1);
                }
            }
        }

        return returnValue;
    }

    public void updateConfigurationForTokenizer(Configuration configuration, CommandLine cmd) {
        packVertexRulesIntoConfiguration(configuration);
        packEdgeRulesIntoConfiguration(configuration);
    }


    public Class<? extends GraphTokenizer> getGraphTokenizerClass() {
        return BasicHBaseTokenizer.class;
    }

    public Class vidClass() {
        return StringType.class;
    }

    public PropertyGraphSchema getGraphSchema() {
        return graphSchema;
    }

    private void generateVertexSchemata() {

        for (String vertexRule : vertexRules) {

            VertexSchema vertexSchema = new VertexSchema();

            String[] columnNames = BasicHBaseGraphBuildingRule.getVertexPropertyColumnsFromVertexRule(vertexRule);

            for (String vertexPropertyColumnName : columnNames) {
                PropertySchema propertySchema = new PropertySchema(vertexPropertyColumnName, String.class);
                vertexSchema.getPropertySchemata().add(propertySchema);
            }

            graphSchema.addVertexSchema(vertexSchema);
        }
    }

    private void generateEdgeSchemata() {

        for (String edgeRule : edgeRules) {

            List<String> columnNames  = BasicHBaseGraphBuildingRule.getEdgePropertyColumnNamesFromEdgeRule(edgeRule);
            String   label            = BasicHBaseGraphBuildingRule.getLabelFromEdgeRule(edgeRule);

            EdgeSchema edgeSchema = new EdgeSchema(label);

            for (String columnName : columnNames) {
                String edgePropertyName = columnName.replaceAll(GBHTableConfig.config.getProperty("HBASE_COLUMN_SEPARATOR"),
                        GBHTableConfig.config.getProperty("TRIBECA_GRAPH_PROPERTY_SEPARATOR"));
                PropertySchema propertySchema = new PropertySchema(edgePropertyName, String.class);
                edgeSchema.getPropertySchemata().add(propertySchema);
            }

            graphSchema.addEdgeSchema(edgeSchema);
        }
    }

    private void packVertexRulesIntoConfiguration(Configuration configuration) {
        String   vertexConfigString = vertexRules[0];

        for (int i = 1; i < vertexRules.length; i++) {
            vertexConfigString += GBHTableConfig.config.getProperty("COL_NAME_SEPARATOR") + vertexRules[i];
        }

        configuration.set(GBHTableConfig.config.getProperty("VCN_CONF_NAME"), vertexConfigString);
    }

    public static String[] unpackVertexRulesFromConfiguration(Configuration configuration) {
        String   separators  = "\\" + GBHTableConfig.config.getProperty("COL_NAME_SEPARATOR");
        String[] vertexRules = configuration.get(GBHTableConfig.config.getProperty("VCN_CONF_NAME")).split(separators);

        return vertexRules;
    }

    private void packEdgeRulesIntoConfiguration(Configuration configuration) {
        String   edgeConfigString = edgeRules[0];

        for (int i = 1; i < edgeRules.length; i++) {
            edgeConfigString += GBHTableConfig.config.getProperty("COL_NAME_SEPARATOR") + edgeRules[i];
        }

        configuration.set(GBHTableConfig.config.getProperty("ECN_CONF_NAME"), edgeConfigString);
    }

    public static String[] unpackEdgeRulesFromConfiguration(Configuration configuration) {
        String   separator   = "\\" + GBHTableConfig.config.getProperty("COL_NAME_SEPARATOR");
        String[] edgeRules   = configuration.get(GBHTableConfig.config.getProperty("ECN_CONF_NAME")).split(separator);
        return edgeRules;
    }

    /*
     * Parse the column names of vertices and properties from command line prompt
     * <vertex_col1>=[<vertex_prop1>,...] [<vertex_col2>=[<vertex_prop1>,...]]
     */

    public static String getVidColNameFromVertexRule(String vertexRule) {

        String[] columnNames = vertexRule.split("\\=");
        String   vertexIdColumnName = columnNames[0];

        return vertexIdColumnName;
    }

    public static String[] getVertexPropertyColumnsFromVertexRule(String vertexRule) {
        String[] vertexPropertyColumns = null;

        if (vertexRule.contains("=")) {
            String[] columnNames = vertexRule.split("\\=");
            vertexPropertyColumns = columnNames[1].split("\\,");
        }  else {
            vertexPropertyColumns = new String[0];
        }

        return vertexPropertyColumns;

    }

    /*
     * The first three attributes in the edge string are source vertex column, destination
     * vertex column and the string label e.g.
     * <src_vertex_col>,<dest_vertex_col>,<label>,[<edge_property_col1>,<edge_property_col2>,...]
     */

    public static String getSrcColNameFromEdgeRule(String edgeRule) {
        String[] columnNames      = edgeRule.split("\\,");
        String   srcVertexColName = columnNames[0];

        return srcVertexColName;
    }

    public static String getDstColNameFromEdgeRule(String edgeRule) {
        String[] columnNames      = edgeRule.split("\\,");
        String   dstVertexColName = columnNames[1];
        return dstVertexColName;
    }

    public static String getLabelFromEdgeRule(String edgeRule) {
        String[] columnNames = edgeRule.split("\\,");
        String   label       = columnNames[2];
        return label;
    }

    public static ArrayList<String> getEdgePropertyColumnNamesFromEdgeRule(String edgeRule){
        String[] columnNames = edgeRule.split("\\,");
        ArrayList<String> edgePropertyColumnNames = new ArrayList<String>();

        if (columnNames.length >= 3) {
            for (int i = 3; i < columnNames.length; i++) {
                edgePropertyColumnNames.add(columnNames[i]);
            }
        }

        return edgePropertyColumnNames;
    }
}
