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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

        if (!validateVertexRules()) {
            LOG.fatal("FAILURE: Attempt to generate a graph using column family names not present in the specified table.");
            System.exit(1);
        }

        if (!validateEdgeRules()) {
            LOG.fatal("FAILURE: Attempt to generate a graph using column family names not present in the specified table.");
            System.exit(1);
        }

        generateEdgeSchemata();
        generateVertexSchemata();

    }

    /**
     * Check the vertex generation for legal column families.
     * @return  true iff the supplied vertex rules all have column families valid for the table
     *
     * Note: because hbase allows different rows to contains different columns under each column family,
     *       we cannot meaningfully validate the full column name against an Hbase table
     */
    private boolean validateVertexRules(){

        boolean returnValue = true;

        for (String vertexRule : vertexRules) {
            if (vertexRule.contains("=")) {
                String[] columnNames = vertexRule.split("\\=");

                returnValue &= hBaseUtils.columnHasValidFamily(columnNames[0], srcTableName);

                String[] vertexPropertiesColumnNames = columnNames[1].split("\\,");
                for (String columnName : vertexPropertiesColumnNames) {
                    returnValue &= hBaseUtils.columnHasValidFamily(columnName, srcTableName);
                }
            } else {
                returnValue &= hBaseUtils.columnHasValidFamily(vertexRule, srcTableName);
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

    private boolean validateEdgeRules(){

        boolean returnValue = true;

        for (String edgeRule : edgeRules) {

            String[] columnNames      = edgeRule.split("\\,");
            String   srcVertexColName = columnNames[0];
            String   tgtVertexColName = columnNames[1];
            String   label            = columnNames[2];

            returnValue &= hBaseUtils.columnHasValidFamily(srcVertexColName, srcTableName);
            returnValue &= hBaseUtils.columnHasValidFamily(tgtVertexColName, srcTableName);

            for (int i = 3; i < columnNames.length; i++) {
                returnValue &= hBaseUtils.columnHasValidFamily(columnNames[i], srcTableName);
            }
        }

        return returnValue;
    }

    public void updateConfigurationForTokenizer(Configuration configuration, CommandLine cmd) {



        String   vertexConfigString = vertexRules[0];

        for (int i = 1; i < vertexRules.length; i++) {
            vertexConfigString += GBHTableConfig.config.getProperty("COL_NAME_SEPARATOR") + vertexRules[i];
        }

        configuration.set(GBHTableConfig.config.getProperty("VCN_CONF_NAME"), vertexConfigString);


        String   edgeConfigString = edgeRules[0];

        for (int i = 1; i < edgeRules.length; i++) {
            edgeConfigString += GBHTableConfig.config.getProperty("COL_NAME_SEPARATOR") + edgeRules[i];
        }

        configuration.set(GBHTableConfig.config.getProperty("ECN_CONF_NAME"), edgeConfigString);
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

            if (vertexRule.contains("=")) {

                String[] columnNames = vertexRule.split("\\=");

                for (String vertexPropertyColumnName : columnNames[1].split("\\,")) {
                    PropertySchema propertySchema = new PropertySchema(vertexPropertyColumnName, String.class);
                    vertexSchema.getPropertySchemata().add(propertySchema);
                }
            }

            graphSchema.addVertexSchema(vertexSchema);
        }
    }

    private void generateEdgeSchemata() {

        for (String edgeRule : edgeRules) {

            String[] columnNames      = edgeRule.split("\\,");
            String   label            = columnNames[2];

            EdgeSchema edgeSchema = new EdgeSchema(label);

            for (int i = 3; i < columnNames.length; i++) {
                String edgePropertyName = columnNames[i].replaceAll(GBHTableConfig.config.getProperty("HBASE_COLUMN_SEPARATOR"),
                        GBHTableConfig.config.getProperty("TRIBECA_GRAPH_PROPERTY_SEPARATOR"));
                PropertySchema propertySchema = new PropertySchema(edgePropertyName, String.class);
                edgeSchema.getPropertySchemata().add(propertySchema);
            }

            graphSchema.addEdgeSchema(edgeSchema);
        }
    }
}
