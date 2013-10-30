

package com.intel.hadoop.graphbuilder.graphconstruction.inputconfiguration;

import com.intel.hadoop.graphbuilder.graphconstruction.inputmappers.GBHTableConfig;
import com.intel.hadoop.graphbuilder.graphconstruction.inputmappers.HBaseReaderMapper;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.util.HBaseUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The methods in this class are used by the full MR chain to properly configure the first MR job to work
 * with the HBaseReaderMapper.
 *
 * Called when setting up the first MR job of a chain,
 * it initializes the configuration to read from teh source table and calls TableMapReduceUtil.initTableMapperJob
 *
 * @see InputConfiguration
 * @see HBaseReaderMapper
 *
 */

public class HBaseInputConfiguration implements InputConfiguration {

    private static final Logger LOG = Logger.getLogger(HBaseInputConfiguration.class);

    private HBaseUtils hBaseUtils   = null;
    private String     srcTableName = null;
    private Scan       scan         = new Scan();

    private Class      mapperClass  = HBaseReaderMapper.class;

    public HBaseInputConfiguration() {
        this.hBaseUtils = HBaseUtils.getInstance();
    }

    public boolean usesHBase() {
        return true;
    }


    private boolean checkColumnName(String fullColumnName, String tableName) {

        boolean returnValue = false;

        if (fullColumnName.contains(":")) {
            String columnFamilyName = fullColumnName.split(":")[0];
            try {
                returnValue = hBaseUtils.tableContainsColumnFamily(tableName, columnFamilyName);
            } catch (IOException e) {
                LOG.fatal("Unhandled IO exception.");
                System.exit(1);
            }
        }

        return returnValue;
    }


    private boolean validateVertexRules(String [] vertexRules, String tableName){

        boolean returnValue = true;

        for (String vertexRule : vertexRules) {
            if (vertexRule.contains("=")) {
                String[] columnNames = vertexRule.split("\\=");

                returnValue &= checkColumnName(columnNames[0], tableName);

                String[] vertexPropertiesColumnNames = columnNames[1].split("\\,");
                for (String columnName : vertexPropertiesColumnNames) {
                    returnValue &= checkColumnName(columnName, tableName);
                }
            } else {
                returnValue &= checkColumnName(vertexRule, tableName);
            }
        }

        return returnValue;
    }

    private boolean validateEdgeRules(String [] edgeRules, String tableName){

        boolean returnValue = true;

        for (String edgeRule : edgeRules) {

            String[] columnNames      = edgeRule.split("\\,");
            String   srcVertexColName = columnNames[0];
            String   tgtVertexColName = columnNames[1];
            String   label            = columnNames[2];

            returnValue &= checkColumnName(srcVertexColName, tableName);
            returnValue &= checkColumnName(tgtVertexColName, tableName);

            for (int i = 3; i < columnNames.length; i++) {
                returnValue &= checkColumnName(columnNames[i], tableName);
            }
        }

        return returnValue;
    }

    public void updateConfigurationForMapper(Configuration configuration, CommandLine cmd) {

        srcTableName = cmd.getOptionValue(GBHTableConfig.config.getProperty("CMD_TABLE_OPTNAME"));

        // Check if input table exists

        try {
            if (!hBaseUtils.tableExists(srcTableName)) {
                LOG.fatal("GRAPHBUILDER ERROR: " + srcTableName + " table does not exist");
                System.exit(1);
            }
        } catch (IOException e) {
            e.printStackTrace();
            LOG.fatal("Could not read input HBase Table named: " + srcTableName);
            System.exit(1);
        }

        String[] vertexRules  = cmd.getOptionValues(GBHTableConfig.config.getProperty("CMD_VERTICES_OPTNAME"));
        if (!validateVertexRules(vertexRules,srcTableName)) {
            LOG.fatal("FAILURE: Attempt to generate a graph using column family names not present in the specified table.");
            System.exit(1);
        }


        String   vertexConfigString = vertexRules[0];

        for (int i = 1; i < vertexRules.length; i++) {
            vertexConfigString += GBHTableConfig.config.getProperty("COL_NAME_SEPARATOR") + vertexRules[i];
        }

        configuration.set(GBHTableConfig.config.getProperty("VCN_CONF_NAME"), vertexConfigString);

        String[] edgeRules  = cmd.getOptionValues(GBHTableConfig.config.getProperty("CMD_EDGES_OPTNAME"));

        if (!validateEdgeRules(edgeRules,srcTableName)) {
            LOG.fatal("FAILURE: Attempt to generate a graph using column family names not present in the specified table.");
            System.exit(1);
        }

        String   edgeConfigString = edgeRules[0];

        for (int i = 1; i < edgeRules.length; i++) {
            edgeConfigString += GBHTableConfig.config.getProperty("COL_NAME_SEPARATOR") + edgeRules[i];
        }

        configuration.set(GBHTableConfig.config.getProperty("ECN_CONF_NAME"), edgeConfigString);
        configuration.set("SRCTABLENAME", srcTableName);

        scan.setCaching(GBHTableConfig.config.getPropertyInt("HBASE_CACHE_SIZE"));
        scan.setCacheBlocks(false);
    }

    public void updateJobForMapper(Job job, CommandLine cmd) {
        try {
            TableMapReduceUtil.initTableMapperJob(srcTableName, scan, HBaseReaderMapper.class, Text.class, PropertyGraphElement.class, job);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.fatal("Could not initialize table mapper job");
            System.exit(1);
        }
    }

    public Class getMapperClass() {
        return mapperClass;
    }

    public String getDescription() {
        return "HBase table, name: " + srcTableName;
    }
}
