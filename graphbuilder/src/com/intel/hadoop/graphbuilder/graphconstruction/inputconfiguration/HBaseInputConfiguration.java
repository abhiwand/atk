

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

        String[] vertexColumnNames  = cmd.getOptionValues(GBHTableConfig.config.getProperty("CMD_VERTICES_OPTNAME"));
        String   vertexConfigString = vertexColumnNames[0];

        for (int i = 1; i < vertexColumnNames.length; i++) {
            vertexConfigString += GBHTableConfig.config.getProperty("COL_NAME_SEPARATOR") + vertexColumnNames[i];
        }

        configuration.set(GBHTableConfig.config.getProperty("VCN_CONF_NAME"), vertexConfigString);

        String[] edgeColumnNames  = cmd.getOptionValues(GBHTableConfig.config.getProperty("CMD_EDGES_OPTNAME"));

        if (edgeColumnNames != null) {
            String   edgeConfigString = edgeColumnNames[0];

            for (int i = 1; i < edgeColumnNames.length; i++) {
                edgeConfigString += GBHTableConfig.config.getProperty("COL_NAME_SEPARATOR") + edgeColumnNames[i];
            }

            configuration.set(GBHTableConfig.config.getProperty("ECN_CONF_NAME"), edgeConfigString);
        }

        String[] directedEdgeColumnNames  = cmd.getOptionValues(GBHTableConfig.config.getProperty("CMD_DIRECTED_EDGES_OPTNAME"));

        if (directedEdgeColumnNames != null) {
            String   directedEdgeConfigString = directedEdgeColumnNames[0];

            for (int i = 1; i < directedEdgeColumnNames.length; i++) {
                directedEdgeConfigString += GBHTableConfig.config.getProperty("COL_NAME_SEPARATOR") + directedEdgeColumnNames[i];
            }

            configuration.set(GBHTableConfig.config.getProperty("DECN_CONF_NAME"), directedEdgeConfigString);
        }

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
