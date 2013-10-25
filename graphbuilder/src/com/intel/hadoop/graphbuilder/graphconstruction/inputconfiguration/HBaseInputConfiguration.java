

package com.intel.hadoop.graphbuilder.graphconstruction.inputconfiguration;

import com.intel.hadoop.graphbuilder.graphconstruction.inputmappers.GBHTableConfig;
import com.intel.hadoop.graphbuilder.graphconstruction.inputmappers.HBaseReaderMapper;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.util.GraphbuilderExit;
import com.intel.hadoop.graphbuilder.util.HBaseUtils;
import com.intel.hadoop.graphbuilder.util.StatusCode;
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
 * Constructor will terminate the process if it cannot connect to HBase.
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
        try {
            this.hBaseUtils = HBaseUtils.getInstance();
        } catch (IOException e) {
            GraphbuilderExit.graphbuilderFatalExitException(StatusCode.UNABLE_TO_CONNECT_TO_HBASE,
                    "Cannot allocate the HBaseUtils object. Check hbase connection.", LOG, e);
        }
    }

    public boolean usesHBase() {
        return true;
    }

    public void updateConfigurationForMapper(Configuration configuration, CommandLine cmd) {

        srcTableName = cmd.getOptionValue(GBHTableConfig.config.getProperty("CMD_TABLE_OPTNAME"));

        // Check if input table exists

        try {
            if (!hBaseUtils.tableExists(srcTableName)) {
                GraphbuilderExit.graphbuilderFatalExitNoException(StatusCode.MISSING_HBASE_TABLE,
                        "GRAPHBUILDER ERROR: " + srcTableName + " table does not exist", LOG);
            }
        } catch (IOException e) {
            GraphbuilderExit.graphbuilderFatalExitException(StatusCode.UNHANDLED_IO_EXCEPTION,
                    "Could not read input HBase Table named: " + srcTableName, LOG, e);
        }

        String[] vertexColumnNames  = cmd.getOptionValues(GBHTableConfig.config.getProperty("CMD_VERTICES_OPTNAME"));
        String   vertexConfigString = vertexColumnNames[0];

        for (int i = 1; i < vertexColumnNames.length; i++) {
            vertexConfigString += GBHTableConfig.config.getProperty("COL_NAME_SEPARATOR") + vertexColumnNames[i];
        }

        configuration.set(GBHTableConfig.config.getProperty("VCN_CONF_NAME"), vertexConfigString);

        String[] edgeColumnNames  = cmd.getOptionValues(GBHTableConfig.config.getProperty("CMD_EDGES_OPTNAME"));
        String   edgeConfigString = edgeColumnNames[0];

        for (int i = 1; i < edgeColumnNames.length; i++) {
            edgeConfigString += GBHTableConfig.config.getProperty("COL_NAME_SEPARATOR") + edgeColumnNames[i];
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
            GraphbuilderExit.graphbuilderFatalExitException(StatusCode.HADOOP_REPORTED_ERROR,
                    "Could not initialize table mapper job", LOG, e);
        }
    }

    public Class getMapperClass() {
        return mapperClass;
    }

    public String getDescription() {
        return "HBase table, name: " + srcTableName;
    }
}
