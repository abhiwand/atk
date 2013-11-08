

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
 * This class handles the set-up time configuration when the raw input is an Hbase table.
 *
 * For graph construction tasks that require multiple chained MR jobs, this class affects only the first MR job,
 * as that is the first mapper that deals with raw input.
 *
 * <ul>
 * <li> It provides a handle to the mapper class used to read hbase tables ({@code HBaseReaderMapper})</li>
 * <li> It prepares the MR job and configuration by calling hbase utilities</li>
 * </ul>
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

    /**
     * Allocate and acquire an instance of the singleton HBaseUtils
     */
    public HBaseInputConfiguration() {
        this.hBaseUtils = HBaseUtils.getInstance();
    }

    /**
     * This input configuration uses hbase.
     * @return  {@literal true }
     */
    public boolean usesHBase() {
        return true;
    }

    /**
     * Perform setup tasks with hbase.
     * @param configuration configuration being prepared for graph construction job
     * @param cmd  user provided command line
     */
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

        configuration.set("SRCTABLENAME", srcTableName);

        scan.setCaching(GBHTableConfig.config.getPropertyInt("HBASE_CACHE_SIZE"));
        scan.setCacheBlocks(false);
    }

    /**
     * Initialize the table mapper job.
     * @param job  Map reduce job in preparation for graph construction
     * @param cmd  User provided command line
     */
    public void updateJobForMapper(Job job, CommandLine cmd) {
        try {
            TableMapReduceUtil.initTableMapperJob(srcTableName, scan, HBaseReaderMapper.class, Text.class, PropertyGraphElement.class, job);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.fatal("Could not initialize table mapper job");
            System.exit(1);
        }
    }

    /**
     * The class of the mapper used.
     * @return {@code HBaseReaderMapper.class}
     * @see HBaseReaderMapper
     */
    public Class getMapperClass() {
        return mapperClass;
    }

    /**
     * Obtain description of the input configuration for logging purposes.
     * @return  "Hbase table name: " appended with source table name
     */
    public String getDescription() {
        return "Hbase table name: " + srcTableName;
    }
}
