

package com.intel.hadoop.graphbuilder.pipeline.input.hbase;

import com.intel.hadoop.graphbuilder.pipeline.input.InputConfiguration;
import com.intel.hadoop.graphbuilder.graphelements.SerializedPropertyGraphElement;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
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
 * Constructor will terminate the process if it cannot connect to HBase.
 *
 * @see com.intel.hadoop.graphbuilder.pipeline.input.InputConfiguration
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
    public HBaseInputConfiguration(String srcTableName) {

        this.srcTableName = srcTableName;
        try {
            this.hBaseUtils = HBaseUtils.getInstance();
        } catch (IOException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.UNABLE_TO_CONNECT_TO_HBASE,
                    "Cannot allocate the HBaseUtils object. Check hbase connection.", LOG, e);
        }

        try {
            if (!hBaseUtils.tableExists(srcTableName)) {
                GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.MISSING_HBASE_TABLE,
                        "GRAPHBUILDER ERROR: " + srcTableName + " table does not exist", LOG);
            }
        } catch (IOException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.UNHANDLED_IO_EXCEPTION,
                    "GRAPHBUILDER ERROR: IO exception when attempting to read HBase table " + srcTableName, LOG, e);
        }
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

        srcTableName = cmd.getOptionValue(GBHTableConfiguration.config.getProperty("CMD_TABLE_OPTNAME"));

        configuration.set("SRCTABLENAME", srcTableName);


        scan.setCaching(GBHTableConfiguration.config.getPropertyInt("HBASE_CACHE_SIZE"));
        scan.setCacheBlocks(false);

        configuration.setBoolean("HBASE_TOKENIZER_FLATTEN_LISTS", cmd.hasOption("flattenlists"));
    }

    /**
     * Initialize the table mapper job.
     * @param job  Map reduce job in preparation for graph construction
     * @param cmd  User provided command line
     */
    public void updateJobForMapper(Job job, CommandLine cmd) {
        try {
            TableMapReduceUtil.initTableMapperJob(srcTableName, scan, HBaseReaderMapper.class, Text.class, SerializedPropertyGraphElement.class, job);
        } catch (IOException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.HADOOP_REPORTED_ERROR,
                    "Could not initialize table mapper job", LOG, e);
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
