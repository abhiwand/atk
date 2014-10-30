package com.intel.graphbuilder.io;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.util.List;

/**
 * HBase's default TableInputFormat class assigns a single mapper per region,
 * This splitting policy degrades performance when the size of individual regions
 * is large. This class increases the number of splits as follows:
 *
 * Total splits = max(hbase.mapreduce.regions.splits, existing number of regions)
 *
 * @see org.apache.hadoop.hbase.mapreduce.TableInputFormat
 * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)
 */
public class HBaseTableInputFormat extends TableInputFormat {

    private final Log LOG = LogFactory.getLog(TableInputFormat.class);
    public static final String NUM_REGION_SPLITS = "hbase.mapreduce.regions.splits";

    /**
     * Gets the input splits using a uniform-splitting policy.
     *
     * @param context Current job context.
     * @return List of input splits.
     * @throws java.io.IOException when creating the list of splits fails.
     * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)
     */
    @Override
    public List<InputSplit> getSplits(JobContext context)
            throws IOException {

        List<InputSplit> splits = super.getSplits(context); //Initial splits matches number of regions
        int requestedSplitCount = getRequestedSplitCount(context, splits);

        if (splits != null) {
            try {
                HBaseUniformSplitter uniformSplitter = new HBaseUniformSplitter(splits);
                splits = uniformSplitter.createInputSplits(requestedSplitCount);

            } catch (IllegalArgumentException e) {
                LOG.warn("Unable to create multiple input splits per region. "
                        + "Will use default split of one mapper per region.", e);
            }
            LOG.info("Generated " + splits.size() + " input splits for HBase table");
        }

        return splits;
    }

    /**
     * Get the requested input split count from the job configuration
     *
     * @param context Current job context which contains configuration.
     * @param splits List of input splits.
     * @return
     */
    private int getRequestedSplitCount(JobContext context, List<InputSplit> splits) {
        Configuration config  = context.getConfiguration();

        int initialSplitCount = splits.size();
        int requestedSplitCount  = config.getInt(NUM_REGION_SPLITS, initialSplitCount);

        return requestedSplitCount;
    }

}

