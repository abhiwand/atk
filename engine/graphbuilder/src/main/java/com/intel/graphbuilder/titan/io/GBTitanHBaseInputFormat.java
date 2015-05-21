//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.graphbuilder.titan.io;

import com.thinkaurelius.titan.hadoop.formats.titan_054.hbase.CachedTitanHBaseInputFormat;
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
 * <p/>
 * Total splits = max(hbase.mapreduce.regions.splits, existing number of regions)
 *
 * @see org.apache.hadoop.hbase.mapreduce.TableInputFormat
 * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)
 */
public class GBTitanHBaseInputFormat extends CachedTitanHBaseInputFormat {


    public static final String NUM_REGION_SPLITS = "hbase.mapreduce.regions.splits";

    private final Log LOG = LogFactory.getLog(TableInputFormat.class);

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
            throws IOException, InterruptedException {

        List<InputSplit> splits = getInitialRegionSplits(context);
        int requestedSplitCount = getRequestedSplitCount(context, splits);

        if (splits != null) {

            HBaseUniformSplitter uniformSplitter = new HBaseUniformSplitter(splits);
            splits = uniformSplitter.createInputSplits(requestedSplitCount);

            LOG.info("Generated " + splits.size() + " input splits for HBase table");
        }


        return splits;
    }

    /**
     * Get initial region splits. Default policy is one split per HBase region.
     *
     * @param context Job Context
     * @return Initial input splits for HBase table
     * @throws IOException
     */
    protected List<InputSplit> getInitialRegionSplits(JobContext context) throws IOException, InterruptedException {
        // This method helps initialize the mocks for unit tests
        return (super.getSplits(context));
    }

    /**
     * Get the requested input split count from the job configuration
     *
     * @param context Current job context which contains configuration.
     * @param splits  List of input splits.
     * @return Requested split count
     */
    protected int getRequestedSplitCount(JobContext context, List<InputSplit> splits) {
        Configuration config = context.getConfiguration();

        int initialSplitCount = splits.size();
        int requestedSplitCount = config.getInt(NUM_REGION_SPLITS, initialSplitCount);

        LOG.info("Set requested input splits for HBase table to: " + requestedSplitCount);
        return requestedSplitCount;
    }
}
