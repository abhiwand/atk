//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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
package com.intel.giraph.io.titan.formats;

import com.intel.giraph.io.titan.GiraphToTitanGraphFactory;
import com.intel.giraph.io.titan.common.GiraphTitanUtils;
import com.intel.graphbuilder.io.titan.formats.util.TitanInputFormat;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.util.List;

/**
 * Abstract class that uses TitanHBaseInputFormat to read Titan/Hadoop (i.e., Faunus) vertices from HBase.
 * <p/>
 * Subclasses can configure TitanHBaseInputFormat by using conf.set
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public abstract class TitanVertexInputFormat<I extends WritableComparable, V extends Writable, E extends Writable>
        extends VertexInputFormat<I, V, E> {

    protected TitanInputFormat titanInputFormat = null;


    /**
     * Check that input configuration is valid.
     *
     * @param conf Configuration
     */
    public void checkInputSpecs(Configuration conf) {

    }

    /**
     * Set up Titan/HBase configuration for Giraph
     *
     * @param conf :Giraph configuration
     */
    @Override
    public void setConf(ImmutableClassesGiraphConfiguration<I, V, E> conf) {
        super.setConf(conf);
        GiraphTitanUtils.sanityCheckInputParameters(conf);
        GiraphToTitanGraphFactory.addFaunusInputConfiguration(conf);

        this.titanInputFormat = TitanInputFormatFactory.getTitanInputFormat(conf);
        this.titanInputFormat.setConf(conf);
    }

    /**
     * Get input splits from Titan/HBase input format
     *
     * @param context           task context
     * @param minSplitCountHint minimal split count
     * @return List<InputSplit> list of input splits
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public List<InputSplit> getSplits(JobContext context, int minSplitCountHint) throws IOException,
            InterruptedException {

        return this.titanInputFormat.getSplits(context);
    }

}
