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
import com.intel.graphbuilder.io.GBTitanHBaseInputFormat;
import com.intel.graphbuilder.io.titan.formats.util.TitanInputFormat;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

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

    protected static TitanInputFormat INPUT_FORMAT = new GBTitanHBaseInputFormat();
    protected static TitanVertexBuilder vertexBuilder = null;


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
        vertexBuilder = new TitanVertexBuilder(conf);
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
        Configuration hadoopConfig = getConf();
        GiraphToTitanGraphFactory.addFaunusInputConfiguration(hadoopConfig);
        INPUT_FORMAT.setConf(hadoopConfig);
        return INPUT_FORMAT.getSplits(context);
    }

    /**
     * Create RecordReader to read Faunus vertices from HBase
     * Subclasses need to implement nextVertex() and getCurrentVertex()
     *
     * @param <I> Vertex index value
     * @param <V> Vertex value
     * @param <E> Edge value
     */
    public static abstract class TitanHBaseVertexReader<I extends WritableComparable, V extends Writable, E extends Writable>
            extends VertexReader<I, V, E> {

        private final RecordReader<NullWritable, FaunusVertex> recordReader;
        private TaskAttemptContext context;
        private static final Logger LOG = Logger.getLogger(TitanHBaseVertexReader.class);

        /**
         * Sets the Titan/HBase TableInputFormat and creates a record reader.
         *
         * @param split   InputSplit
         * @param context Context
         * @throws IOException
         */
        public TitanHBaseVertexReader(InputSplit split, TaskAttemptContext context)
                throws IOException {
            GiraphToTitanGraphFactory.addFaunusInputConfiguration(context.getConfiguration());
            INPUT_FORMAT.setConf(context.getConfiguration());
            try {
                this.recordReader = INPUT_FORMAT.createRecordReader(split, context);
            } catch (InterruptedException e) {
                LOG.error("Interruption while creating record reader for Titan/HBase", e);
                throw new IOException(e);
            }
        }

        /**
         * Initialize the record reader
         *
         * @param inputSplit Input split to be used for reading vertices.
         * @param context    Context from the task.
         * @throws IOException
         * @throws InterruptedException
         */
        public void initialize(InputSplit inputSplit,
                               TaskAttemptContext context)
                throws IOException, InterruptedException {
            this.recordReader.initialize(inputSplit, context);
            this.context = context;
        }

        /**
         * Get progress of Titan/HBase record reader
         *
         * @return Progress of record reader
         * @throws IOException
         * @throws InterruptedException
         */
        public float getProgress() throws IOException, InterruptedException {
            return this.recordReader.getProgress();
        }

        /**
         * Get Titan/HBase record reader
         *
         * @return Record recordReader to be used for reading.
         */
        protected RecordReader<NullWritable, FaunusVertex> getRecordReader() {
            return this.recordReader;
        }

        /**
         * Get task context
         *
         * @return Context passed to initialize.
         */
        protected TaskAttemptContext getContext() {
            return this.context;
        }

        /**
         * Close the Titan/HBase record reader
         *
         * @throws IOException
         */
        public void close() throws IOException {
            this.recordReader.close();
        }

    }

}
