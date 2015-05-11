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

package com.intel.giraph.io.titan.formats;

import com.intel.giraph.io.titan.GiraphToTitanGraphFactory;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.formats.titan_050.util.CachedTitanInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Create RecordReader to read Faunus vertices from HBase
 * Subclasses need to implement nextVertex() and getCurrentVertex()
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public abstract class TitanVertexReader<I extends WritableComparable, V extends Writable, E extends Writable>
        extends VertexReader<I, V, E> {

    protected TitanVertexBuilder vertexBuilder = null;
    protected CachedTitanInputFormat titanInputFormat = null;
    private final RecordReader<NullWritable, FaunusVertex> recordReader;
    private TaskAttemptContext context;
    private final Logger LOG = Logger.getLogger(TitanVertexReader.class);

    /**
     * Sets the Titan/HBase TableInputFormat and creates a record reader.
     *
     * @param split   InputSplit
     * @param context Context
     * @throws java.io.IOException
     */
    public TitanVertexReader(InputSplit split, TaskAttemptContext context)
            throws IOException {
        Configuration conf = context.getConfiguration();
        GiraphToTitanGraphFactory.addFaunusInputConfiguration(conf);

        this.vertexBuilder = new TitanVertexBuilder(conf);
        this.titanInputFormat = TitanInputFormatFactory.getTitanInputFormat(conf);
        this.titanInputFormat.setConf(conf);

        try {
            this.recordReader = this.titanInputFormat.createRecordReader(split, context);
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