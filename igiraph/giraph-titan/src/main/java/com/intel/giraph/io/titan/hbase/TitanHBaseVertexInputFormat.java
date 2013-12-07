//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2013 Intel Corporation All Rights Reserved.
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
package com.intel.giraph.io.titan.hbase;

import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

/**
 * Base class that wraps an HBase TableInputFormat and underlying Scan object to
 * help instantiate vertices from an HBase table. All the static
 * TableInputFormat properties necessary to configure an HBase job are
 * available.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public abstract class TitanHBaseVertexInputFormat<I extends WritableComparable, V extends Writable, E extends Writable>
    extends VertexInputFormat<I, V, E> {

    /**
     * use HBase table input format
     */
    protected static final TableInputFormat INPUT_FORMAT = new TableInputFormat();

    /**
     * Takes an instance of RecordReader that supports HBase row-key, result
     * records. Subclasses can focus on vertex instantiation details without
     * worrying about connection semantics. Subclasses are expected to implement
     * nextVertex() and getCurrentVertex()
     *
     * @param <I> Vertex index value
     * @param <V> Vertex value
     * @param <E> Edge value
     */
    public abstract static class HBaseVertexReader<I extends WritableComparable, V extends Writable, E extends Writable>
        extends VertexReader<I, V, E> {

        /**
         * Reader instance
         */
        private RecordReader<ImmutableBytesWritable, Result> reader;
        /**
         * Context passed to initialize
         */
        private TaskAttemptContext context;

        /**
         * Create the hbase record reader. Override this to use a different
         * underlying record reader (useful for testing).
         *
         * @param inputSplit the split to read
         * @param context    the context passed to initialize
         * @return the record reader to be used
         * @throws IOException          exception that can be thrown during creation
         * @throws InterruptedException exception that can be thrown during
         *                              creation
         */
        protected RecordReader<ImmutableBytesWritable, Result>
        createHBaseRecordReader(InputSplit inputSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
            INPUT_FORMAT.setConf(context.getConfiguration());
            return INPUT_FORMAT.createRecordReader(inputSplit, context);
        }

        /**
         * initialize
         *
         * @param inputSplit Input split to be used for reading vertices.
         * @param context    Context from the task.
         * @throws IOException
         * @throws InterruptedException
         */
        public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException,
            InterruptedException {
            reader = createHBaseRecordReader(inputSplit, context);
            reader.initialize(inputSplit, context);
            this.context = context;
        }

        /**
         * close
         *
         * @throws IOException
         */
        public void close() throws IOException {
            reader.close();
        }

        /**
         * getProgress
         *
         * @return progress
         * @throws IOException
         * @throws InterruptedException
         */
        public float getProgress() throws IOException, InterruptedException {
            return reader.getProgress();
        }

        /**
         * getRecordReader
         *
         * @return Record reader to be used for reading.
         */
        protected RecordReader<ImmutableBytesWritable, Result> getRecordReader() {
            return reader;
        }

        /**
         * getContext
         *
         * @return Context passed to initialize.
         */
        protected TaskAttemptContext getContext() {
            return context;
        }

    }

    /**
     * getSplits from TableInputFormat
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
        INPUT_FORMAT.setConf(getConf());
        return INPUT_FORMAT.getSplits(context);
    }
}
