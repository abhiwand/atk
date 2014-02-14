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

import com.intel.giraph.io.titan.GiraphToTitanGraphFactory;
import com.intel.giraph.io.titan.common.GiraphTitanUtils;
import com.thinkaurelius.titan.diskstorage.Backend;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.LONG_DOUBLE_NULL;

/**
 * TitanHBaseVertexInputFormatLongDoubleNull loads vertex
 * with <code>Long</code> vertex ID's,
 * <code>Double</code> vertex values,
 * and <code>Float</code> edge weights.
 */
public class TitanHBaseVertexInputFormatLongDoubleNull extends
    TitanHBaseVertexInputFormat<LongWritable, DoubleWritable, NullWritable> {

    /**
     * LOG class
     */
    private static final Logger LOG = Logger.getLogger(TitanHBaseVertexInputFormatLongDoubleNull.class);

    /**
     * checkInputSpecs
     *
     * @param conf : Giraph configuration
     */
    @Override
    public void checkInputSpecs(Configuration conf) {
    }

    /**
     * set up HBase based on users' configuration
     *
     * @param conf :Giraph configuration
     */
    @Override
    public void setConf(ImmutableClassesGiraphConfiguration<LongWritable, DoubleWritable, NullWritable> conf) {
        GiraphTitanUtils.setupHBase(conf);
        super.setConf(conf);
    }


    /**
     * create TitanHBaseVertexReader
     *
     * @param split   : inputsplits from TableInputFormat
     * @param context : task context
     * @return VertexReader
     * @throws IOException
     * @throws RuntimeException
     */
    public VertexReader<LongWritable, DoubleWritable, NullWritable>
    createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {

        return new TitanHBaseVertexReader(split, context);

    }

    /**
     * Uses the RecordReader to return HBase data
     */
    public static class TitanHBaseVertexReader extends
        HBaseVertexReader<LongWritable, DoubleWritable, NullWritable> {
        /**
         * Graph Reader to parse data in Titan Graph semantics
         */
        private TitanHBaseGraphReader graphReader;
        /**
         * Giraph Veretex
         */
        private Vertex vertex = null;
        /**
         * task context
         */
        private final TaskAttemptContext context;

        /**
         * TitanHBaseVertexReader constructor
         *
         * @param split   InputSplit from TableInputFormat
         * @param context task context
         * @throws IOException
         */
        public TitanHBaseVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
            this.context = context;
        }

        /**
         * @param inputSplit Input Split form HBase
         * @param context    task context
         */
        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException,
            InterruptedException {
            super.initialize(inputSplit, context);
            this.graphReader = new TitanHBaseGraphReader(
                GiraphToTitanGraphFactory.generateTitanConfiguration(context.getConfiguration(),
                    GIRAPH_TITAN.get(context.getConfiguration())));
        }

        /**
         * check whether these is nextVertex available
         *
         * @return boolean
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public boolean nextVertex() throws IOException, InterruptedException {
            //the edge store name used by Titan
            final byte[] edgeStoreFamily = Bytes.toBytes(Backend.EDGESTORE_NAME);

            while (getRecordReader().nextKeyValue()) {
                final Vertex temp = graphReader.readGiraphVertex(LONG_DOUBLE_NULL, getConf(),
                    getRecordReader().getCurrentKey().copyBytes(),
                    getRecordReader().getCurrentValue().getMap().get(edgeStoreFamily));
                if (null != temp) {
                    vertex = temp;
                    return true;
                }
            }
            vertex = null;
            return false;
        }

        /**
         * getCurrentVetex
         *
         * @return Vertex : Giraph vertex
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public Vertex<LongWritable, DoubleWritable, NullWritable> getCurrentVertex() throws IOException,
            InterruptedException {
            return vertex;
        }

        /**
         * close
         *
         * @throws IOException
         */
        public void close() throws IOException {
            this.graphReader.shutdown();
            super.close();
        }
    }
}
