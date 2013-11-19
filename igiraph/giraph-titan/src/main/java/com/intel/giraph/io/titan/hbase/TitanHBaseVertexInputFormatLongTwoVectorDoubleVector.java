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

import com.intel.giraph.io.titan.common.GiraphTitanUtils;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import com.thinkaurelius.titan.diskstorage.Backend;
import org.apache.hadoop.hbase.util.Bytes;
import com.intel.giraph.io.titan.GiraphToTitanGraphFactory;
import org.apache.mahout.math.Vector;
import com.intel.mahout.math.TwoVectorWritable;
import com.intel.mahout.math.DoubleWithVectorWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import java.io.IOException;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.LONG_TWO_VECTOR_DOUBLE_VECTOR;

/**
 * TitanHBaseVertexInputFormatLongTwoVectorDoubleVector loads vertex
 * with <code>long</code> vertex ID's,
 * <code>TwoVector</code> vertex values: one for prior and
 * one for posterior,
 * and <code>DoubleVector</code> edge weights.
 */
public class TitanHBaseVertexInputFormatLongTwoVectorDoubleVector extends
        TitanHBaseVertexInputFormat<LongWritable, TwoVectorWritable, DoubleWithVectorWritable> {

    /**
     * LOG class
     */
    private static final Logger LOG = Logger
            .getLogger(TitanHBaseVertexInputFormatLongTwoVectorDoubleVector.class);

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
    public void setConf(
            ImmutableClassesGiraphConfiguration<LongWritable, TwoVectorWritable, DoubleWithVectorWritable> conf) {
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
    public VertexReader<LongWritable, TwoVectorWritable, DoubleWithVectorWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {

        return new TitanHBaseVertexReader(split, context);

    }

    /**
     * Uses the RecordReader to get HBase data
     */
    public static class TitanHBaseVertexReader extends
            HBaseVertexReader<LongWritable, TwoVectorWritable, DoubleWithVectorWritable> {
        /**
         * reader to parse Titan graph
         */
        private TitanHBaseGraphReader graphReader;
        /**
         * Giraph vertex
         */
        private Vertex<LongWritable, TwoVectorWritable, DoubleWithVectorWritable> vertex;
        /**
         * task context
         */
        private final TaskAttemptContext context;
        /**
         * The length of vertex value vector
         */
        private int cardinality = 0;

        /**
         * TitanHBaseVertexReader constructor
         *
         * @param split   Input Split
         * @param context Task context
         * @throws IOException
         */
        public TitanHBaseVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
            this.context = context;
        }

        /**
         * initialize TitanHBaseVertexReader
         *
         * @param inputSplit input splits
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
         * check whether there is next vertex
         *
         * @return boolean true if there is next vertex
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public boolean nextVertex() throws IOException, InterruptedException {
            //the edge store name used by Titan
            final byte[] edgeStoreFamily = Bytes.toBytes(Backend.EDGESTORE_NAME);

            if (getRecordReader().nextKeyValue()) {
                final Vertex<LongWritable, TwoVectorWritable, DoubleWithVectorWritable> temp = graphReader
                        .readGiraphVertex(LONG_TWO_VECTOR_DOUBLE_VECTOR, getConf(), getRecordReader()
                                .getCurrentKey().copyBytes(), getRecordReader().getCurrentValue().getMap()
                                .get(edgeStoreFamily));
                if (null != temp) {
                    vertex = temp;
                    return true;
                } else if (getRecordReader().nextKeyValue()) {
                    final Vertex<LongWritable, TwoVectorWritable, DoubleWithVectorWritable> temp1 = graphReader
                            .readGiraphVertex(LONG_TWO_VECTOR_DOUBLE_VECTOR, getConf(), getRecordReader()
                                    .getCurrentKey().copyBytes(), getRecordReader().getCurrentValue().getMap()
                                    .get(edgeStoreFamily));
                    if (null != temp1) {
                        vertex = temp1;
                        return true;
                    }
                }
            }
            return false;
        }

        /**
         * get current vertex with ID in long; value as two vectors, both in
         * double edge as two vectors, both in double
         *
         * @return Vertex Giraph vertex
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public Vertex<LongWritable, TwoVectorWritable, DoubleWithVectorWritable> getCurrentVertex()
            throws IOException, InterruptedException {
            return vertex;
        }

        /**
         * get vertex value
         *
         * @return TwoVectorWritable vertex value in two vectors
         * @throws IOException
         */
        protected TwoVectorWritable getValue() throws IOException {
            TwoVectorWritable vertexValue = vertex.getValue();
            Vector priorVector = vertexValue.getPriorVector();
            if (cardinality != priorVector.size()) {
                if (cardinality == 0) {
                    cardinality = priorVector.size();
                } else {
                    throw new IllegalArgumentException("Error in input data:" + "different cardinality!");
                }
            }
            return vertexValue;
        }

        /**
         * get edges of this vertex
         *
         * @return Iterable of Giraph edges
         * @throws IOException
         */
        protected Iterable<Edge<LongWritable, DoubleWithVectorWritable>> getEdges() throws IOException {
            return vertex.getEdges();
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