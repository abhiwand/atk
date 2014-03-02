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

import com.intel.giraph.io.EdgeData4CFWritable;
import com.intel.giraph.io.VertexData4CFWritable;
import com.intel.giraph.io.titan.GiraphToTitanGraphFactory;
import com.intel.giraph.io.titan.common.GiraphTitanUtils;
import com.thinkaurelius.titan.diskstorage.Backend;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.apache.mahout.math.Vector;

import java.io.IOException;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.*;


/**
 * TitanHBaseVertexInputFormatPropertyGraph4CF loads vertex
 * Features <code>VertexData</code> vertex values and
 * <code>EdgeData</code> out-edge info.
 * <p/>
 * Each vertex follows this format:
 * (<vertex id>, <vertex valueVector>, <vertex property>,
 * ((<dest vertex id>, <edge value>, <edge property>), ...))
 * <p/>
 * Here is an example of left-side vertex, with vertex id 1,
 * vertex value 4,3 marked as "l", and two edges.
 * First edge has a destination vertex 2, edge value 2.1, marked as "tr".
 * Second edge has a destination vertex 3, edge value 0.7,marked as "va".
 * [1,[4,3],[L],[[2,2.1,[tr]],[3,0.7,[va]]]]
 */
public class TitanHBaseVertexInputFormatPropertyGraph4CF extends
    TitanHBaseVertexInputFormat<LongWritable, VertexData4CFWritable, EdgeData4CFWritable> {

    /**
     * LOG class
     */
    private static final Logger LOG = Logger
        .getLogger(TitanHBaseVertexInputFormatPropertyGraph4CF.class);

    /**
     * checkInputSpecs
     *
     * @param conf : Giraph configuration
     */
    @Override
    public void checkInputSpecs(Configuration conf) {
    }

    /**
     * set up HBase with based on users' configuration
     *
     * @param conf :Giraph configuration
     */
    @Override
    public void setConf(
        ImmutableClassesGiraphConfiguration<LongWritable, VertexData4CFWritable, EdgeData4CFWritable> conf) {
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
    public VertexReader<LongWritable, VertexData4CFWritable, EdgeData4CFWritable> createVertexReader(
        InputSplit split, TaskAttemptContext context) throws IOException {

        return new TitanHBaseVertexReader(split, context);

    }

    /**
     * Uses the RecordReader to get HBase data
     */
    public static class TitanHBaseVertexReader extends
        HBaseVertexReader<LongWritable, VertexData4CFWritable, EdgeData4CFWritable> {
        /**
         * reader to parse Titan graph
         */
        private TitanHBaseGraphReader graphReader;
        /**
         * Giraph vertex
         */
        private Vertex<LongWritable, VertexData4CFWritable, EdgeData4CFWritable> vertex;
        /**
         * task context
         */
        private final TaskAttemptContext context;
        /**
         * The length of vertex value vector
         */
        private int cardinality = -1;

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

            while (getRecordReader().nextKeyValue()) {
                final Vertex<LongWritable, VertexData4CFWritable, EdgeData4CFWritable> temp = graphReader
                    .readGiraphVertex(PROPERTY_GRAPH_4_CF, getConf(), getRecordReader()
                        .getCurrentKey().copyBytes(), getRecordReader().getCurrentValue().getMap()
                        .get(edgeStoreFamily));
                if (null != temp) {
                    vertex = temp;
                    return true;
                }
            }
            vertex = null;
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
        public Vertex<LongWritable, VertexData4CFWritable, EdgeData4CFWritable> getCurrentVertex()
            throws IOException, InterruptedException {
            return vertex;
        }

        /**
         * get vertex value
         *
         * @return VertexDataWritable vertex value in vector
         * @throws IOException
         */
        protected VertexData4CFWritable getValue() throws IOException {
            VertexData4CFWritable vertexValue = vertex.getValue();
            Vector vector = vertexValue.getVector();
            if (cardinality != vector.size()) {
                if (cardinality == -1) {
                    cardinality = vector.size();
                } else {
                    throw new IllegalArgumentException(INPUT_DATA_ERROR);
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
        protected Iterable<Edge<LongWritable, EdgeData4CFWritable>> getEdges() throws IOException {
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
