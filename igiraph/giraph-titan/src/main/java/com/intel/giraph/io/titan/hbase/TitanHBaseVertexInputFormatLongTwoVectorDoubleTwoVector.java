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

import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_HOSTNAME;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_TABLENAME;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_PORT;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.GIRAPH_TITAN;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;

import com.intel.giraph.io.titan.GiraphToTitanGraphFactory;

import org.apache.mahout.math.Vector;

import com.intel.mahout.math.TwoVectorWritable;
import com.intel.mahout.math.DoubleWithTwoVectorWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.log4j.Logger;

import com.thinkaurelius.titan.diskstorage.Backend;

import java.io.IOException;
import java.nio.charset.Charset;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

/**
 * TitanHBaseVertexInputFormatLongDoubleFloat which input vertex with Long
 * vertex id Double vector as vertex value and Double edge value
 *
 */
public class TitanHBaseVertexInputFormatLongTwoVectorDoubleTwoVector extends
        TitanHBaseVertexInputFormat<LongWritable, TwoVectorWritable, DoubleWithTwoVectorWritable> {

    /** the edge store name used by Titan */
    static final byte[] EDGE_STORE_FAMILY = Bytes.toBytes(Backend.EDGESTORE_NAME);
    /** LOG class */
    private static final Logger LOG = Logger
            .getLogger(TitanHBaseVertexInputFormatLongTwoVectorDoubleTwoVector.class);

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
            ImmutableClassesGiraphConfiguration<LongWritable, TwoVectorWritable, DoubleWithTwoVectorWritable> conf) {
        conf.set(TableInputFormat.INPUT_TABLE, GIRAPH_TITAN_STORAGE_TABLENAME.get(conf));
        conf.set(HConstants.ZOOKEEPER_QUORUM, GIRAPH_TITAN_STORAGE_HOSTNAME.get(conf));
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, GIRAPH_TITAN_STORAGE_PORT.get(conf));

        Scan scan = new Scan();
        scan.addFamily(Backend.EDGESTORE_NAME.getBytes(Charset.forName("UTF-8")));

        try {
            conf.set(TableInputFormat.SCAN, convertScanToString(scan));
        } catch (IOException e) {
            LOG.error("cannot write scan into a Base64 encoded string!");
        }

        super.setConf(conf);
    }

    /**
     * Writes the given scan into a Base64 encoded string.
     *
     * @param scan The scan to write out.
     * @return The scan saved in a Base64 encoded string.
     * @throws IOException When writing the scan fails.
     */
    static String convertScanToString(Scan scan) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        scan.write(dos);
        return Base64.encodeBytes(out.toByteArray());
    }

    /**
     * create TitanHBaseVertexReader
     *
     * @param split : inputsplits from TableInputFormat
     * @param context : task context
     * @return VertexReader
     * @throws IOException
     * @throws RuntimeException
     */
    public VertexReader<LongWritable, TwoVectorWritable, DoubleWithTwoVectorWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {

        return new TitanHBaseVertexReader(split, context);

    }

    /**
     * Uses the RecordReader to get HBase data
     */
    public static class TitanHBaseVertexReader extends
            HBaseVertexReader<LongWritable, TwoVectorWritable, DoubleWithTwoVectorWritable> {
        /** reader to parse Titan graph */
        private TitanHBaseGraphReader graphReader;
        /** Giraph vertex */
        private Vertex<LongWritable, TwoVectorWritable, DoubleWithTwoVectorWritable> vertex;
        /** task context */
        private final TaskAttemptContext context;
        /** The length of vertex value vector */
        private int cardinality = 0;
        /** Data vector */
        private final Vector vector = null;

        /**
         * TitanHBaseVertexReader constructor
         *
         * @param split Input Split
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
         * @param context task context
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
            if (getRecordReader().nextKeyValue()) {
                final Vertex<LongWritable, TwoVectorWritable, DoubleWithTwoVectorWritable> temp = graphReader
                        .readGiraphVertexLoaderLongTwoVectorDoubleTwoVector(getConf(), getRecordReader()
                                .getCurrentKey().copyBytes(), getRecordReader().getCurrentValue().getMap()
                                .get(EDGE_STORE_FAMILY));
                if (null != temp) {
                    vertex = temp;
                    return true;
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
        public Vertex<LongWritable, TwoVectorWritable, DoubleWithTwoVectorWritable> getCurrentVertex()
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
        protected Iterable<Edge<LongWritable, DoubleWithTwoVectorWritable>> getEdges() throws IOException {
            return vertex.getEdges();
        }

    }
}