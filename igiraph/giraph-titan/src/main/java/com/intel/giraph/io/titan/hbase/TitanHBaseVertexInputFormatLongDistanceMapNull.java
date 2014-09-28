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
package com.intel.giraph.io.titan.hbase;

import com.intel.giraph.io.DistanceMapWritable;
import com.intel.giraph.io.titan.GiraphToTitanGraphFactory;
import com.intel.giraph.io.titan.common.GiraphTitanUtils;
import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_LABEL_LIST;

/**
 * TitanHBaseVertexInputFormatLongDistanceMapNull loads vertex
 * with <code>Long</code> vertex ID's,
 * <code>DistanceMap</code> vertex values,
 * and <code>Null</code> edge weights.
 */
public class TitanHBaseVertexInputFormatLongDistanceMapNull extends
        TitanHBaseVertexInputFormat<LongWritable, DistanceMapWritable, NullWritable> {

    /**
     * LOG class
     */
    private static final Logger LOG = Logger.getLogger(TitanHBaseVertexInputFormatLongDistanceMapNull.class);

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
    public void setConf(ImmutableClassesGiraphConfiguration<LongWritable, DistanceMapWritable, NullWritable> conf) {
        GiraphTitanUtils.setupHBase(conf);   //Move
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
    public VertexReader<LongWritable, DistanceMapWritable, NullWritable>
    createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {

        return new TitanHBaseVertexReader(split, context);            //Make into a class

    }

    /**
     * Uses the RecordReader to return HBase data
     */
    public static class TitanHBaseVertexReader extends
            HBaseVertexReader<LongWritable, DistanceMapWritable, NullWritable> {
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

            if (getRecordReader().nextKeyValue()) {
                vertex = readGiraphVertex(getConf(), getRecordReader().getCurrentValue());
                return true;
            }

            vertex = null;
            return false;
        }



        public Vertex<LongWritable, DistanceMapWritable, NullWritable> readGiraphVertex(final ImmutableClassesGiraphConfiguration conf, final FaunusVertex faunusVertex) {
            Vertex<LongWritable, DistanceMapWritable, NullWritable> vertex = conf.createVertex();
            String regexp = "[\\s,\\t]+";     //.split("/,?\s+/");
            long vertexId = faunusVertex.getLongId();

            vertex.initialize(new LongWritable(vertexId), new DistanceMapWritable());

            final String[] edgeLabelList = INPUT_EDGE_LABEL_LIST.get(conf).split(regexp);
            Set<String> edgeLabelKeys = new HashSet<>(Arrays.asList(edgeLabelList));

            for (final String edgeLabelKey : edgeLabelKeys) {
                EdgeLabel edgeLabel = faunusVertex.tx().getEdgeLabel(edgeLabelKey);
                for (final TitanEdge titanEdge : faunusVertex.getTitanEdges(Direction.OUT, edgeLabel)) {
                    Edge<LongWritable, NullWritable> edge = EdgeFactory.create(new LongWritable(
                            titanEdge.getOtherVertex(faunusVertex).getLongId()), NullWritable.get());
                    vertex.addEdge(edge);
                }
            }
            return (vertex);
        }

        /**
         * getCurrentVetex
         *
         * @return Vertex : Giraph vertex
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public Vertex<LongWritable, DistanceMapWritable, NullWritable> getCurrentVertex() throws IOException,
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
