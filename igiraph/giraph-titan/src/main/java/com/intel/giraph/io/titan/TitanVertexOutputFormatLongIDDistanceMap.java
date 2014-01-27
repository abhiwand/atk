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
package com.intel.giraph.io.titan;

import com.intel.giraph.io.DistanceMapWritable;
import com.intel.giraph.io.titan.common.GiraphTitanUtils;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.CLOSED_GRAPH;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.OUTPUT_VERTEX_PROPERTY_KEY_LIST;

/**
 * The Vertex Output Format which writes back Giraph algorithm results
 * to Titan.
 * <p/>
 * Each Vertex is with <code>Long</code> id,
 * and <code>DistanceMap</code> values.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public class TitanVertexOutputFormatLongIDDistanceMap<I extends LongWritable,
    V extends DistanceMapWritable, E extends NullWritable>
    extends TextVertexOutputFormat<I, V, E> {
    /**
     * LOG class
     */
    private static final Logger LOG = Logger
        .getLogger(TitanVertexOutputFormatLongIDDistanceMap.class);


    /**
     * set up Titan based on users' configuration
     *
     * @param conf : Giraph configuration
     */
    @Override
    public void setConf(ImmutableClassesGiraphConfiguration<I, V, E> conf) {
        GiraphTitanUtils.setupTitanOutput(conf);
        super.setConf(conf);
    }

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
        return new TitanLongIDDistanceMapWriter();
    }

    /**
     * VertexWriter that writes Giraph results to Titan via BluePrint API
     * vertices with <code>Long</code> id
     * and <code>TwoVector</code> values.
     */
    protected class TitanLongIDDistanceMapWriter extends TextVertexWriterToEachLine {

        /**
         * TitanFactory to write back results
         */
        private TitanGraph graph = null;
        /**
         * Vertex value properties to filter
         */
        private String[] vertexValuePropertyKeyList = null;

        @Override
        public void initialize(TaskAttemptContext context) throws IOException,
            InterruptedException {
            super.initialize(context);
            this.graph = TitanGraphWriter.open(context);
            vertexValuePropertyKeyList = OUTPUT_VERTEX_PROPERTY_KEY_LIST.get(context.getConfiguration()).split(",");
        }

        @Override
        public Text convertVertexToLine(Vertex<I, V, E> vertex) throws IOException {

            long vertexId = vertex.getId().get();
            long numSources = 0;
            long sumHopCounts = 0;
            com.tinkerpop.blueprints.Vertex bluePrintVertex = this.graph.getVertex(vertexId);
            HashMap<Long, Integer> distanceMap = vertex.getValue().getDistanceMap();

            for (Map.Entry<Long, Integer> entry : distanceMap.entrySet()) {
                numSources++;
                sumHopCounts += entry.getValue();
            }

            bluePrintVertex.setProperty(vertexValuePropertyKeyList[0], Long.toString(numSources));
            bluePrintVertex.setProperty(vertexValuePropertyKeyList[1], Long.toString(sumHopCounts));
            return null;
        }

        /**
         * close
         *
         * @param context Task attempt context
         * @throws IOException
         */
        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            this.graph.commit();
            this.graph.shutdown();
            LOG.info(CLOSED_GRAPH);
            super.close(context);
        }
    }
}
