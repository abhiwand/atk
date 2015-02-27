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

import com.intel.giraph.io.DistanceMapWritable;
import com.intel.giraph.io.titan.TitanGraphWriter;
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
    extends TitanVertexOutputFormat<I,V,E> {

    private static final Logger LOG = Logger.getLogger(TitanVertexOutputFormatLongIDDistanceMap.class);


    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
        return new TitanLongIDDistanceMapWriter();
    }

    /**
     * VertexWriter that writes Giraph results to Titan via BluePrint API
     * vertices with <code>Long</code> id
     * and <code>TwoVector</code> values.
     */
    public class TitanLongIDDistanceMapWriter extends TitanVertexWriterToEachLine {
        /**
         * Vertex value properties to filter
         */
        protected String[] vertexValuePropertyKeyList = null;

        /**
         * Initialize Titan vertex writer and open graph
         * @param context Task attempt context
         */
        @Override
        public void initialize(TaskAttemptContext context) throws IOException,
                InterruptedException {
            super.initialize(context);
            vertexValuePropertyKeyList = OUTPUT_VERTEX_PROPERTY_KEY_LIST.get(context.getConfiguration()).split(regexp);
        }

        /**
         * Write results to Titan vertex
         *
         * @param vertex Giraph vertex
         * @return   Text line to be written
         * @throws IOException
         */
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
            commitVerticesInBatches();
            return null;
        }

    }
}
