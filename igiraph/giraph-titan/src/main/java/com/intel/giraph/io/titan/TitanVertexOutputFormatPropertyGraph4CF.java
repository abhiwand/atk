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

import com.intel.giraph.io.VertexData4CFWritable;
import com.intel.giraph.io.titan.common.GiraphTitanUtils;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.apache.mahout.math.Vector;

import java.io.IOException;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.CLOSED_GRAPH;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.CURRENT_VERTEX;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.EXPECTED_SIZE_OF_VERTEX_PROPERTY;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.OPENED_GRAPH;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.OUTPUT_VERTEX_BIAS;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.OUTPUT_VERTEX_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.REAL_SIZE_OF_VERTEX_PROPERTY;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.VECTOR_VALUE;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.VERTEX_PROPERTY_MISMATCH;

/**
 * The Vertex Output Format which writes back Giraph algorithm results
 * to Titan for Collaborative filter algorithms.
 * <p/>
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
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public class TitanVertexOutputFormatPropertyGraph4CF<I extends LongWritable,
    V extends VertexData4CFWritable, E extends Writable>
    extends TextVertexOutputFormat<I, V, E> {

    /**
     * LOG class
     */
    private static final Logger LOG = Logger
        .getLogger(TitanVertexOutputFormatPropertyGraph4CF.class);


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
        return new TitanVertexPropertyGraph4CFWriter();
    }

    /**
     * VertexWriter that writes Giraph results to Titan via BluePrint API
     * vertices with <code>Long</code> id
     * and <code>TwoVector</code> values.
     */
    protected class TitanVertexPropertyGraph4CFWriter extends TextVertexWriterToEachLine {

        /**
         * TitanFactory to write back results
         */
        private TitanGraph graph = null;
        /**
         * Vertex value properties to filter
         */
        private String[] vertexValuePropertyKeyList = null;
        /**
         * Enable Vertex Bias output or not
         */
        private String enableVertexBias = "false";
        /**
         * Enable vector value
         */
        private String enableVectorValue = "true";

        @Override
        public void initialize(TaskAttemptContext context) throws IOException,
            InterruptedException {
            super.initialize(context);
            this.graph = TitanGraphWriter.open(context);
            LOG.info(OPENED_GRAPH);
            enableVertexBias = OUTPUT_VERTEX_BIAS.get(context.getConfiguration());
            enableVectorValue = VECTOR_VALUE.get(context.getConfiguration());
            vertexValuePropertyKeyList = OUTPUT_VERTEX_PROPERTY_KEY_LIST.get(context.getConfiguration()).split(",");
        }


        @Override
        public Text convertVertexToLine(Vertex<I, V, E> vertex) throws IOException {

            long vertexId = vertex.getId().get();
            com.tinkerpop.blueprints.Vertex bluePrintVertex = this.graph.getVertex(vertexId);
            Vector vector = vertex.getValue().getVector();
            int numValueProperty = 0;

            //output bias if enabled
            if (enableVertexBias.equals("true")) {
                //if output bias is enabled, the last property name is for output bias
                numValueProperty = vertexValuePropertyKeyList.length - 1;
                bluePrintVertex.setProperty(vertexValuePropertyKeyList[numValueProperty],
                    Double.toString(vertex.getValue().getBias()));
            } else {
                numValueProperty = vertexValuePropertyKeyList.length;
            }

            //output vertex value
            if (enableVectorValue.equals("true")) {
                //output in a comma separated value format
                if (numValueProperty == 1) {
                    String vertexValue = "";
                    for (int i = 0; i < vector.size(); i++) {
                        vertexValue += Double.toString(vector.getQuick(i));
                        if (i < (vector.size() - 1)) {
                            vertexValue = vertexValue + ",";
                        }
                    }
                    bluePrintVertex.setProperty(vertexValuePropertyKeyList[0], vertexValue);
                } else {
                    generateErrorMsg(1, vertex.getId().get());
                }
            } else {
                if (vector.size() == numValueProperty) {
                    for (int i = 0; i < vector.size(); i++) {
                        bluePrintVertex.setProperty(vertexValuePropertyKeyList[i],
                            Double.toString(vector.getQuick(i)));
                    }
                } else {
                    generateErrorMsg(vector.size(), vertex.getId().get());
                }
            }
            return null;
        }

        /**
         *
         * @param size  The number of vertex value properties
         * @param vertexId  The vertex Id
         */
        public void generateErrorMsg(int size, long vertexId) {
            LOG.error(VERTEX_PROPERTY_MISMATCH + EXPECTED_SIZE_OF_VERTEX_PROPERTY + size +
                REAL_SIZE_OF_VERTEX_PROPERTY + vertexValuePropertyKeyList.length);
            throw new IllegalArgumentException(VERTEX_PROPERTY_MISMATCH +
                CURRENT_VERTEX + vertexId);
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

