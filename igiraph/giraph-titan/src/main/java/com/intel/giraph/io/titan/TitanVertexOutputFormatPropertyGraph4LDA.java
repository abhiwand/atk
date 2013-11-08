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

import com.intel.giraph.io.VertexData4LDAWritable;
import com.intel.giraph.io.VertexData4LDAWritable.VertexType;
import com.intel.giraph.io.titan.common.GiraphTitanUtils;
import com.intel.mahout.math.DoubleWithVectorWritable;
import com.thinkaurelius.titan.core.*;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.apache.mahout.math.Vector;
import org.json.JSONArray;
import org.json.JSONException;

import java.io.IOException;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.*;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.EDGE_TYPE_PROPERTY_KEY;

/**
 * The Vertex Output Format which writes back Giraph algorithm results
 * to Titan for LDA algorithms.
 * <p/>
 * Features <code>VertexData4LDAWritable</code> vertex values and
 * <code>DoubleWithVectorWritable</code> out-edge info.
 * <p/>
 * Each vertex follows this format:
 * (<vertex id>, <vertex valueVector>, <vertex property>,
 * ((<dest vertex id>, <edge value>, <edge property>), ...))
 * <p/>
 * Here is an example of left-side vertex, with vertex id 1,
 * vertex value 4,3 marked as "d", and two edges.
 * First edge has a destination vertex 2, edge value 2.1.
 * Second edge has a destination vertex 3, edge value 0.7.
 * [1,[4,3],[d],[[2,2.1,[]],[3,0.7,[]]]]
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public class TitanVertexOutputFormatPropertyGraph4LDA<I extends LongWritable,
        V extends VertexData4LDAWritable, E extends DoubleWithVectorWritable>
        extends TextVertexOutputFormat<I, V, E> {

    /**
     * LOG class
     */
    private static final Logger LOG = Logger
            .getLogger(TitanVertexOutputFormatPropertyGraph4LDA.class);


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
        return new TitanVertexPropertyGraph4LDAWriter();
    }

    /**
     * VertexWriter that writes Giraph results to Titan via BluePrint API
     * vertices with <code>Long</code> id
     * and <code>TwoVector</code> values.
     */
    protected class TitanVertexPropertyGraph4LDAWriter extends TextVertexWriterToEachLine {

        /**
         * reader to parse Titan graph
         */
        private TitanGraph graph;
        /**
         * TitanTransaction to write back results
         */
        private TitanTransaction tx = null;
        /**
         * Vertex properties to filter
         */
        private String[] vertexPropertyKeyList;


        @Override
        public void initialize(TaskAttemptContext context) throws IOException,
                InterruptedException {
            super.initialize(context);
            this.graph = TitanGraphWriter.open(context);
            assert (null != this.graph);
            tx = graph.newTransaction();
            vertexPropertyKeyList = OUTPUT_VERTEX_PROPERTY_KEY_LIST.get(context.getConfiguration()).split(",");
            for (int i = 0; i < vertexPropertyKeyList.length; i++) {
                if(!tx.containsType(vertexPropertyKeyList[i])){
                    LOG.info("create vertex.property in Titan " + vertexPropertyKeyList[i]);
                    this.graph.makeKey(vertexPropertyKeyList[i]).dataType(String.class).make();
                }
            }
        }


        @Override
        public Text convertVertexToLine(Vertex<I, V, E> vertex) throws IOException {

            long vertex_id = vertex.getId().get();
            com.tinkerpop.blueprints.Vertex bluePrintVertex = this.graph.getVertex(vertex_id);
            Vector vector = vertex.getValue().getVector();

            //output vertex value
            if (vector.size() == vertexPropertyKeyList.length) {
                for (int i = 0; i < vector.size(); i++) {
                    bluePrintVertex.setProperty(vertexPropertyKeyList[i], Double.toString(vector.getQuick(i)));
                    //bluePrintVertex.setProperty(vertexPropertyKeyList[i], vector.getQuick(i));
                }
            } else {
                LOG.error("The number of output vertex property does not match! " +
                        "The size of vertex value vector is : " + vector.size() +
                        ", The size of output vertex property is: " + vertexPropertyKeyList.length);
                throw new IllegalArgumentException("The number of output vertex property does not match. Current Vertex is: " + vertex.getId());
            }

            return null;
        }

        @Override
        public void close(TaskAttemptContext context)
                throws IOException, InterruptedException {
            this.graph.shutdown();
            super.close(context);
        }
    }
}
