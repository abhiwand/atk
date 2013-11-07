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
import com.intel.mahout.math.DoubleWithVectorWritable;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Direction;
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

import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.*;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.EDGE_TYPE_PROPERTY_KEY;

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
public class TitanVertexOutputFormatPropertyGraph4LDA <I extends LongWritable,
        V extends VertexData4LDAWritable, E extends DoubleWithVectorWritable>
        extends TextVertexOutputFormat<I, V, E> {

    /**
     * LOG class
     */
    private static final Logger LOG = Logger
            .getLogger(TitanVertexOutputFormatPropertyGraph4LDA.class);


    /**
     * set up Titan with based on users' configuration
     *
     * @param conf : Giraph configuration
     */
    @Override
    public void setConf(ImmutableClassesGiraphConfiguration<I, V, E> conf) {
        sanityCheckInputParameters(conf);
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        super.setConf(conf);
    }

    /**
     * check whether input parameter is valid
     *
     * @param conf : Giraph configuration
     */
    public void sanityCheckInputParameters(ImmutableClassesGiraphConfiguration<I, V, E> conf) {
        String[] vertexPropertyKeyList = OUTPUT_VERTEX_PROPERTY_KEY_LIST.get(conf).split(",");
        if (vertexPropertyKeyList.length == 0) {
            throw new IllegalArgumentException("Please configure output vertex property list by -D" +
                    OUTPUT_VERTEX_PROPERTY_KEY_LIST.getKey() + ". Otherwise no vertex result will be written.");
        }

        if (GIRAPH_TITAN_STORAGE_BACKEND.get(conf).equals("")) {
            throw new IllegalArgumentException("Please configure Titan storage backend by -D" +
                    GIRAPH_TITAN_STORAGE_BACKEND.getKey() + ". Otherwise no vertex will be read from Titan.");
        }

        if (GIRAPH_TITAN_STORAGE_TABLENAME.get(conf).equals("")) {
            throw new IllegalArgumentException("Please configure Titan storage Table name by -D" +
                    GIRAPH_TITAN_STORAGE_TABLENAME.getKey() + ". Otherwise no vertex will be read from Titan.");
        }

        if (GIRAPH_TITAN_STORAGE_HOSTNAME.get(conf).equals("")) {
            throw new IllegalArgumentException("Please configure Titan storage hostname by -D" +
                    GIRAPH_TITAN_STORAGE_HOSTNAME.getKey() + ". Otherwise no vertex will be read from Titan.");
        }

        if (GIRAPH_TITAN_STORAGE_PORT.isDefaultValue(conf)) {
            LOG.info(GIRAPH_TITAN_STORAGE_PORT.getKey() + " is configured as default value. " +
                    "Ensure you are using port " + GIRAPH_TITAN_STORAGE_PORT.get(conf));
        }

        if (GIRAPH_TITAN_STORAGE_READ_ONLY.get(conf).equals("true")) {
            throw new IllegalArgumentException("Please turnoff Titan storage read-only by -D" +
                    GIRAPH_TITAN_STORAGE_READ_ONLY.getKey() + ". Otherwise no vertex will be read from Titan.");
        }

        if (VERTEX_TYPE_PROPERTY_KEY.get(conf).equals("")) {
            LOG.info("No vertex type property specified. Ensure your " +
                    "InputFormat does not require one.");
        }

        if (EDGE_TYPE_PROPERTY_KEY.get(conf).equals("")) {
            LOG.info("No edge type property specified. Ensure your " +
                    "InputFormat does not require one.");
        }
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
         * reader to parse Titan graph
         */
        private TitanGraph graph;
        /**
         * Vertex properties to filter
         */
        private String[] vertexPropertyKeyList;
        /**
         * Enable Vertex Bias output or not
         */
        private String enableVertexBias;

        @Override
        public void initialize(TaskAttemptContext context) throws IOException,
                InterruptedException {
            super.initialize(context);
            this.graph = TitanGraphWriter.open(context);
            assert (null != this.graph);
            enableVertexBias = OUTPUT_VERTEX_BIAS.get(context.getConfiguration());
            vertexPropertyKeyList = OUTPUT_VERTEX_PROPERTY_KEY_LIST.get(context.getConfiguration()).split(",");
            for (int i = 0; i < vertexPropertyKeyList.length; i++) {
                LOG.info("create vertex.property in Titan " + vertexPropertyKeyList[i]);
                //     this.graph.makeType().name().unique(Direction.OUT).dataType(String.class)
                //             .makePropertyKey();
                this.graph.makeKey(vertexPropertyKeyList[i]).dataType(String.class).make();

                //  this.graph.makeType().name(vertexPropertyKeyList[i]).unique(Direction.OUT).dataType(Double.class)
                //        .makePropertyKey();
            }
        }


        @Override
        public Text convertVertexToLine(Vertex<I, V, E> vertex) throws IOException {

            long vertex_id = vertex.getId().get();
            com.tinkerpop.blueprints.Vertex bluePrintVertex = this.graph.getVertex(vertex_id);
            Vector vector = vertex.getValue().getVector();
            JSONArray jsonVertex = new JSONArray();
            int numValueProperty = 0;

            //output bias if enabled
            if (enableVertexBias.equals("true")) {
                //if output bias is enabled, the last property name is for output bias
                numValueProperty = vertexPropertyKeyList.length - 1;
                bluePrintVertex.setProperty(vertexPropertyKeyList[numValueProperty], Double.toString(vertex.getValue().getBias()));
            } else {
                numValueProperty = vertexPropertyKeyList.length;
            }

            //output vertex value
            if (vector.size() == numValueProperty) {
                try {
                    jsonVertex.put(vertex_id);
                    JSONArray jsonBiasArray = new JSONArray();
                    jsonBiasArray.put(vertex.getValue().getBias());
                    jsonVertex.put(jsonBiasArray);
                    JSONArray jsonValueArray = new JSONArray();
                    for (int i = 0; i < vector.size(); i++) {
                        jsonValueArray.put(vector.getQuick(i));
                        bluePrintVertex.setProperty(vertexPropertyKeyList[i], Double.toString(vector.getQuick(i)));
                        //bluePrintVertex.setProperty(vertexPropertyKeyList[i], vector.getQuick(i));
                    }
                    this.graph.commit();
                    jsonVertex.put(jsonValueArray);
                    // add vertex type
                    JSONArray jsonTypeArray = new JSONArray();
                    VertexType vertexType = vertex.getValue().getType();
                    String vertexString = null;
                    switch (vertexType) {
                        case DOC:
                            vertexString = "d";
                            break;
                        case WORD:
                            vertexString = "w";
                            break;
                        default:
                            throw new IllegalArgumentException(String.format("Unrecognized vertex type: %s", vertexType.toString()));
                    }
                    jsonTypeArray.put(vertexString);
                    jsonVertex.put(jsonTypeArray);
                } catch (JSONException e) {
                    throw new IllegalArgumentException("writeVertex: Couldn't write vertex " + vertex);
                }
            } else {
                LOG.error("The number of output vertex property does not match! " +
                        "The size of vertex value vector is : " + vector.size() +
                        ", The size of output vertex property is: " + vertexPropertyKeyList.length);
                throw new IllegalArgumentException("The number of output vertex property does not match. Current Vertex is: " + vertex.getId());
            }

            return new Text(jsonVertex.toString());
        }
    }
}
