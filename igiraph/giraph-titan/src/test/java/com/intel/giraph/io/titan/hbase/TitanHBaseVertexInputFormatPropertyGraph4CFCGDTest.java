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

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.intel.giraph.algorithms.cgd.ConjugateGradientDescentComputation;
import com.intel.giraph.io.EdgeDataWritable;
import com.intel.giraph.io.VertexData4CGDWritable;
import com.intel.giraph.io.formats.JsonPropertyGraph4CFOutputFormat;
import com.intel.giraph.io.titan.TitanTestBase;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanVertex;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.io.LongWritable;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.EDGE_TYPE_PROPERTY_KEY;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_LABEL_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.TITAN_ID_OFFSET;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.VERTEX_TYPE_PROPERTY_KEY;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.LEFT_VERTEX_TYPE;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.RIGHT_VERTEX_TYPE;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.EDGE_TYPE_TRAIN;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.EDGE_TYPE_TEST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.EDGE_TYPE_VALIDATION;


import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

/**
 * Test TitanHBaseVertexInputFormatPropertyGraph4CF which loads vertex
 * with  <code>VertexData</code> vertex values and
 * <code>EdgeData</code> out-edge info.
 * <p/>
 * Each vertex follows this format:
 * (<vertex id>, <vertex valueVector>, <vertex property>,
 * ((<dest vertex id>, <edge value>, <edge property>), ...))
 * <p/>
 * Here is an example of left-side vertex, with vertex id 1,
 * vertex value 4,3 marked as "l", and two edges.
 * First edge has a destination vertex 2, edge value 2.1, marked as EDGE_TYPE_TRAIN.
 * Second edge has a destination vertex 3, edge value 0.7,marked as "va".
 * [1,[4,3],[L],[[2,2.1,[tr]],[3,0.7,[va]]]]
 */
public class TitanHBaseVertexInputFormatPropertyGraph4CFCGDTest 
    extends TitanTestBase<LongWritable, VertexData4CGDWritable, EdgeDataWritable> {

    @Override
    protected void configure() throws Exception {
        giraphConf.setComputationClass(ConjugateGradientDescentComputation.class);
        giraphConf.setMasterComputeClass(ConjugateGradientDescentComputation.ConjugateGradientDescentMasterCompute.class);
        giraphConf.setAggregatorWriterClass(ConjugateGradientDescentComputation.ConjugateGradientDescentAggregatorWriter.class);
        giraphConf.setVertexInputFormatClass(TitanHBaseVertexInputFormatPropertyGraph4CFCGD.class);
        giraphConf.setVertexOutputFormatClass(JsonPropertyGraph4CFOutputFormat.class);
        giraphConf.set("cgd.maxSupersteps", "6");
        giraphConf.set("cgd.featureDimension", "3");
        giraphConf.set("cgd.lambda", "0.05");
        giraphConf.set("cgd.convergenceThreshold", "0");
        giraphConf.set("cgd.minVal", "1");
        giraphConf.set("cgd.maxVal", "5");
        giraphConf.set("cgd.numCGDIters", "5");
        giraphConf.set("cgd.biasOn", "true");

        INPUT_EDGE_PROPERTY_KEY_LIST.set(giraphConf, "weight");
        INPUT_EDGE_LABEL_LIST.set(giraphConf, "edge");
        VERTEX_TYPE_PROPERTY_KEY.set(giraphConf, "vertexType");
        EDGE_TYPE_PROPERTY_KEY.set(giraphConf, "edgeType");

    }

    //@Ignore
    @Test
    public void VertexInputFormatPropertyGraph4CFCGDTest() throws Exception {
        /*
        String[] graph = new String[] {
            "[0,[],[L],[[2,1,[tr]],[3,2,[te]]]]",
            "[1,[],[L],[[2,5,[tr]],[4,3,[va]]]]",
            "[2,[],[R],[[0,1,[tr]],[1,5,[tr]]]]",
            "[3,[],[R],[[0,2,[te]]]]",
            "[4,[],[R],[[1,3,[va]]]]"
        };
        */

        double[][] expectedValues = new double[][]{
            {0.009727852297685321, 0.16196986703936098, 0.11821940082368845, 0.04343929992598407},
            {0.0513853529653213, 1.074278784023375, 0.7841122636458053, 0.2990103285744924},
            {0.28903519391401, 2.6748825147340485, 1.9524983677650556, 0.8443278159632931},
            {0, 0, 0, 0},
            {0, 0, 0, 0}
        };

        TitanKey vertexType = tx.makeKey("vertexType").dataType(String.class).make();
        TitanKey edgeType = tx.makeKey("edgeType").dataType(String.class).make();
        TitanKey weight = tx.makeKey("weight").dataType(String.class).make();
        TitanLabel edge = tx.makeLabel("edge").make();

        int numVertices = 5;
        TitanVertex[] nodes = new TitanVertex[numVertices];
        for (int i = 0; i < numVertices; i++) {
            nodes[i] = tx.addVertex();
        }
        nodes[0].addProperty(vertexType, LEFT_VERTEX_TYPE);
        nodes[1].addProperty(vertexType, LEFT_VERTEX_TYPE);
        nodes[2].addProperty(vertexType, RIGHT_VERTEX_TYPE);
        nodes[3].addProperty(vertexType, RIGHT_VERTEX_TYPE);
        nodes[4].addProperty(vertexType, RIGHT_VERTEX_TYPE);

        TitanEdge[] edges = new TitanEdge[8];
        edges[0] = nodes[0].addEdge(edge, nodes[2]);
        edges[0].setProperty(weight, "1.0");
        edges[0].setProperty(edgeType, EDGE_TYPE_TRAIN);
        edges[1] = nodes[0].addEdge(edge, nodes[3]);
        edges[1].setProperty(weight, "2.0");
        edges[1].setProperty(edgeType, EDGE_TYPE_TEST);
        edges[2] = nodes[1].addEdge(edge, nodes[2]);
        edges[2].setProperty(weight, "5.0");
        edges[2].setProperty(edgeType, EDGE_TYPE_TRAIN);
        edges[3] = nodes[1].addEdge(edge, nodes[4]);
        edges[3].setProperty(weight, "3.0");
        edges[3].setProperty(edgeType, EDGE_TYPE_VALIDATION);
        edges[4] = nodes[2].addEdge(edge, nodes[0]);
        edges[4].setProperty(weight, "1.0");
        edges[4].setProperty(edgeType, EDGE_TYPE_TRAIN);
        edges[5] = nodes[2].addEdge(edge, nodes[1]);
        edges[5].setProperty(weight, "5.0");
        edges[5].setProperty(edgeType, EDGE_TYPE_TRAIN);
        TitanEdge e6 = nodes[3].addEdge(edge, nodes[0]);
        e6.setProperty(weight, "2.0");
        e6.setProperty(edgeType, EDGE_TYPE_TEST);
        edges[7] = nodes[4].addEdge(edge, nodes[1]);
        edges[7].setProperty(weight, "3.0");
        edges[7].setProperty(edgeType, EDGE_TYPE_VALIDATION);

        tx.commit();


        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0]);
        Assert.assertNotNull(results);
        for (String resultLine : results) {
            LOG.info(" got: " + resultLine);
        }
        // verify results
        Map<Long, Double[]> vertexValues = parseVertexValues(results);
        assertNotNull(vertexValues);
        assertEquals(5, vertexValues.size());
        for (Map.Entry<Long, Double[]> entry : vertexValues.entrySet()) {
            Double[] vertexValue = entry.getValue();
            assertEquals(4, vertexValue.length);
            for (int j = 0; j < 3; j++) {
                assertEquals(expectedValues[(int) (entry.getKey() / TITAN_ID_OFFSET) - 1][j], vertexValue[j], 0.01d);
            }
        }
    }

    private Map<Long, Double[]> parseVertexValues(Iterable<String> results) {
        Map<Long, Double[]> vertexValues = Maps.newHashMapWithExpectedSize(Iterables.size(results));
        for (String line : results) {
            try {
                JSONArray jsonVertex = new JSONArray(line);
                if (jsonVertex.length() != 4) {
                    throw new IllegalArgumentException("Wrong vertex output format!");
                }
                // get vertex id
                long id = jsonVertex.getLong(0);
                // get vertex bias
                JSONArray biasArray = jsonVertex.getJSONArray(1);
                if (biasArray.length() != 1) {
                    throw new IllegalArgumentException("Wrong vertex bias value output value format!");
                }
                double bias = biasArray.getDouble(0);
                JSONArray valueArray = jsonVertex.getJSONArray(2);
                if (valueArray.length() != 3) {
                    throw new IllegalArgumentException("Wrong vertex vector output value format!");
                }
                Double[] values = new Double[4];
                values[0] = bias;
                for (int i = 0; i < 3; i++) {
                    values[i + 1] = valueArray.getDouble(i);
                }
                vertexValues.put(id, values);
                // get vertex type
                JSONArray typeArray = jsonVertex.getJSONArray(3);
                if (typeArray.length() != 1) {
                    throw new IllegalArgumentException("Wrong vertex type output value format!");
                }
            } catch (JSONException e) {
                throw new IllegalArgumentException("Couldn't get vertex from line " + line, e);
            }
        }
        return vertexValues;
    }
}
