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
package com.intel.giraph.io.titan;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.intel.giraph.algorithms.als.AlternatingLeastSquaresComputation;
import com.intel.giraph.algorithms.als.AlternatingLeastSquaresComputation.AlternatingLeastSquaresAggregatorWriter;
import com.intel.giraph.algorithms.als.AlternatingLeastSquaresComputation.AlternatingLeastSquaresMasterCompute;
import com.intel.giraph.algorithms.cgd.ConjugateGradientDescentComputation;
import com.intel.giraph.io.formats.JsonPropertyGraph4CFOutputFormat;
import com.intel.giraph.io.titan.hbase.TitanHBaseVertexInputFormatPropertyGraph4CF;
import com.intel.giraph.io.titan.hbase.TitanHBaseVertexInputFormatPropertyGraph4CFCGD;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanVertex;
import org.apache.giraph.utils.InternalVertexRunner;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.*;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;


/**
 * This class is for testing TitanHBaseVertexInputFormatPropertyGraph4CF
 * and TitanVertexOutputFormatPropertyGraph4CF
 * The test contains the following steps:
 * firstly load a graph to Titan/HBase,
 * then read out the graph via  TitanHBaseVertexInputFormatPropertyGraph4CF,
 * then run algorithm with input data,
 * finally write back results to Titan via TitanVertexOutputFormatPropertyGraph4CF
 */
public class TitanVertexFormatPropertyGraph4CFTest
    extends TitanTestBase{
            /*
        String[] graph = new String[] {
                "[0,[],[L],[[2,1,[tr]],[3,2,[te]]]]",
                "[1,[],[L],[[2,5,[tr]],[4,3,[va]]]]",
                "[2,[],[R],[[0,1,[tr]],[1,5,[tr]]]]",
                "[3,[],[R],[[0,2,[te]]]]",
                "[4,[],[R],[[1,3,[va]]]]"
        };
        */

    private double[][] expectedAlsValues = new double[][]{
        {0.22733103186672185, 0.16592728476946825, 0.06253175723477887},
        {1.136655159333612, 0.8296364238473429, 0.3126587861738814},
        {2.7235109462314817, 1.9878710470306467, 0.7491538832804335},
        {0, 0, 0},
        {0, 0, 0}
    };

    private double[][] expectedCgdValues = new double[][]{
        {0.009727852297685321, 0.16196986703936098, 0.11821940082368845, 0.04343929992598407},
        {0.0513853529653213, 1.074278784023375, 0.7841122636458053, 0.2990103285744924},
        {0.28903519391401, 2.6748825147340485, 1.9524983677650556, 0.8443278159632931},
        {0, 0, 0, 0},
        {0, 0, 0, 0}
    };

    private int numKeys = 3;
    private int numVertices = 5;
    private TitanVertex[] nodes = new TitanVertex[numVertices];


    @Override
    protected void configure() throws Exception {
        //load the graph to Titan
        TitanKey vertexType = tx.makeKey("vertexType").dataType(String.class).make();
        TitanKey edgeType = tx.makeKey("edgeType").dataType(String.class).make();
        TitanKey weight = tx.makeKey("weight").dataType(String.class).make();
        TitanLabel edge = tx.makeLabel("edge").make();

        for (int i = 0; i < numVertices; i++) {
            nodes[i] = tx.addVertex();
        }
        nodes[0].addProperty(vertexType, VERTEX_TYPE_LEFT);
        nodes[1].addProperty(vertexType, VERTEX_TYPE_LEFT);
        nodes[2].addProperty(vertexType, VERTEX_TYPE_RIGHT);
        nodes[3].addProperty(vertexType, VERTEX_TYPE_RIGHT);
        nodes[4].addProperty(vertexType, VERTEX_TYPE_RIGHT);

        TitanEdge[] edges = new TitanEdge[8];
        edges[0] = nodes[0].addEdge(edge, nodes[2]);
        edges[0].setProperty(weight, "1.0");
        edges[0].setProperty(edgeType, TYPE_TRAIN);
        edges[1] = nodes[0].addEdge(edge, nodes[3]);
        edges[1].setProperty(weight, "2.0");
        edges[1].setProperty(edgeType, TYPE_TEST);
        edges[2] = nodes[1].addEdge(edge, nodes[2]);
        edges[2].setProperty(weight, "5.0");
        edges[2].setProperty(edgeType, TYPE_TRAIN);
        edges[3] = nodes[1].addEdge(edge, nodes[4]);
        edges[3].setProperty(weight, "3.0");
        edges[3].setProperty(edgeType, TYPE_VALIDATE);
        edges[4] = nodes[2].addEdge(edge, nodes[0]);
        edges[4].setProperty(weight, "1.0");
        edges[4].setProperty(edgeType, TYPE_TRAIN);
        edges[5] = nodes[2].addEdge(edge, nodes[1]);
        edges[5].setProperty(weight, "5.0");
        edges[5].setProperty(edgeType, TYPE_TRAIN);
        edges[6] = nodes[3].addEdge(edge, nodes[0]);
        edges[6].setProperty(weight, "2.0");
        edges[6].setProperty(edgeType, TYPE_TEST);
        edges[7] = nodes[4].addEdge(edge, nodes[1]);
        edges[7].setProperty(weight, "3.0");
        edges[7].setProperty(edgeType, TYPE_VALIDATE);

        tx.commit();

    }

    @Test
    public void PropertyGraph4CFTest() throws Exception {
        giraphConf.setComputationClass(AlternatingLeastSquaresComputation.class);
        giraphConf.setMasterComputeClass(AlternatingLeastSquaresMasterCompute.class);
        giraphConf.setAggregatorWriterClass(AlternatingLeastSquaresAggregatorWriter.class);
        giraphConf.setVertexInputFormatClass(TitanHBaseVertexInputFormatPropertyGraph4CF.class);
        giraphConf.setVertexOutputFormatClass(TitanVertexOutputFormatPropertyGraph4CF.class);
        giraphConf.set("als.maxSupersteps", "6");
        giraphConf.set("als.featureDimension", "3");
        giraphConf.set("als.lambda", "0.05");
        giraphConf.set("als.convergenceThreshold", "0");

        INPUT_VERTEX_VALUE_PROPERTY_KEY_LIST.set(giraphConf, "default");
        INPUT_EDGE_VALUE_PROPERTY_KEY_LIST.set(giraphConf, "weight");
        INPUT_EDGE_LABEL_LIST.set(giraphConf, "edge");
        VERTEX_TYPE_PROPERTY_KEY.set(giraphConf, "vertexType");
        EDGE_TYPE_PROPERTY_KEY.set(giraphConf, "edgeType");
        VECTOR_VALUE.set(giraphConf, "false");
        OUTPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "als_p0,als_p1,als_p2");
        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0]);
        Assert.assertNotNull(results);

        //verify data is written to Titan
        startNewTransaction();
        long[] nid;
        TitanKey[] resultKey;
        String[] keyName;
        nid = new long[numVertices];
        resultKey = new TitanKey[numKeys];
        keyName = new String[numKeys];
        keyName[0] = "als_p0";
        keyName[1] = "als_p1";
        keyName[2] = "als_p2";
        //check keys are generated for Titan
        for (int i = 0; i < numKeys; i++) {
            assertTrue(tx.containsType(keyName[i]));
            resultKey[i] = tx.getPropertyKey(keyName[i]);
            assertEquals(resultKey[i].getName(), keyName[i]);
            assertEquals(resultKey[i].getDataType(), String.class);
        }

        for (int i = 0; i < numVertices; i++) {
            nid[i] = nodes[i].getID();
            assertTrue(tx.containsVertex(nid[i]));
            nodes[i] = tx.getVertex(nid[i]);

            for (int j = 0; j < numKeys; j++) {
                assertEquals(expectedAlsValues[i][j], Double.parseDouble(nodes[i].getProperty(resultKey[j]).
                    toString()), 0.01d);
            }
        }
    }

    @Test
    public void PropertyGraph4CFVectorTest() throws Exception {
        giraphConf.setComputationClass(AlternatingLeastSquaresComputation.class);
        giraphConf.setMasterComputeClass(AlternatingLeastSquaresMasterCompute.class);
        giraphConf.setAggregatorWriterClass(AlternatingLeastSquaresAggregatorWriter.class);
        giraphConf.setVertexInputFormatClass(TitanHBaseVertexInputFormatPropertyGraph4CF.class);
        giraphConf.setVertexOutputFormatClass(TitanVertexOutputFormatPropertyGraph4CF.class);
        giraphConf.set("als.maxSupersteps", "6");
        giraphConf.set("als.featureDimension", "3");
        giraphConf.set("als.lambda", "0.05");
        giraphConf.set("als.convergenceThreshold", "0");

        INPUT_VERTEX_VALUE_PROPERTY_KEY_LIST.set(giraphConf, "default");
        INPUT_EDGE_VALUE_PROPERTY_KEY_LIST.set(giraphConf, "weight");
        INPUT_EDGE_LABEL_LIST.set(giraphConf, "edge");
        VERTEX_TYPE_PROPERTY_KEY.set(giraphConf, "vertexType");
        EDGE_TYPE_PROPERTY_KEY.set(giraphConf, "edgeType");
        VECTOR_VALUE.set(giraphConf, "true");
        OUTPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "als_result");
        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0]);
        Assert.assertNotNull(results);

        //verify data is written to Titan
        startNewTransaction();
        String keyName = "als_result";

        //check keys are generated for Titan
        assertTrue(tx.containsType(keyName));
        TitanKey resultKey = tx.getPropertyKey(keyName);
        assertEquals(resultKey.getName(), keyName);
        assertEquals(resultKey.getDataType(), String.class);

        for (int i = 0; i < numVertices; i++) {
            long nid = nodes[i].getID();
            assertTrue(tx.containsVertex(nid));
            nodes[i] = tx.getVertex(nid);

            //split by comma
            String alsResult = nodes[i].getProperty(resultKey).toString();
            String[] valueString = alsResult.split(",");
            for (int j = 0; j < numKeys; j++) {
                assertEquals(expectedAlsValues[i][j], Double.parseDouble(valueString[j]), 0.01d);
            }
        }
    }

    //@Ignore
    @Test
    public void PropertyGraph4CFCGDTest() throws Exception {
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

        INPUT_EDGE_VALUE_PROPERTY_KEY_LIST.set(giraphConf, "weight");
        INPUT_EDGE_LABEL_LIST.set(giraphConf, "edge");
        VERTEX_TYPE_PROPERTY_KEY.set(giraphConf, "vertexType");
        EDGE_TYPE_PROPERTY_KEY.set(giraphConf, "edgeType");
        VECTOR_VALUE.set(giraphConf, "false");

        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0]);
        Assert.assertNotNull(results);
        for (String resultLine : results) {
            LOG.info(" got: " + resultLine);
        }
        // verify results
        Map<Long, Double[]> vertexValues = parseVertexValues(results);
        Assert.assertNotNull(vertexValues);
        assertEquals(numVertices, vertexValues.size());
        for (Map.Entry<Long, Double[]> entry : vertexValues.entrySet()) {
            Double[] vertexValue = entry.getValue();
            assertEquals(4, vertexValue.length);
            for (int j = 0; j < numKeys; j++) {
                assertEquals(expectedCgdValues[(int) (entry.getKey() / TITAN_ID_OFFSET) - 1][j], vertexValue[j], 0.01d);
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
                for (int i = 0; i < numKeys; i++) {
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
