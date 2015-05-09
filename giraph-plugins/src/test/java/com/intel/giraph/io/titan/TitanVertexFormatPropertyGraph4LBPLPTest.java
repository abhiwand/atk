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

package com.intel.giraph.io.titan;

import com.intel.giraph.algorithms.lbp.LoopyBeliefPropagationComputation;
import com.intel.giraph.algorithms.lp.LabelPropagationComputation;
import com.intel.giraph.io.titan.formats.TitanVertexInputFormatPropertyGraph4LBP;
import com.intel.giraph.io.titan.formats.TitanVertexInputFormatPropertyGraph4LP;
import com.intel.giraph.io.titan.formats.TitanVertexOutputFormatPropertyGraph4LBP;
import com.intel.giraph.io.titan.formats.TitanVertexOutputFormatPropertyGraph4LP;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This class is for testing following formats:
 * TitanHBaseVertexInputFormatPropertyGraph4LBP,
 * TitanVertexOutputFormatPropertyGraph4LBP,
 * TitanHBaseVertexInputFormatPropertyGraph4LP,
 * TitanVertexOutputFormatPropertyGraph4LP
 * <p/>
 * The test contains the following steps:
 * firstly load a graph to Titan/HBase,
 * then read out the graph from TitanHBaseVertexInputFormat,
 * then run algorithm with input data,
 * finally write back results to Titan.
 */
public class TitanVertexFormatPropertyGraph4LBPLPTest
        extends TitanTestBase {
    private int numKeys = 3;
    private int numVertices = 5;
    private TitanVertex[] nodes = new TitanVertex[numVertices];
    /*test graph
    String[] graph = new String[] {
        "[0,[1,0.1,0.1],[],[[1,1,[]],[3,3,[]]]]",
        "[1,[0.2,2,2],[\"TR\"],[[0,1,[]],[2,2,[]],[3,1,[]]]]",
        "[2,[0.3,0.3,3],[\"TR\"],[[1,2,[]],[4,4,[]]]]",
        "[3,[0.4,4,0.4],[\"TE\"],[[0,3,[]],[1,1,[]],[4,4,[]]]]",
        "[4,[5,5,0.5],[\"VA\"],[[3,4,[]],[2,4,[]]]]"
    };
    */
    private HashMap<Integer, Double[]> expectedLbpValues = new HashMap<Integer, Double[]>();
    private HashMap<Integer, Double[]> expectedLpValues = new HashMap<Integer, Double[]>();


    @Override
    protected void configure() throws Exception {
        expectedLbpValues.put(0, new Double[]{0.562, 0.088, 0.350});
        expectedLbpValues.put(1, new Double[]{0.042, 0.102, 0.855});
        expectedLbpValues.put(2, new Double[]{0.038, 0.087, 0.874});
        expectedLbpValues.put(3, new Double[]{0.228, 0.048, 0.724});
        expectedLbpValues.put(4, new Double[]{0.039, 0.088, 0.874});

        expectedLpValues.put(0, new Double[]{0.833, 0.083, 0.083});
        expectedLpValues.put(1, new Double[]{0.271, 0.271, 0.458});
        expectedLpValues.put(2, new Double[]{0.083, 0.083, 0.833});
        expectedLpValues.put(3, new Double[]{0.083, 0.833, 0.083});
        expectedLpValues.put(4, new Double[]{0.083, 0.458, 0.458});

        TitanManagement graphManager = graph.getManagementSystem();
        graphManager.makePropertyKey("red").dataType(String.class).make();
        graphManager.makePropertyKey("blue").dataType(String.class).make();
        graphManager.makePropertyKey("yellow").dataType(String.class).make();
        graphManager.makePropertyKey("weight").dataType(String.class).make();
        graphManager.makePropertyKey("vertexType").dataType(String.class).make();
        graphManager.makePropertyKey("prior").dataType(String.class).make();
        graphManager.makeEdgeLabel("friend").make();
        graphManager.commit();

        TitanTransaction tx = graph.newTransaction();
        for (int i = 0; i < numVertices; i++) {
            nodes[i] = tx.addVertex();
        }

        String red = "red";
        String blue = "blue";
        String yellow = "yellow";
        String weight = "weight";
        String vertexType = "vertexType";
        String prior = "prior";
        String friend = "friend";

        nodes[0].addProperty(red, "1");
        nodes[0].addProperty(blue, "0.1");
        nodes[0].addProperty(yellow, "0.1");
        nodes[0].addProperty(vertexType, TYPE_TRAIN);
        nodes[0].addProperty(prior, "1 0.1 0.1");
        nodes[1].addProperty(red, "0.2");
        nodes[1].addProperty(blue, "2");
        nodes[1].addProperty(yellow, "2");
        nodes[1].addProperty(vertexType, TYPE_TRAIN);
        nodes[1].addProperty(prior, "0.2, 2, 2");
        nodes[2].addProperty(red, "0.3");
        nodes[2].addProperty(blue, "0.3");
        nodes[2].addProperty(yellow, "3");
        nodes[2].addProperty(vertexType, TYPE_TRAIN);
        nodes[2].addProperty(prior, "0.3, 0.3, 3");
        nodes[3].addProperty(red, "0.4");
        nodes[3].addProperty(blue, "4");
        nodes[3].addProperty(yellow, "0.4");
        nodes[3].addProperty(vertexType, TYPE_TEST);
        nodes[3].addProperty(prior, "0.4, 4, 0.4");
        nodes[4].addProperty(red, "5");
        nodes[4].addProperty(blue, "5");
        nodes[4].addProperty(yellow, "0.5");
        nodes[4].addProperty(vertexType, TYPE_VALIDATE);
        nodes[4].addProperty(prior, "5, 5, 0.5");

        TitanEdge[] edges = new TitanEdge[12];
        edges[0] = nodes[0].addEdge(friend, nodes[1]);
        edges[0].setProperty(weight, "1.0");
        edges[1] = nodes[0].addEdge(friend, nodes[3]);
        edges[1].setProperty(weight, "3.0");
        edges[2] = nodes[1].addEdge(friend, nodes[0]);
        edges[2].setProperty(weight, "1.0");
        edges[3] = nodes[1].addEdge(friend, nodes[2]);
        edges[3].setProperty(weight, "2.0");
        edges[4] = nodes[1].addEdge(friend, nodes[3]);
        edges[4].setProperty(weight, "1.0");
        edges[5] = nodes[2].addEdge(friend, nodes[1]);
        edges[5].setProperty(weight, "2.0");
        edges[6] = nodes[2].addEdge(friend, nodes[4]);
        edges[6].setProperty(weight, "4.0");
        edges[7] = nodes[3].addEdge(friend, nodes[0]);
        edges[7].setProperty(weight, "3.0");
        edges[8] = nodes[3].addEdge(friend, nodes[1]);
        edges[8].setProperty(weight, "1.0");
        edges[9] = nodes[3].addEdge(friend, nodes[4]);
        edges[9].setProperty(weight, "4.0");
        edges[10] = nodes[4].addEdge(friend, nodes[3]);
        edges[10].setProperty(weight, "4.0");
        edges[11] = nodes[4].addEdge(friend, nodes[2]);
        edges[11].setProperty(weight, "4.0");

        tx.commit();
    }

    //@Ignore
    @Test
    public void PropertyGraph4LBPTest() throws Exception {
        giraphConf.setComputationClass(LoopyBeliefPropagationComputation.class);
        giraphConf.setMasterComputeClass(LoopyBeliefPropagationComputation.
                LoopyBeliefPropagationMasterCompute.class);
        giraphConf.setAggregatorWriterClass(LoopyBeliefPropagationComputation.
                LoopyBeliefPropagationAggregatorWriter.class);
        giraphConf.setVertexInputFormatClass(TitanVertexInputFormatPropertyGraph4LBP.class);
        giraphConf.setVertexOutputFormatClass(TitanVertexOutputFormatPropertyGraph4LBP.class);
        giraphConf.set("lbp.maxSupersteps", "5");

        INPUT_VERTEX_VALUE_PROPERTY_KEY_LIST.set(giraphConf, "red,blue,yellow");
        INPUT_EDGE_VALUE_PROPERTY_KEY_LIST.set(giraphConf, "weight");
        INPUT_EDGE_LABEL_LIST.set(giraphConf, "friend");
        VERTEX_TYPE_PROPERTY_KEY.set(giraphConf, "vertexType");
        VECTOR_VALUE.set(giraphConf, "false");
        OUTPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "lbp_red,lbp_blue,lbp_yellow");

        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0]);
        Assert.assertNotNull(results);

        //verify data is written to Titan
        tx = graph.newTransaction();
        long[] nid = new long[numVertices];
        assertTrue(tx.containsRelationType("lbp_red"));
        assertTrue(tx.containsRelationType("lbp_blue"));
        assertTrue(tx.containsRelationType("lbp_yellow"));
        PropertyKey result_blue = tx.getPropertyKey("lbp_blue");
        assertEquals(result_blue.getDataType(), String.class);
        assertEquals(result_blue.getName(), "lbp_blue");
        for (int i = 0; i < numVertices; i++) {
            nid[i] = nodes[i].getLongId();
            assertTrue(tx.containsVertex(nid[i]));
            nodes[i] = tx.getVertex(nid[i]);
            assertEquals(expectedLbpValues.get(i)[1],
                    Double.parseDouble(nodes[i].getProperty(result_blue).toString()), 0.01d);
        }
        tx.commit();
    }

    //@Ignore
    @Test
    public void PropertyGraph4LBPVectorTest() throws Exception {
        giraphConf.setComputationClass(LoopyBeliefPropagationComputation.class);
        giraphConf.setMasterComputeClass(LoopyBeliefPropagationComputation.
                LoopyBeliefPropagationMasterCompute.class);
        giraphConf.setAggregatorWriterClass(LoopyBeliefPropagationComputation.
                LoopyBeliefPropagationAggregatorWriter.class);
        giraphConf.setVertexInputFormatClass(TitanVertexInputFormatPropertyGraph4LBP.class);
        giraphConf.setVertexOutputFormatClass(TitanVertexOutputFormatPropertyGraph4LBP.class);
        giraphConf.set("lbp.maxSupersteps", "5");

        INPUT_VERTEX_VALUE_PROPERTY_KEY_LIST.set(giraphConf, "prior");
        INPUT_EDGE_VALUE_PROPERTY_KEY_LIST.set(giraphConf, "weight");
        INPUT_EDGE_LABEL_LIST.set(giraphConf, "friend");
        VERTEX_TYPE_PROPERTY_KEY.set(giraphConf, "vertexType");
        VECTOR_VALUE.set(giraphConf, "true");
        OUTPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "lbp_results");

        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0]);
        Assert.assertNotNull(results);

        tx = graph.newTransaction();
        //check keys are generated for Titan
        String keyName = "lbp_results";
        assertTrue(tx.containsRelationType(keyName));
        PropertyKey resultKey = tx.getPropertyKey(keyName);
        assertEquals(resultKey.getName(), keyName);
        assertEquals(resultKey.getDataType(), String.class);

        //verify data is written to Titan
        for (int i = 0; i < numVertices; i++) {
            long nid = nodes[i].getLongId();
            assertTrue(tx.containsVertex(nid));
            nodes[i] = tx.getVertex(nid);

            //split by comma
            String lbpResult = nodes[i].getProperty(resultKey).toString();
            String[] valueString = lbpResult.split(",");
            for (int j = 0; j < numKeys; j++) {
                assertEquals(expectedLbpValues.get(i)[j], Double.parseDouble(valueString[j]), 0.01d);
            }
        }
        tx.commit();
    }

    @Test
    public void PropertyGraph4LPTest() throws Exception {
        giraphConf.setComputationClass(LabelPropagationComputation.class);
        giraphConf.setMasterComputeClass(LabelPropagationComputation.
                LabelPropagationMasterCompute.class);
        giraphConf.setAggregatorWriterClass(LabelPropagationComputation.
                LabelPropagationAggregatorWriter.class);
        giraphConf.setVertexInputFormatClass(TitanVertexInputFormatPropertyGraph4LP.class);
        giraphConf.setVertexOutputFormatClass(TitanVertexOutputFormatPropertyGraph4LP.class);
        giraphConf.set("lp.maxSupersteps", "5");
        giraphConf.set("lp.convergenceThreshold", "0.1");
        giraphConf.set("lp.anchorThreshold", "0.8");
        giraphConf.set("lp.lambda", "0");
        giraphConf.set("lp.bidirectionalCheck", "true");

        INPUT_VERTEX_VALUE_PROPERTY_KEY_LIST.set(giraphConf, "red,blue,yellow");
        INPUT_EDGE_VALUE_PROPERTY_KEY_LIST.set(giraphConf, "weight");
        INPUT_EDGE_LABEL_LIST.set(giraphConf, "friend");
        VECTOR_VALUE.set(giraphConf, "false");
        OUTPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "lp_red,lp_blue,lp_yellow");

        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0]);
        Assert.assertNotNull(results);

        //verify data is written to Titan
        tx = graph.newTransaction();
        assertTrue(tx.containsRelationType("lp_red"));
        assertTrue(tx.containsRelationType("lp_blue"));
        assertTrue(tx.containsRelationType("lp_yellow"));
        PropertyKey result_blue = tx.getPropertyKey("lp_blue");
        assertEquals(result_blue.getDataType(), String.class);
        assertEquals(result_blue.getName(), "lp_blue");
        for (int i = 0; i < numVertices; i++) {
            long nid = nodes[i].getLongId();
            assertTrue(tx.containsVertex(nid));
            nodes[i] = tx.getVertex(nid);
            assertEquals(expectedLpValues.get(i)[1],
                    Double.parseDouble(nodes[i].getProperty(result_blue).toString()), 0.01d);
        }
        tx.commit();
    }

    @Test
    public void PropertyGraph4LPVectorTest() throws Exception {
        giraphConf.setComputationClass(LabelPropagationComputation.class);
        giraphConf.setMasterComputeClass(LabelPropagationComputation.
                LabelPropagationMasterCompute.class);
        giraphConf.setAggregatorWriterClass(LabelPropagationComputation.
                LabelPropagationAggregatorWriter.class);
        giraphConf.setVertexInputFormatClass(TitanVertexInputFormatPropertyGraph4LP.class);
        giraphConf.setVertexOutputFormatClass(TitanVertexOutputFormatPropertyGraph4LP.class);
        giraphConf.set("lp.maxSupersteps", "5");
        giraphConf.set("lp.convergenceThreshold", "0.1");
        giraphConf.set("lp.anchorThreshold", "0.8");
        giraphConf.set("lp.lambda", "0");
        giraphConf.set("lp.bidirectionalCheck", "true");

        INPUT_VERTEX_VALUE_PROPERTY_KEY_LIST.set(giraphConf, "prior");
        INPUT_EDGE_VALUE_PROPERTY_KEY_LIST.set(giraphConf, "weight");
        INPUT_EDGE_LABEL_LIST.set(giraphConf, "friend");
        VECTOR_VALUE.set(giraphConf, "true");
        OUTPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "lp_results");

        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0]);
        Assert.assertNotNull(results);

        //verify data is written to Titan
        tx = graph.newTransaction();
        //check keys are generated for Titan
        String keyName = "lp_results";
        assertTrue(tx.containsRelationType(keyName));
        PropertyKey resultKey = tx.getPropertyKey(keyName);
        assertEquals(resultKey.getName(), keyName);
        assertEquals(resultKey.getDataType(), String.class);

        //verify data is written to Titan
        for (int i = 0; i < numVertices; i++) {
            long nid = nodes[i].getLongId();
            assertTrue(tx.containsVertex(nid));
            nodes[i] = tx.getVertex(nid);

            //split by comma
            String lpResult = nodes[i].getProperty(resultKey).toString();
            String[] valueString = lpResult.split(",");
            for (int j = 0; j < numKeys; j++) {
                assertEquals(expectedLpValues.get(i)[j], Double.parseDouble(valueString[j]), 0.01d);
            }
        }
        tx.commit();
    }
}