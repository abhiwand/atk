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

import com.intel.giraph.algorithms.lda.CVB0LDAComputation;
import com.intel.giraph.algorithms.lda.CVB0LDAComputation.CVB0LDAAggregatorWriter;
import com.intel.giraph.algorithms.lda.CVB0LDAComputation.CVB0LDAMasterCompute;
import com.intel.giraph.io.VertexData4LDAWritable;
import com.intel.giraph.io.titan.hbase.TitanHBaseVertexInputFormatPropertyGraph4LDA;
import com.intel.mahout.math.DoubleWithVectorWritable;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanVertex;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.io.LongWritable;
import org.junit.Assert;
import org.junit.Test;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.*;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 * This class is for testing TitanHBaseVertexInputFormatPropertyGraph4LDA
 * and TitanVertexOutputFormatPropertyGraph4LDA
 * The test contains the following steps:
 * firstly load a graph to Titan/HBase,
 * then read out the graph via  TitanHBaseVertexInputFormatPropertyGraph4LDA,
 * then run algorithm with input data,
 * finally write back results to Titan via TitanVertexOutputFormatPropertyGraph4LDA
 */
public class TitanVertexFormatPropertyGraph4LDATest 
    extends TitanTestBase<LongWritable, VertexData4LDAWritable, DoubleWithVectorWritable> {
        /*
        String[] graph = new String[] {
                "[0,[],[L],[[6,2,[]],[8,1,[]]]]",
                "[1,[],[L],[[6,4,[]],[8,4,[]]]]",
                "[2,[],[L],[[7,3,[]]]]",
                "[3,[],[L],[[7,6,[]]]]",
                "[4,[],[L],[[9,1,[]],[10,3,[]]]]",
                "[5,[],[L],[[9,4,[]],[10,2,[]]]]",
                "[6,[],[R],[[0,2,[]],[1,4,[]]]]",
                "[7,[],[R],[[2,3,[]],[3,6,[]]]]",
                "[8,[],[R],[[0,1,[]],[1,4,[]]]]",
                "[9,[],[R],[[4,1,[]],[5,4,[]]]]",
                "[10,[],[R],[[4,3,[]],[5,2,[]]]]"
        };
        */

    private double[][] expectedValues = new double[][]{
        {0.15863483822017285, 0.031873080137769697, 0.8094920816420574},
        {0.09239858901071828, 0.012522565962482012, 0.8950788450267997},
        {0.924679657940628, 0.04403787436202393, 0.03128246769734804},
        {0.962300077861499, 0.021424254061240638, 0.016275668077260483},
        {0.024203941505452374, 0.9221690887384729, 0.053626969756074803},
        {0.016373920784401155, 0.9607148775164212, 0.02291120169917766},
        {0.07160457232542619, 0.010366361339420923, 0.5594055353975846},
        {0.7647283135683827, 0.01776726509649644, 0.011057738285362},
        {0.037871265297471875, 0.010241008617535364, 0.49656174068786074},
        {0.008731744582534676, 0.4990641425734286, 0.013873214396170841},
        {0.008849556294726512, 0.4881329883043053, 0.025325000431417853}
    };

    private int numKeys = 3;
    private int numVertices = 11;
    private TitanVertex[] nodes = new TitanVertex[numVertices];


    @Override
    protected void configure() throws Exception {
        giraphConf.setComputationClass(CVB0LDAComputation.class);
        giraphConf.setMasterComputeClass(CVB0LDAMasterCompute.class);
        giraphConf.setAggregatorWriterClass(CVB0LDAAggregatorWriter.class);
        giraphConf.setVertexInputFormatClass(TitanHBaseVertexInputFormatPropertyGraph4LDA.class);
        giraphConf.setVertexOutputFormatClass(TitanVertexOutputFormatPropertyGraph4LDA.class);
        giraphConf.set("lda.maxSupersteps", "5");
        giraphConf.set("lda.numTopics", "3");
        giraphConf.set("lda.alpha", "0.1");
        giraphConf.set("lda.beta", "0.1");
        giraphConf.set("lda.convergenceThreshold", "0");
        giraphConf.set("lda.evaluateCost", "true");

        INPUT_EDGE_VALUE_PROPERTY_KEY_LIST.set(giraphConf, "frequency");
        INPUT_EDGE_LABEL_LIST.set(giraphConf, "edge");
        VERTEX_TYPE_PROPERTY_KEY.set(giraphConf, "vertexType");
        EDGE_TYPE_PROPERTY_KEY.set(giraphConf, "edgeType");


        TitanKey vertexType = tx.makeKey("vertexType").dataType(String.class).make();
        TitanKey frequency = tx.makeKey("frequency").dataType(String.class).make();
        TitanLabel edge = tx.makeLabel("edge").make();

        for (int i = 0; i < numVertices; i++) {
            nodes[i] = tx.addVertex();
        }
        nodes[0].addProperty(vertexType, VERTEX_TYPE_LEFT);
        nodes[1].addProperty(vertexType, VERTEX_TYPE_LEFT);
        nodes[2].addProperty(vertexType, VERTEX_TYPE_LEFT);
        nodes[3].addProperty(vertexType, VERTEX_TYPE_LEFT);
        nodes[4].addProperty(vertexType, VERTEX_TYPE_LEFT);
        nodes[5].addProperty(vertexType, VERTEX_TYPE_LEFT);
        nodes[6].addProperty(vertexType, VERTEX_TYPE_RIGHT);
        nodes[7].addProperty(vertexType, VERTEX_TYPE_RIGHT);
        nodes[8].addProperty(vertexType, VERTEX_TYPE_RIGHT);
        nodes[9].addProperty(vertexType, VERTEX_TYPE_RIGHT);
        nodes[10].addProperty(vertexType, VERTEX_TYPE_RIGHT);

        TitanEdge[] edges = new TitanEdge[20];
        edges[0] = nodes[0].addEdge(edge, nodes[6]);
        edges[0].setProperty(frequency, "2");
        edges[1] = nodes[0].addEdge(edge, nodes[8]);
        edges[1].setProperty(frequency, "1");
        edges[2] = nodes[1].addEdge(edge, nodes[6]);
        edges[2].setProperty(frequency, "4");
        edges[3] = nodes[1].addEdge(edge, nodes[8]);
        edges[3].setProperty(frequency, "4");
        edges[4] = nodes[2].addEdge(edge, nodes[7]);
        edges[4].setProperty(frequency, "3");
        edges[5] = nodes[3].addEdge(edge, nodes[7]);
        edges[5].setProperty(frequency, "6");
        edges[6] = nodes[4].addEdge(edge, nodes[9]);
        edges[6].setProperty(frequency, "1");
        edges[7] = nodes[4].addEdge(edge, nodes[10]);
        edges[7].setProperty(frequency, "3");
        edges[8] = nodes[5].addEdge(edge, nodes[9]);
        edges[8].setProperty(frequency, "4");
        edges[9] = nodes[5].addEdge(edge, nodes[10]);
        edges[9].setProperty(frequency, "2");
        edges[10] = nodes[6].addEdge(edge, nodes[0]);
        edges[10].setProperty(frequency, "2");
        edges[11] = nodes[6].addEdge(edge, nodes[1]);
        edges[11].setProperty(frequency, "4");
        edges[12] = nodes[7].addEdge(edge, nodes[2]);
        edges[12].setProperty(frequency, "3");
        edges[13] = nodes[7].addEdge(edge, nodes[3]);
        edges[13].setProperty(frequency, "6");
        edges[14] = nodes[8].addEdge(edge, nodes[0]);
        edges[14].setProperty(frequency, "1");
        edges[15] = nodes[8].addEdge(edge, nodes[1]);
        edges[15].setProperty(frequency, "4");
        edges[16] = nodes[9].addEdge(edge, nodes[4]);
        edges[16].setProperty(frequency, "1");
        edges[17] = nodes[9].addEdge(edge, nodes[5]);
        edges[17].setProperty(frequency, "4");
        edges[18] = nodes[10].addEdge(edge, nodes[4]);
        edges[18].setProperty(frequency, "3");
        edges[19] = nodes[10].addEdge(edge, nodes[5]);
        edges[19].setProperty(frequency, "2");

        tx.commit();
    }

    @Test
    public void PropertyGraph4LDATest() throws Exception {
        VECTOR_VALUE.set(giraphConf, "false");
        OUTPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "lda_p0,lda_p1,lda_p2");

        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0]);
        Assert.assertNotNull(results);

        //verify data is written to Titan
        startNewTransaction();
        TitanKey[] resultKey;
        String[] keyName;
        resultKey = new TitanKey[numKeys];
        keyName = new String[numKeys];
        keyName[0] = "lda_p0";
        keyName[1] = "lda_p1";
        keyName[2] = "lda_p2";
        //check keys are generated for Titan
        for (int i = 0; i < numKeys; i++) {
            assertTrue(tx.containsType(keyName[i]));
            resultKey[i] = tx.getPropertyKey(keyName[i]);
            assertEquals(resultKey[i].getName(), keyName[i]);
            assertEquals(resultKey[i].getDataType(), String.class);
        }

        for (int i = 0; i < numVertices; i++) {
            long nid = nodes[i].getID();
            assertTrue(tx.containsVertex(nid));
            nodes[i] = tx.getVertex(nid);

            LOG.info(" LDA i " + i + ", got {" +
                + Double.parseDouble(nodes[i].getProperty(resultKey[0]).toString()) + ", "
                + Double.parseDouble(nodes[i].getProperty(resultKey[1]).toString()) + ", "
                + Double.parseDouble(nodes[i].getProperty(resultKey[2]).toString()) + "}");

            for (int j = 0; j < numKeys; j++) {
            //   assertEquals(expectedValues[i][j], Double.parseDouble(nodes[i].getProperty(resultKey[j])
             //       .toString()), 0.01d);
            }
        }
    }

    @Test
    public void PropertyGraph4LDAVectorTest() throws Exception {
        VECTOR_VALUE.set(giraphConf, "true");
        OUTPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "lda_result");

        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0]);
        Assert.assertNotNull(results);

        //verify data is written to Titan
        startNewTransaction();
        long[] nid = new long[numVertices];
        String keyName = "lda_result";

        //check keys are generated for Titan
        assertTrue(tx.containsType(keyName));
        TitanKey resultKey = tx.getPropertyKey(keyName);
        assertEquals(resultKey.getName(), keyName);
        assertEquals(resultKey.getDataType(), String.class);


        for (int i = 0; i < numVertices; i++) {
            nid[i] = nodes[i].getID();
            assertTrue(tx.containsVertex(nid[i]));
            nodes[i] = tx.getVertex(nid[i]);

            //split by comma
            String ldaResult = nodes[i].getProperty(resultKey).toString();
            String[] valueString = ldaResult.split(",");
            LOG.info(" LDA i " + i + ", got {" +
                + Double.parseDouble(valueString[0]) + ", "
                + Double.parseDouble(valueString[1]) + ", "
                + Double.parseDouble(valueString[2]) + "}");

            for (int j = 0; j < numKeys; j++) {
              //  assertEquals(expectedValues[i][j], Double.parseDouble(valueString[j]), 0.01d);
            }
        }
    }
}
