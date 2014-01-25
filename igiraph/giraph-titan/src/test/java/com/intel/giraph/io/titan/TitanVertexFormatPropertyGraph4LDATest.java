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

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.EDGE_TYPE_PROPERTY_KEY;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_LABEL_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_VALUE_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.OUTPUT_VERTEX_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.VECTOR_VALUE;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.VERTEX_TYPE_PROPERTY_KEY;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.DOC_VERTEX;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.WORD_VERTEX;
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
                "[0,[],[d],[[6,2,[]],[8,1,[]]]]",
                "[1,[],[d],[[6,4,[]],[8,4,[]]]]",
                "[2,[],[d],[[7,3,[]]]]",
                "[3,[],[d],[[7,6,[]]]]",
                "[4,[],[d],[[9,1,[]],[10,3,[]]]]",
                "[5,[],[d],[[9,4,[]],[10,2,[]]]]",
                "[6,[],[w],[[0,2,[]],[1,4,[]]]]",
                "[7,[],[w],[[2,3,[]],[3,6,[]]]]",
                "[8,[],[w],[[0,1,[]],[1,4,[]]]]",
                "[9,[],[w],[[4,1,[]],[5,4,[]]]]",
                "[10,[],[w],[[4,3,[]],[5,2,[]]]]"
        };
        */

    private double[][] expectedValues = new double[][]{
        {0.34330578417595814, 0.03307608753257313, 0.6236181282914688},
        {0.23566890200157475, 0.012958157794674302, 0.7513729402037509},
        {0.8257527740877763, 0.1392818338203528, 0.034965392091870946},
        {0.9276499121921703, 0.05457343457808104, 0.01777665322974867},
        {0.026942351988061924, 0.7936753899650367, 0.17938225804690128},
        {0.017954584867493708, 0.9229842037339282, 0.05906121139857818},
        {0.17403206195690205, 0.010823843975648704, 0.4326990116083441},
        {0.7185121532451695, 0.06932005707999805, 0.013316968112006687},
        {0.0880306604961593, 0.010531015401042884, 0.43444275445561503},
        {0.009445300727080892, 0.48194149633720995, 0.031159478346300173},
        {0.009979824206784269, 0.42738358794031034, 0.0883817882566642}
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
        nodes[0].addProperty(vertexType, DOC_VERTEX);
        nodes[1].addProperty(vertexType, DOC_VERTEX);
        nodes[2].addProperty(vertexType, DOC_VERTEX);
        nodes[3].addProperty(vertexType, DOC_VERTEX);
        nodes[4].addProperty(vertexType, DOC_VERTEX);
        nodes[5].addProperty(vertexType, DOC_VERTEX);
        nodes[6].addProperty(vertexType, WORD_VERTEX);
        nodes[7].addProperty(vertexType, WORD_VERTEX);
        nodes[8].addProperty(vertexType, WORD_VERTEX);
        nodes[9].addProperty(vertexType, WORD_VERTEX);
        nodes[10].addProperty(vertexType, WORD_VERTEX);

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
        for (String resultLine : results) {
            System.out.println(" got: " + resultLine);
        }

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

            for (int j = 0; j < numKeys; j++) {
                assertEquals(expectedValues[i][j], Double.parseDouble(nodes[i].getProperty(resultKey[j])
                    .toString()), 0.01d);
            }
        }
    }

    @Test
    public void PropertyGraph4LDAVectorTest() throws Exception {
        VECTOR_VALUE.set(giraphConf, "true");
        OUTPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "lda_result");

        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0]);
        Assert.assertNotNull(results);
        for (String resultLine : results) {
            System.out.println(" got: " + resultLine);
        }

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
            for (int j = 0; j < numKeys; j++) {
                assertEquals(expectedValues[i][j], Double.parseDouble(valueString[j]), 0.01d);
            }
        }
    }
}
