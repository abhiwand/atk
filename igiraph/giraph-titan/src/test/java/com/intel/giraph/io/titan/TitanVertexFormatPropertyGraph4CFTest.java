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

import com.intel.giraph.algorithms.als.AlternatingLeastSquaresComputation;
import com.intel.giraph.algorithms.als.AlternatingLeastSquaresComputation.AlternatingLeastSquaresAggregatorWriter;
import com.intel.giraph.algorithms.als.AlternatingLeastSquaresComputation.AlternatingLeastSquaresMasterCompute;
import com.intel.giraph.io.EdgeDataWritable;
import com.intel.giraph.io.VertexDataWritable;
import com.intel.giraph.io.titan.hbase.TitanHBaseVertexInputFormatPropertyGraph4CF;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanVertex;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.io.LongWritable;
import org.junit.Assert;
import org.junit.Test;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_LABEL_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_VERTEX_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.OUTPUT_VERTEX_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.VERTEX_TYPE_PROPERTY_KEY;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.EDGE_TYPE_PROPERTY_KEY;
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
    extends TitanTestBase<LongWritable, VertexDataWritable, EdgeDataWritable> {

    @Override
    protected void configure() throws Exception {
        giraphConf.setComputationClass(AlternatingLeastSquaresComputation.class);
        giraphConf.setMasterComputeClass(AlternatingLeastSquaresMasterCompute.class);
        giraphConf.setAggregatorWriterClass(AlternatingLeastSquaresAggregatorWriter.class);
        giraphConf.setVertexInputFormatClass(TitanHBaseVertexInputFormatPropertyGraph4CF.class);
        giraphConf.setVertexOutputFormatClass(TitanVertexOutputFormatPropertyGraph4CF.class);
        giraphConf.set("als.maxSupersteps", "6");
        giraphConf.set("als.featureDimension", "3");
        giraphConf.set("als.lambda", "0.05");
        giraphConf.set("als.convergenceThreshold", "0");

        INPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "default");
        INPUT_EDGE_PROPERTY_KEY_LIST.set(giraphConf, "weight");
        INPUT_EDGE_LABEL_LIST.set(giraphConf, "edge");
        VERTEX_TYPE_PROPERTY_KEY.set(giraphConf, "vertexType");
        EDGE_TYPE_PROPERTY_KEY.set(giraphConf, "edgeType");
        OUTPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "result_p0,result_p1,result_p2");
    }

    @Test
    public void VertexOutputFormatPropertyGraph4CFTest() throws Exception {
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
            {0.22733103186672185, 0.16592728476946825, 0.06253175723477887},
            {1.136655159333612, 0.8296364238473429, 0.3126587861738814},
            {2.7235109462314817, 1.9878710470306467, 0.7491538832804335},
            {0, 0, 0},
            {0, 0, 0}
        };

        TitanKey vertexType = tx.makeKey("vertexType").dataType(String.class).make();
        TitanKey edgeType = tx.makeKey("edgeType").dataType(String.class).make();
        TitanKey weight = tx.makeKey("weight").dataType(String.class).make();
        TitanLabel edge = tx.makeLabel("edge").make();

        TitanVertex[] nodes;
        nodes = new TitanVertex[5];
        nodes[0] = tx.addVertex();
        nodes[0].addProperty(vertexType, "L");
        nodes[1] = tx.addVertex();
        nodes[1].addProperty(vertexType, "L");
        nodes[2] = tx.addVertex();
        nodes[2].addProperty(vertexType, "R");
        nodes[3] = tx.addVertex();
        nodes[3].addProperty(vertexType, "R");
        nodes[4] = tx.addVertex();
        nodes[4].addProperty(vertexType, "R");

        TitanEdge[] edges;
        edges = new TitanEdge[8];
        edges[0] = nodes[0].addEdge(edge, nodes[2]);
        edges[0].setProperty(weight, "1.0");
        edges[0].setProperty(edgeType, "tr");
        edges[1] = nodes[0].addEdge(edge, nodes[3]);
        edges[1].setProperty(weight, "2.0");
        edges[1].setProperty(edgeType, "te");
        edges[2] = nodes[1].addEdge(edge, nodes[2]);
        edges[2].setProperty(weight, "5.0");
        edges[2].setProperty(edgeType, "tr");
        edges[3] = nodes[1].addEdge(edge, nodes[4]);
        edges[3].setProperty(weight, "3.0");
        edges[3].setProperty(edgeType, "va");
        edges[4] = nodes[2].addEdge(edge, nodes[0]);
        edges[4].setProperty(weight, "1.0");
        edges[4].setProperty(edgeType, "tr");
        edges[5] = nodes[2].addEdge(edge, nodes[1]);
        edges[5].setProperty(weight, "5.0");
        edges[5].setProperty(edgeType, "tr");
        edges[6] = nodes[3].addEdge(edge, nodes[0]);
        edges[6].setProperty(weight, "2.0");
        edges[6].setProperty(edgeType, "te");
        edges[7] = nodes[4].addEdge(edge, nodes[1]);
        edges[7].setProperty(weight, "3.0");
        edges[7].setProperty(edgeType, "va");

        tx.commit();


        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0]);
        Assert.assertNotNull(results);
        for (String resultLine : results) {
            LOG.info(" got: " + resultLine);
        }

        //verify data is written to Titan
        startNewTransaction();
        long[] nid;
        TitanKey[] resultKey;
        String[] keyName;
        nid = new long[5];
        resultKey = new TitanKey[3];
        keyName = new String[3];
        keyName[0] = "result_p0";
        keyName[1] = "result_p1";
        keyName[2] = "result_p2";
        //check keys are generated for Titan
        for (int i = 0; i < 3; i++) {
            assertTrue(tx.containsType(keyName[i]));
            resultKey[i] = tx.getPropertyKey(keyName[i]);
            assertEquals(resultKey[i].getName(), keyName[i]);
            assertEquals(resultKey[i].getDataType(), String.class);
        }

        for (int i = 0; i < 5; i++) {
            nid[i] = nodes[i].getID();
            assertTrue(tx.containsVertex(nid[i]));
            nodes[i] = tx.getVertex(nid[i]);

            for (int j = 0; j < 3; j++) {
                assertEquals(expectedValues[i][j], Double.parseDouble(nodes[i].getProperty(resultKey[j]).toString()), 0.01d);
            }
        }
    }
}
