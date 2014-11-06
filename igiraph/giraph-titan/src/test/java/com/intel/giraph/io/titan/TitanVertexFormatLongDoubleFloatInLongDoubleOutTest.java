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

import com.intel.giraph.algorithms.pr.PageRankComputation;
import com.intel.giraph.io.titan.hbase.TitanHBaseVertexInputFormatLongDoubleNull;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.Assert;
import org.junit.Test;


import static com.intel.giraph.io.titan.common.GiraphTitanConstants.*;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;


/**
 * This class is for testing TitanHBaseVertexInputFormatLongDoubleNull
 * and TitanVertexOutputFormatLongIDDoubleValue
 * The test contains the following steps:
 * firstly load a graph to Titan/HBase,
 * then read out the graph via  TitanHBaseVertexInputFormatLongDoubleNull,
 * then run algorithm with input data,
 * finally write back results to Titan via TitanVertexOutputFormatLongIDDoubleValue
 */
public class TitanVertexFormatLongDoubleFloatInLongDoubleOutTest
        extends TitanTestBase<LongWritable, DoubleWritable, NullWritable> {

    @Override
    protected void configure() {
        giraphConf.setComputationClass(PageRankComputation.class);
        giraphConf.setMasterComputeClass(PageRankComputation.PageRankMasterCompute.class);
        giraphConf.setAggregatorWriterClass(PageRankComputation.PageRankAggregatorWriter.class);
        giraphConf.setVertexInputFormatClass(TitanHBaseVertexInputFormatLongDoubleNull.class);
        giraphConf.setVertexOutputFormatClass(TitanVertexOutputFormatLongIDDoubleValue.class);
        giraphConf.set("pr.maxSupersteps", "30");
        giraphConf.set("pr.resetProbability", "0.15");
        giraphConf.set("pr.convergenceThreshold", "0.0001");

        INPUT_VERTEX_VALUE_PROPERTY_KEY_LIST.set(giraphConf, "default");
        INPUT_EDGE_VALUE_PROPERTY_KEY_LIST.set(giraphConf, "weight");
        INPUT_EDGE_LABEL_LIST.set(giraphConf, "edge");
        OUTPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "rank");
    }

    //@Ignore
    @Test
    public void VertexFormatLongDoubleFloatInLongDoubleOutTest() throws Exception {
        /*  input graph
        [0,0,[[1,1],[3,3]]]
        [1,0,[[0,1],[2,2],[3,1]]]
        [2,0,[[1,2],[4,4]]]
        [3,0,[[0,3],[1,1],[4,4]]]
        [4,0,[[3,4],[2,4]]]
        };

        */

        double[] expectedValues = new double[]{
                0.16682289373110673,
                0.24178880797750443,
                0.17098446073203238,
                0.24178880797750443,
                0.17098446073203238
        };


        TitanManagement graphManager = graph.getManagementSystem();
        graphManager.makePropertyKey("weight").dataType(String.class).make();
        graphManager.makeEdgeLabel("edge").make();
        graphManager.commit();

        TitanTransaction tx = graph.newTransaction();
        int numVertices = 5;
        TitanVertex[] nodes = new TitanVertex[numVertices];
        for (int i = 0; i < numVertices; i++) {
            nodes[i] = tx.addVertex();
        }

        //graph.commit();
        TitanEdge[] edges = new TitanEdge[12];
        edges[0] = nodes[0].addEdge("edge", nodes[1]);
        edges[0].setProperty("weight", "1.0");
        edges[1] = nodes[0].addEdge("edge", nodes[3]);
        edges[1].setProperty("weight", "3.0");
        edges[2] = nodes[1].addEdge("edge", nodes[0]);
        edges[2].setProperty("weight", "1.0");
        edges[3] = nodes[1].addEdge("edge", nodes[2]);
        edges[3].setProperty("weight", "2.0");
        edges[4] = nodes[1].addEdge("edge", nodes[3]);
        edges[4].setProperty("weight", "1.0");
        edges[5] = nodes[2].addEdge("edge", nodes[1]);
        edges[5].setProperty("weight", "2.0");
        edges[6] = nodes[2].addEdge("edge", nodes[4]);
        edges[6].setProperty("weight", "4.0");
        edges[7] = nodes[3].addEdge("edge", nodes[0]);
        edges[7].setProperty("weight", "3.0");
        edges[8] = nodes[3].addEdge("edge", nodes[1]);
        edges[8].setProperty("weight", "1.0");
        edges[9] = nodes[3].addEdge("edge", nodes[4]);
        edges[9].setProperty("weight", "4.0");
        edges[10] = nodes[4].addEdge("edge", nodes[3]);
        edges[10].setProperty("weight", "4.0");
        edges[11] = nodes[4].addEdge("edge", nodes[2]);
        edges[11].setProperty("weight", "4.0");
        tx.commit();


        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0]);
        Assert.assertNotNull(results);

        TitanTransaction tx1 = graph.newTransaction();
        long[] nid;
        PropertyKey resultKey;
        String keyName = "rank";
        nid = new long[5];
        //check keys are generated for Titan
        assertTrue(tx1.containsRelationType(keyName));
        resultKey = tx1.getPropertyKey(keyName);
        assertEquals(resultKey.getName(), keyName);
        assertEquals(resultKey.getDataType(), String.class);


        for (int i = 0; i < 5; i++) {
            nid[i] = nodes[i].getLongId();
            assertTrue(tx1.containsVertex(nid[i]));
            nodes[i] = tx1.getVertex(nid[i]);

            assertEquals(expectedValues[i], Double.parseDouble(nodes[i].getProperty(resultKey).toString()), 0.01d);

        }
        tx1.commit();
    }
}
