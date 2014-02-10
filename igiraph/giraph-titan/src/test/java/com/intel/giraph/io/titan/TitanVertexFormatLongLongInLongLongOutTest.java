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

import com.intel.giraph.algorithms.cc.ConnectedComponentsComputation;
import com.intel.giraph.combiner.MinimumLongCombiner;
import com.intel.giraph.io.titan.hbase.TitanHBaseVertexInputFormatLongLongNull;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanVertex;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.Assert;
import org.junit.Test;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_LABEL_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.OUTPUT_VERTEX_PROPERTY_KEY_LIST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This class is for testing TitanHBaseVertexInputFormatLongLongNull
 * <p/>
 * which loads vertex with <code>long</code> vertex ID's,
 * <code>long</code> vertex values
 * and <code>null</code> edge weights.
 * <p/>
 * This test firstly load a graph to Titan/HBase, then read out the graph from
 * TitanHBaseVertexInputFormat. Then run algorithm with input data.
 */
public class TitanVertexFormatLongLongInLongLongOutTest
    extends TitanTestBase<LongWritable, LongWritable, NullWritable> {
    double[] expectedValues = new double[]{
        4, 4, 4, 4, 4, 24, 24, 24, 36, 36
    };


    @Override
    protected void configure() throws Exception {
        giraphConf.setComputationClass(ConnectedComponentsComputation.class);
        giraphConf.setMasterComputeClass(ConnectedComponentsComputation.
            ConnectedComponentsMasterCompute.class);
        giraphConf.setAggregatorWriterClass(ConnectedComponentsComputation.
            ConnectedComponentsAggregatorWriter.class);
        giraphConf.setOutEdgesClass(ByteArrayEdges.class);
        giraphConf.setCombinerClass(MinimumLongCombiner.class);
        giraphConf.setVertexInputFormatClass(TitanHBaseVertexInputFormatLongLongNull.class);
        giraphConf.setVertexOutputFormatClass(TitanVertexOutputFormatLongIDLongValue.class);

        INPUT_EDGE_LABEL_LIST.set(giraphConf, "edge");
        OUTPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "component_id");

    }

    //@Ignore
    @Test
    public void VertexInputFormatLongLongNullTest() throws Exception {
        /*
        inputGraph = new String[]{
            "0 2 4",
            "1 3 ",
            "2 0 3",
            "3 1 2 4",
            "4 0 3",

            "5 6 7",
            "6 5 7",
            "7 5 6",

            "8 9",
            "9 8"
        };
        */

        TitanLabel edge = tx.makeLabel("edge").make();

        int numVertices = 10;
        TitanVertex[] nodes = new TitanVertex[numVertices];
        for (int i = 0; i < numVertices; i++) {
            nodes[i] = tx.addVertex();
        }

        TitanEdge[] edges = new TitanEdge[18];
        edges[0] = nodes[0].addEdge(edge, nodes[2]);
        edges[1] = nodes[0].addEdge(edge, nodes[4]);
        edges[2] = nodes[1].addEdge(edge, nodes[3]);
        edges[3] = nodes[2].addEdge(edge, nodes[0]);
        edges[4] = nodes[2].addEdge(edge, nodes[3]);
        edges[5] = nodes[3].addEdge(edge, nodes[1]);
        edges[6] = nodes[3].addEdge(edge, nodes[2]);
        edges[7] = nodes[3].addEdge(edge, nodes[4]);
        edges[8] = nodes[4].addEdge(edge, nodes[0]);
        edges[9] = nodes[4].addEdge(edge, nodes[3]);
        edges[10] = nodes[5].addEdge(edge, nodes[6]);
        edges[11] = nodes[5].addEdge(edge, nodes[7]);
        edges[12] = nodes[6].addEdge(edge, nodes[5]);
        edges[13] = nodes[6].addEdge(edge, nodes[7]);
        edges[14] = nodes[7].addEdge(edge, nodes[5]);
        edges[15] = nodes[7].addEdge(edge, nodes[6]);
        edges[16] = nodes[8].addEdge(edge, nodes[9]);
        edges[17] = nodes[9].addEdge(edge, nodes[8]);

        tx.commit();

        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0]);
        Assert.assertNotNull(results);

        //verify data is written to Titan
        startNewTransaction();
        long[] nid = new long[numVertices];
        TitanKey resultKey;
        String keyName = "component_id";

        //check keys are generated for Titan
        assertTrue(tx.containsType(keyName));
        resultKey = tx.getPropertyKey(keyName);
        assertEquals(resultKey.getName(), keyName);
        assertEquals(resultKey.getDataType(), String.class);


        for (int i = 0; i < numVertices; i++) {
            nid[i] = nodes[i].getID();
            assertTrue(tx.containsVertex(nid[i]));
            nodes[i] = tx.getVertex(nid[i]);
            assertEquals(expectedValues[i], Double.parseDouble(nodes[i].getProperty(resultKey).toString()), 0d);
        }
    }

}

