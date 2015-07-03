/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.taproot.giraph.io.titan;

import com.intel.taproot.giraph.algorithms.cc.ConnectedComponentsComputation;
import com.intel.taproot.giraph.combiner.MinimumLongCombiner;
import com.intel.taproot.giraph.io.titan.formats.TitanVertexInputFormatLongLongNull;
import com.intel.taproot.giraph.io.titan.formats.TitanVertexOutputFormatLongIDLongValue;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.Assert;
import org.junit.Test;

import static com.intel.taproot.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_LABEL_LIST;
import static com.intel.taproot.giraph.io.titan.common.GiraphTitanConstants.OUTPUT_VERTEX_PROPERTY_KEY_LIST;
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


    @Override
    protected void configure() throws Exception {
        giraphConf.setComputationClass(ConnectedComponentsComputation.class);
        giraphConf.setMasterComputeClass(ConnectedComponentsComputation.
                ConnectedComponentsMasterCompute.class);
        giraphConf.setAggregatorWriterClass(ConnectedComponentsComputation.
                ConnectedComponentsAggregatorWriter.class);
        giraphConf.setOutEdgesClass(ByteArrayEdges.class);
        giraphConf.setMessageCombinerClass(MinimumLongCombiner.class);
        giraphConf.setVertexInputFormatClass(TitanVertexInputFormatLongLongNull.class);
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

        TitanManagement graphManager = graph.getManagementSystem();
        graphManager.makeEdgeLabel("edge").make();
        graphManager.commit();

        TitanTransaction tx = graph.newTransaction();
        int numVertices = 10;
        TitanVertex[] nodes = new TitanVertex[numVertices];
        for (int i = 0; i < numVertices; i++) {
            nodes[i] = tx.addVertex();
        }

        int[][] expectedSubgraphs = {
                {0, 1, 2, 3, 4},
                {5, 6, 7},
                {8, 9}};


        TitanEdge[] edges = new TitanEdge[18];
        edges[0] = nodes[0].addEdge("edge", nodes[2]);
        edges[1] = nodes[0].addEdge("edge", nodes[4]);
        edges[2] = nodes[1].addEdge("edge", nodes[3]);
        edges[3] = nodes[2].addEdge("edge", nodes[0]);
        edges[4] = nodes[2].addEdge("edge", nodes[3]);
        edges[5] = nodes[3].addEdge("edge", nodes[1]);
        edges[6] = nodes[3].addEdge("edge", nodes[2]);
        edges[7] = nodes[3].addEdge("edge", nodes[4]);
        edges[8] = nodes[4].addEdge("edge", nodes[0]);
        edges[9] = nodes[4].addEdge("edge", nodes[3]);
        edges[10] = nodes[5].addEdge("edge", nodes[6]);
        edges[11] = nodes[5].addEdge("edge", nodes[7]);
        edges[12] = nodes[6].addEdge("edge", nodes[5]);
        edges[13] = nodes[6].addEdge("edge", nodes[7]);
        edges[14] = nodes[7].addEdge("edge", nodes[5]);
        edges[15] = nodes[7].addEdge("edge", nodes[6]);
        edges[16] = nodes[8].addEdge("edge", nodes[9]);
        edges[17] = nodes[9].addEdge("edge", nodes[8]);

        tx.commit();

        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0]);
        Assert.assertNotNull(results);

        //verify data is written to Titan
        tx = graph.newTransaction();
        long[] nid = new long[numVertices];
        PropertyKey resultKey;
        String keyName = "component_id";

        //check keys are generated for Titan
        assertTrue(tx.containsRelationType(keyName));
        resultKey = tx.getPropertyKey(keyName);
        assertEquals(resultKey.getName(), keyName);
        assertEquals(resultKey.getDataType(), String.class);

        for (int i = 0; i < expectedSubgraphs.length; i++) {
            TitanVertex firstNode =  tx.getVertex(nodes[expectedSubgraphs[i][0]].getLongId());
            long expectedComponentId = Long.parseLong(firstNode.getProperty(resultKey).toString());

            for (int j = 0; j < expectedSubgraphs[i].length; j++) {
                int nodeIndex =  expectedSubgraphs[i][j];
                nid[nodeIndex] = nodes[nodeIndex].getLongId();
                assertTrue(tx.containsVertex(nid[nodeIndex]));
                nodes[nodeIndex] = tx.getVertex(nid[nodeIndex]);

                assertEquals(expectedComponentId, Long.parseLong(nodes[nodeIndex].getProperty(resultKey).toString()), 0l);
            }

        }
        tx.commit();
    }

}
