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

import com.intel.taproot.giraph.algorithms.pr.PageRankComputation;
import com.intel.taproot.giraph.io.titan.formats.TitanVertexInputFormatLongDoubleNull;
import com.intel.taproot.giraph.io.titan.formats.TitanVertexOutputFormatLongIDDoubleValue;
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


import static com.intel.taproot.giraph.io.titan.common.GiraphTitanConstants.*;
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
        giraphConf.setVertexInputFormatClass(TitanVertexInputFormatLongDoubleNull.class);
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
