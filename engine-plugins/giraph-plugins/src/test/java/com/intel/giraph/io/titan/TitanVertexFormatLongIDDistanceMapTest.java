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

package com.intel.giraph.io.titan;

import com.intel.giraph.algorithms.apl.AveragePathLengthComputation;
import com.intel.giraph.io.DistanceMapWritable;
import com.intel.giraph.io.titan.formats.TitanVertexInputFormatLongDistanceMapNull;
import com.intel.giraph.io.titan.formats.TitanVertexOutputFormatLongIDDistanceMap;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.TitanManagement;
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
 * Test TitanVertexOutputFormatLongIDDistanceMap which writes
 * back Giraph algorithm results to Titan.
 * <p/>
 * Each Vertex is with <code>Long</code> id,
 * and <code>DistanceMap</code> values.
 */
public class TitanVertexFormatLongIDDistanceMapTest
        extends TitanTestBase<LongWritable, DistanceMapWritable, NullWritable> {

    @Override
    protected void configure() throws Exception {
        giraphConf.setComputationClass(AveragePathLengthComputation.class);
        giraphConf.setMasterComputeClass(AveragePathLengthComputation.AveragePathLengthMasterCompute.class);
        giraphConf.setAggregatorWriterClass(AveragePathLengthComputation.AveragePathLengthAggregatorWriter.class);
        giraphConf.setVertexInputFormatClass(TitanVertexInputFormatLongDistanceMapNull.class);
        giraphConf.setVertexOutputFormatClass(TitanVertexOutputFormatLongIDDistanceMap.class);

        INPUT_EDGE_LABEL_LIST.set(giraphConf, "edge");
        OUTPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "result_p0,result_p1");
    }

    @Test
    public void VertexFormatLongIDDistanceMapTest() throws Exception {
        /*
        // edge list for test
        String[] graph = new String[]{
                "0 1",
                "0 3",
                "1 2",
                "1 3",
                "2 0",
                "2 1",
                "2 4",
                "3 4",
                "4 2",
                "4 3"
        };
        */

        TitanManagement graphManager = graph.getManagementSystem();
        graphManager.makeEdgeLabel("edge").make();
        graphManager.commit();

        TitanTransaction tx = graph.newTransaction();
        int numVertices = 5;
        TitanVertex[] nodes = new TitanVertex[numVertices];
        for (int i = 0; i < numVertices; i++) {
            nodes[i] = tx.addVertex();
        }

        TitanEdge[] edges = new TitanEdge[10];
        edges[0] = nodes[0].addEdge("edge", nodes[1]);
        edges[1] = nodes[0].addEdge("edge", nodes[3]);
        edges[2] = nodes[1].addEdge("edge", nodes[2]);
        edges[3] = nodes[1].addEdge("edge", nodes[3]);
        edges[4] = nodes[2].addEdge("edge", nodes[0]);
        edges[5] = nodes[2].addEdge("edge", nodes[1]);
        edges[6] = nodes[2].addEdge("edge", nodes[4]);
        edges[7] = nodes[3].addEdge("edge", nodes[4]);
        edges[8] = nodes[4].addEdge("edge", nodes[2]);
        edges[9] = nodes[4].addEdge("edge", nodes[3]);

        tx.commit();

        Integer[][] EXPECT_OUTPUT = {{4, 8}, {4, 7}, {4, 6}, {4, 5}, {4, 6}};

        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0], new String[0]);
        Assert.assertNotNull(results);

        //verify data is written to Titan
        tx = graph.newTransaction();
        long[] nid;
        PropertyKey[] resultKey;
        String[] keyName;
        nid = new long[5];
        resultKey = new PropertyKey[2];
        keyName = new String[2];
        keyName[0] = "result_p0";
        keyName[1] = "result_p1";
        //check keys are generated for Titan
        for (int i = 0; i < 2; i++) {
            assertTrue(tx.containsRelationType(keyName[i]));
            resultKey[i] = tx.getPropertyKey(keyName[i]);
            assertEquals(resultKey[i].getName(), keyName[i]);
            assertEquals(resultKey[i].getDataType(), String.class);
        }

        for (int i = 0; i < 5; i++) {
            nid[i] = nodes[i].getLongId();
            assertTrue(tx.containsVertex(nid[i]));
            nodes[i] = tx.getVertex(nid[i]);

            for (int j = 0; j < 2; j++) {
                assertEquals(EXPECT_OUTPUT[i][j], Double.parseDouble(nodes[i].getProperty(resultKey[j]).toString()), 0.01d);
            }
        }
        tx.commit();
    }
}
