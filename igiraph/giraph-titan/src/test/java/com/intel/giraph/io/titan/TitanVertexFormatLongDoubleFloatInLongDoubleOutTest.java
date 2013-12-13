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

import com.intel.giraph.algorithms.pr.PageRankComputation;
import com.intel.giraph.io.titan.hbase.TitanHBaseVertexInputFormatLongDoubleFloat;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

import java.io.IOException;
import java.util.Iterator;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_AUTOTYPE;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_BACKEND;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_HOSTNAME;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_PORT;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_READ_ONLY;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_TABLENAME;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_LABEL_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_VERTEX_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.OUTPUT_VERTEX_PROPERTY_KEY_LIST;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

//import org.apache.giraph.examples.SimplePageRankComputation;

/**
 * This class is for testing TitanHBaseVertexInputFormatLongDoubleFloat
 * and TitanVertexOutputFormatLongIDDoubleValue
 * The test contains the following steps:
 * firstly load a graph to Titan/HBase,
 * then read out the graph via  TitanHBaseVertexInputFormatLongDoubleFloat,
 * then run algorithm with input data,
 * finally write back results to Titan via TitanVertexOutputFormatLongIDDoubleValue
 */
public class TitanVertexFormatLongDoubleFloatInLongDoubleOutTest 
    extends TitanTestBase<LongWritable, DoubleWritable, FloatWritable> {

    @Override
    protected void configure() {
        giraphConf.setComputationClass(PageRankComputation.class);
        giraphConf.setMasterComputeClass(PageRankComputation.PageRankMasterCompute.class);
        giraphConf.setAggregatorWriterClass(PageRankComputation.PageRankAggregatorWriter.class);
        giraphConf.setVertexInputFormatClass(TitanHBaseVertexInputFormatLongDoubleFloat.class);
        giraphConf.setVertexOutputFormatClass(TitanVertexOutputFormatLongIDDoubleValue.class);
        giraphConf.set("pr.maxSupersteps", "30");
        giraphConf.set("pr.resetProbability", "0.15");
        giraphConf.set("pr.convergenceThreshold", "0.0001");

        INPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "default");
        INPUT_EDGE_PROPERTY_KEY_LIST.set(giraphConf, "weight");
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

        TitanKey weight = tx.makeKey("weight").dataType(String.class).make();
        TitanLabel edge = tx.makeLabel("edge").make();

        TitanVertex[] nodes;
        nodes = new TitanVertex[5];
        nodes[0] = tx.addVertex();
        nodes[1] = tx.addVertex();
        nodes[2] = tx.addVertex();
        nodes[3] = tx.addVertex();
        nodes[4] = tx.addVertex();

        TitanEdge[] edges;
        edges = new TitanEdge[12];
        edges[0] = nodes[0].addEdge(edge, nodes[1]);
        edges[0].setProperty(weight, "1.0");
        edges[1] = nodes[0].addEdge(edge, nodes[3]);
        edges[1].setProperty(weight, "3.0");
        edges[2] = nodes[1].addEdge(edge, nodes[0]);
        edges[2].setProperty(weight, "1.0");
        edges[3] = nodes[1].addEdge(edge, nodes[2]);
        edges[3].setProperty(weight, "2.0");
        edges[4] = nodes[1].addEdge(edge, nodes[3]);
        edges[4].setProperty(weight, "1.0");
        edges[5] = nodes[2].addEdge(edge, nodes[1]);
        edges[5].setProperty(weight, "2.0");
        edges[6] = nodes[2].addEdge(edge, nodes[4]);
        edges[6].setProperty(weight, "4.0");
        edges[7] = nodes[3].addEdge(edge, nodes[0]);
        edges[7].setProperty(weight, "3.0");
        edges[8] = nodes[3].addEdge(edge, nodes[1]);
        edges[8].setProperty(weight, "1.0");
        edges[9] = nodes[3].addEdge(edge, nodes[4]);
        edges[9].setProperty(weight, "4.0");
        edges[10] = nodes[4].addEdge(edge, nodes[3]);
        edges[10].setProperty(weight, "4.0");
        edges[11] = nodes[4].addEdge(edge, nodes[2]);
        edges[11].setProperty(weight, "4.0");

        tx.commit();


        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0]);
        Assert.assertNotNull(results);
        Iterator<String> result = results.iterator();
        while (result.hasNext()) {
            String resultLine = result.next();
            LOG.info(" got: " + resultLine);
        }

        startNewTransaction();        
        long[] nid;
        TitanKey resultKey = null;
        String keyName = "rank";
        nid = new long[5];
        //check keys are generated for Titan
        assertTrue(tx.containsType(keyName));
        resultKey = tx.getPropertyKey(keyName);
        assertEquals(resultKey.getName(), keyName);
        assertEquals(resultKey.getDataType(), String.class);


        for (int i = 0; i < 5; i++) {
            nid[i] = nodes[i].getID();
            assertTrue(tx.containsVertex(nid[i]));
            nodes[i] = tx.getVertex(nid[i]);

            assertEquals(expectedValues[i], Double.parseDouble(nodes[i].getProperty(resultKey).toString()), 0.01d);

        }
    }
}
