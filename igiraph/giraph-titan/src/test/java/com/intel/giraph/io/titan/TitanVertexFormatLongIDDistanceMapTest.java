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

import com.intel.giraph.algorithms.apl.AveragePathLengthComputation;
import com.intel.giraph.io.DistanceMapWritable;
import com.intel.giraph.io.titan.hbase.TitanHBaseVertexInputFormatLongDistanceMapNull;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;
import org.junit.*;

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
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.OUTPUT_VERTEX_PROPERTY_KEY_LIST;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

//import org.junit.Ignore;

/**
 * Test TitanVertexOutputFormatLongIDDistanceMap which writes
 * back Giraph algorithm results to Titan.
 * <p/>
 * Each Vertex is with <code>Long</code> id,
 * and <code>DistanceMap</code> values.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public class TitanVertexFormatLongIDDistanceMapTest {
    /**
     * LOG class
     */
    private static final Logger LOG = Logger
        .getLogger(TitanVertexFormatLongIDDistanceMapTest.class);

    public TitanTestGraph graph = null;
    public TitanTransaction tx = null;
    private GraphDatabaseConfiguration titanConfig = null;

    private ImmutableClassesGiraphConfiguration<LongWritable, DistanceMapWritable, NullWritable> conf;

    @Before
    public void setUp() throws Exception {
        GiraphConfiguration giraphConf = new GiraphConfiguration();
        giraphConf.setComputationClass(AveragePathLengthComputation.class);
        giraphConf.setVertexInputFormatClass(TitanHBaseVertexInputFormatLongDistanceMapNull.class);
        giraphConf.setVertexOutputFormatClass(TitanVertexOutputFormatLongIDDistanceMap.class);

        GIRAPH_TITAN_STORAGE_BACKEND.set(giraphConf, "hbase");
        GIRAPH_TITAN_STORAGE_HOSTNAME.set(giraphConf, "localhost");
        GIRAPH_TITAN_STORAGE_TABLENAME.set(giraphConf, "titan");
        GIRAPH_TITAN_STORAGE_PORT.set(giraphConf, "2181");
        GIRAPH_TITAN_STORAGE_READ_ONLY.set(giraphConf, "false");
        GIRAPH_TITAN_AUTOTYPE.set(giraphConf, "none");
        GIRAPH_TITAN.set(giraphConf, "giraph.titan.input");
        INPUT_EDGE_LABEL_LIST.set(giraphConf, "edge");
        OUTPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "result_p0,result_p1");

        HBaseAdmin hbaseAdmin = new HBaseAdmin(giraphConf);
        String tableName = GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf);
        //even delete an existing table needs the table is enabled before deletion
        if (hbaseAdmin.isTableDisabled(tableName)) {
            hbaseAdmin.enableTable(tableName);
        }

        if (hbaseAdmin.isTableAvailable(tableName)) {
            hbaseAdmin.disableTable(tableName);
            hbaseAdmin.deleteTable(tableName);
        }


        conf = new ImmutableClassesGiraphConfiguration<LongWritable, DistanceMapWritable, NullWritable>(
            giraphConf);

        BaseConfiguration baseConfig = GiraphToTitanGraphFactory.generateTitanConfiguration(conf,
            GIRAPH_TITAN.get(giraphConf));
        titanConfig = new GraphDatabaseConfiguration(baseConfig);
        open();
    }

    @Ignore("Interacts with real resource")
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

        TitanLabel edge = tx.makeLabel("edge").make();

        TitanVertex[] nodes;
        nodes = new TitanVertex[5];
        nodes[0] = tx.addVertex();
        nodes[1] = tx.addVertex();
        nodes[2] = tx.addVertex();
        nodes[3] = tx.addVertex();
        nodes[4] = tx.addVertex();

        TitanEdge[] edges;
        edges = new TitanEdge[10];
        edges[0] = nodes[0].addEdge(edge, nodes[1]);
        edges[1] = nodes[0].addEdge(edge, nodes[3]);
        edges[2] = nodes[1].addEdge(edge, nodes[2]);
        edges[3] = nodes[1].addEdge(edge, nodes[3]);
        edges[4] = nodes[2].addEdge(edge, nodes[0]);
        edges[5] = nodes[2].addEdge(edge, nodes[1]);
        edges[6] = nodes[2].addEdge(edge, nodes[4]);
        edges[7] = nodes[3].addEdge(edge, nodes[4]);
        edges[8] = nodes[4].addEdge(edge, nodes[2]);
        edges[9] = nodes[4].addEdge(edge, nodes[3]);

        tx.commit();

        Integer[][] EXPECT_OUTPUT = {{4, 8}, {4, 7}, {4, 6}, {4, 5}, {4, 6}};

        Iterable<String> results = InternalVertexRunner.run(conf, new String[0], new String[0]);
        Assert.assertNotNull(results);

        Iterator<String> result = results.iterator();
        while (result.hasNext()) {
            String resultLine = result.next();
            LOG.info(" got: " + resultLine);
        }

        //verify data is written to Titan
        clopen();
        long[] nid;
        TitanKey[] resultKey;
        String[] keyName;
        nid = new long[5];
        resultKey = new TitanKey[2];
        keyName = new String[2];
        keyName[0] = "result_p0";
        keyName[1] = "result_p1";
        //check keys are generated for Titan
        for (int i = 0; i < 2; i++) {
            assertTrue(tx.containsType(keyName[i]));
            resultKey[i] = tx.getPropertyKey(keyName[i]);
            assertEquals(resultKey[i].getName(), keyName[i]);
            assertEquals(resultKey[i].getDataType(), String.class);
        }

        for (int i = 0; i < 5; i++) {
            nid[i] = nodes[i].getID();
            assertTrue(tx.containsVertex(nid[i]));
            nodes[i] = tx.getVertex(nid[i]);

            for (int j = 0; j < 2; j++) {
                assertEquals(EXPECT_OUTPUT[i][j], Double.parseDouble(nodes[i].getProperty(resultKey[j]).toString()), 0.01d);
            }
        }
    }

    @After
    public void done() throws IOException {
        close();
        LOG.info("***Done with VertexFormatLongIDDistanceMapTest****");
    }


    private void open() {
        graph = new TitanTestGraph(titanConfig);
        tx = graph.newTransaction();
        if (tx == null) {
            LOG.error("IGIRAPH ERROR: Unable to create Titan transaction! ");
            throw new RuntimeException(
                "execute: Failed to create Titan transaction!");
        }
    }

    public void close() {
        if (null != tx && tx.isOpen()) {
            tx.rollback();
        }

        if (null != graph) {
            graph.shutdown();
        }
    }

    private void clopen() {
        close();
        open();
    }
}

