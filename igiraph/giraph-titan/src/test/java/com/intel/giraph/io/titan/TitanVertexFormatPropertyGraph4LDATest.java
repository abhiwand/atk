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
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.junit.*;

import java.io.IOException;
import java.util.Iterator;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.EDGE_TYPE_PROPERTY_KEY;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_AUTOTYPE;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_BACKEND;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_HOSTNAME;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_PORT;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_READ_ONLY;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_TABLENAME;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_LABEL_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.OUTPUT_VERTEX_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.VERTEX_TYPE_PROPERTY_KEY;
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
public class TitanVertexFormatPropertyGraph4LDATest {
    /**
     * LOG class
     */
    private static final Logger LOG = Logger
        .getLogger(TitanVertexFormatPropertyGraph4LDATest.class);

    public TitanTestGraph graph = null;
    public TitanTransaction tx = null;
    private GiraphConfiguration giraphConf = null;
    private GraphDatabaseConfiguration titanConfig = null;
    private ImmutableClassesGiraphConfiguration<LongWritable, VertexData4LDAWritable, DoubleWithVectorWritable> conf;

    @Before
    public void setUp() throws Exception {
        giraphConf = new GiraphConfiguration();
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

        GIRAPH_TITAN_STORAGE_BACKEND.set(giraphConf, "hbase");
        GIRAPH_TITAN_STORAGE_HOSTNAME.set(giraphConf, "localhost");
        GIRAPH_TITAN_STORAGE_TABLENAME.set(giraphConf, "titan");
        GIRAPH_TITAN_STORAGE_PORT.set(giraphConf, "2181");
        GIRAPH_TITAN_STORAGE_READ_ONLY.set(giraphConf, "false");
        GIRAPH_TITAN_AUTOTYPE.set(giraphConf, "none");
        GIRAPH_TITAN.set(giraphConf, "giraph.titan.input");
        INPUT_EDGE_PROPERTY_KEY_LIST.set(giraphConf, "frequency");
        INPUT_EDGE_LABEL_LIST.set(giraphConf, "edge");
        VERTEX_TYPE_PROPERTY_KEY.set(giraphConf, "vertexType");
        EDGE_TYPE_PROPERTY_KEY.set(giraphConf, "edgeType");
        OUTPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "result_p0,result_p1,result_p2");

        HBaseAdmin hbaseAdmin = new HBaseAdmin(giraphConf);
        if (hbaseAdmin.isTableAvailable(GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf))) {
            hbaseAdmin.disableTable(GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf));
            hbaseAdmin.deleteTable(GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf));
        }

        conf = new ImmutableClassesGiraphConfiguration(giraphConf);

        BaseConfiguration baseConfig = GiraphToTitanGraphFactory.generateTitanConfiguration(conf,
            GIRAPH_TITAN.get(giraphConf));
        titanConfig = new GraphDatabaseConfiguration(baseConfig);
        open();

    }

    //@Ignore("Interacts with real resource")
    @Test
    public void VertexFormatPropertyGraph4LDATest() throws Exception {
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

        double[][] expectedValues = new double[][]{
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


        TitanKey vertexType = tx.makeKey("vertexType").dataType(String.class).make();
        TitanKey frequency = tx.makeKey("frequency").dataType(String.class).make();
        TitanLabel edge = tx.makeLabel("edge").make();

        TitanVertex[] nodes;
        nodes = new TitanVertex[11];
        nodes[0] = tx.addVertex();
        nodes[0].addProperty(vertexType, "d");
        nodes[1] = tx.addVertex();
        nodes[1].addProperty(vertexType, "d");
        nodes[2] = tx.addVertex();
        nodes[2].addProperty(vertexType, "d");
        nodes[3] = tx.addVertex();
        nodes[3].addProperty(vertexType, "d");
        nodes[4] = tx.addVertex();
        nodes[4].addProperty(vertexType, "d");
        nodes[5] = tx.addVertex();
        nodes[5].addProperty(vertexType, "d");
        nodes[6] = tx.addVertex();
        nodes[6].addProperty(vertexType, "w");
        nodes[7] = tx.addVertex();
        nodes[7].addProperty(vertexType, "w");
        nodes[8] = tx.addVertex();
        nodes[8].addProperty(vertexType, "w");
        nodes[9] = tx.addVertex();
        nodes[9].addProperty(vertexType, "w");
        nodes[10] = tx.addVertex();
        nodes[10].addProperty(vertexType, "w");

        TitanEdge[] edges;
        edges = new TitanEdge[20];
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


        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0]);
        Assert.assertNotNull(results);
        Iterator<String> result = results.iterator();
        while (result.hasNext()) {
            String resultLine = result.next();
            System.out.println(" got: " + resultLine);
        }

        //verify data is written to Titan
        clopen();
        long[] nid;
        TitanKey[] resultKey;
        String[] keyName;
        nid = new long[11];
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

        for (int i = 0; i < 11; i++) {
            nid[i] = nodes[i].getID();
            assertTrue(tx.containsVertex(nid[i]));
            nodes[i] = tx.getVertex(nid[i]);

            for (int j = 0; j < 3; j++) {
                assertEquals(expectedValues[i][j], Double.parseDouble(nodes[i].getProperty(resultKey[j]).toString()), 0.01d);
            }
        }
    }

    @After
    public void done() throws IOException {
        close();
        System.out.println("***Done with VertexFormatPropertyGraph4LDATest****");
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
