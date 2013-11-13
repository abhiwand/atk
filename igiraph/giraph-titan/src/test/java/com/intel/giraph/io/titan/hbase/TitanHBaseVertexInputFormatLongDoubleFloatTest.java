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
package com.intel.giraph.io.titan.hbase;

import com.intel.giraph.io.titan.GiraphToTitanGraphFactory;
import com.intel.giraph.io.titan.TitanTestGraph;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
//import org.junit.Ignore;

import java.io.IOException;
import java.util.Iterator;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_BACKEND;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_HOSTNAME;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_TABLENAME;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_PORT;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_READ_ONLY;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_AUTOTYPE;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_LABEL_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_VERTEX_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_PROPERTY_KEY_LIST;
import org.apache.log4j.Logger;
import static junit.framework.Assert.assertEquals;


/**
 * This test firstly load a graph to Titan/HBase, then read out the graph from
 * TitanHBaseVertexInputFormat. No special preparation needed before the test.
 */
public class TitanHBaseVertexInputFormatLongDoubleFloatTest {
    /**
     * LOG class
     */
    private static final Logger LOG = Logger
            .getLogger(TitanHBaseVertexInputFormatLongDoubleFloatTest.class);

    public TitanTestGraph graph;
    protected String[] EXPECT_JSON_OUTPUT;
    private ImmutableClassesGiraphConfiguration<LongWritable, DoubleWritable, DoubleWritable> conf;

    @Before
    public void setUp() throws Exception {
        GiraphConfiguration giraphConf = new GiraphConfiguration();
        giraphConf.setComputationClass(EmptyComputation.class);
        giraphConf.setVertexInputFormatClass(TitanHBaseVertexInputFormatLongDoubleFloat.class);
        giraphConf.setVertexOutputFormatClass(JsonLongDoubleFloatDoubleVertexOutputFormat.class);
        GIRAPH_TITAN_STORAGE_BACKEND.set(giraphConf, "hbase");
        GIRAPH_TITAN_STORAGE_HOSTNAME.set(giraphConf, "localhost");
        GIRAPH_TITAN_STORAGE_TABLENAME.set(giraphConf, "titan");
        GIRAPH_TITAN_STORAGE_PORT.set(giraphConf, "2181");
        GIRAPH_TITAN_STORAGE_READ_ONLY.set(giraphConf, "false");
        GIRAPH_TITAN_AUTOTYPE.set(giraphConf, "none");
        GIRAPH_TITAN.set(giraphConf, "giraph.titan.input");
        INPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "age");
        INPUT_EDGE_PROPERTY_KEY_LIST.set(giraphConf, "time");
        INPUT_EDGE_LABEL_LIST.set(giraphConf, "battled");

        HBaseAdmin hbaseAdmin = new HBaseAdmin(giraphConf);
        if (hbaseAdmin.isTableAvailable(GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf))) {
            hbaseAdmin.disableTable(GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf));
            hbaseAdmin.deleteTable(GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf));
        }

        conf = new ImmutableClassesGiraphConfiguration<LongWritable, DoubleWritable, DoubleWritable>(
                giraphConf);

        BaseConfiguration baseConfig = GiraphToTitanGraphFactory.generateTitanConfiguration(conf,
                GIRAPH_TITAN.get(giraphConf));
        GraphDatabaseConfiguration titanConfig = new GraphDatabaseConfiguration(baseConfig);
        graph = new TitanTestGraph(titanConfig);
    }

    //@Ignore
    @Test
    public void VertexInputLongDoubleFloatTest() throws Exception {
        graph.makeKey("age").dataType(String.class).make();
        graph.makeKey("time").dataType(String.class).make();
        graph.makeLabel("battled").make();

        com.tinkerpop.blueprints.Vertex n1 = graph.addVertex(null);
        n1.setProperty("age", "1000");
        com.tinkerpop.blueprints.Vertex n2 = graph.addVertex(null);
        n2.setProperty("age", "2000");
        n1.addEdge("battled", n2).setProperty("time", "333");

        graph.commit();

        EXPECT_JSON_OUTPUT = new String[]{"[8,2000,[]]", "[4,1000,[[8,333]]]"};

        Iterable<String> results = InternalVertexRunner.run(conf, new String[0], new String[0]);
        Assert.assertNotNull(results);

        Iterator<String> result = results.iterator();
        int i = 0;
        while (i < EXPECT_JSON_OUTPUT.length && result.hasNext()) {
            String expectedLine = EXPECT_JSON_OUTPUT[i];
            String resultLine = result.next();
            LOG.info("expected: " + expectedLine + ", got: " + resultLine);
            assertEquals(resultLine, expectedLine);
            i++;
        }

    }

    @After
    public void done() throws IOException {
        close();
        LOG.info("***Done with VertexInputLongDoubleFloatTest****");
    }

    public void close() {
        if (null != graph) {
            graph.shutdown();
        }
    }

    /*
     * Test compute method that sends each edge a notification of its parents.
     * The test set only has a 1-1 parent-to-child ratio for this unit test.
     */
    public static class EmptyComputation extends
            BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

        @Override
        public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
                            Iterable<DoubleWritable> messages) throws IOException {
            vertex.voteToHalt();
        }
    }
}
