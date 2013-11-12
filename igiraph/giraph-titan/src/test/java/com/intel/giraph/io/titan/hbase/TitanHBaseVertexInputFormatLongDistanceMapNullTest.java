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
import com.intel.giraph.io.DistanceMapWritable;
import com.intel.giraph.algorithms.apl.AveragePathLengthComputation;
import com.intel.giraph.io.formats.AveragePathLengthComputationOutputFormat;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_BACKEND;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_HOSTNAME;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_TABLENAME;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_PORT;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_READ_ONLY;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_AUTOTYPE;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_LABEL_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.TITAN_ID_OFFSET;
import org.apache.log4j.Logger;
import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

/**
 * Test TitanHBaseVertexInputFormatLongDistanceMapNullTest
 * which loads vertex with <code>long</code> vertex ID's,
 * <code>DistanceMap</code> vertex values
 * and <code>NullWritable</code> edge weights.
 * <p/>
 * This test firstly load a graph to Titan/HBase, then read out the graph from
 * TitanHBaseVertexInputFormat. Then run algorithm with input data.
 */
public class TitanHBaseVertexInputFormatLongDistanceMapNullTest {
    /**
     * LOG class
     */
    private static final Logger LOG = Logger
            .getLogger(TitanHBaseVertexInputFormatLongDistanceMapNullTest.class);

    public TitanTestGraph graph;
    private ImmutableClassesGiraphConfiguration<LongWritable, DistanceMapWritable, NullWritable> conf;

    @Before
    public void setUp() throws Exception {
        GiraphConfiguration giraphConf = new GiraphConfiguration();
        giraphConf.setComputationClass(AveragePathLengthComputation.class);
        giraphConf.setVertexInputFormatClass(TitanHBaseVertexInputFormatLongDistanceMapNull.class);
        giraphConf.setVertexOutputFormatClass(AveragePathLengthComputationOutputFormat.class);

        GIRAPH_TITAN_STORAGE_BACKEND.set(giraphConf, "hbase");
        GIRAPH_TITAN_STORAGE_HOSTNAME.set(giraphConf, "localhost");
        GIRAPH_TITAN_STORAGE_TABLENAME.set(giraphConf, "titan");
        GIRAPH_TITAN_STORAGE_PORT.set(giraphConf, "2181");
        GIRAPH_TITAN_STORAGE_READ_ONLY.set(giraphConf, "false");
        GIRAPH_TITAN_AUTOTYPE.set(giraphConf, "none");
        GIRAPH_TITAN.set(giraphConf, "giraph.titan.input");
        INPUT_EDGE_LABEL_LIST.set(giraphConf, "edge");

        HBaseAdmin hbaseAdmin = new HBaseAdmin(giraphConf);
        if (hbaseAdmin.isTableAvailable(GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf))) {
            hbaseAdmin.disableTable(GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf));
            hbaseAdmin.deleteTable(GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf));
        }

        conf = new ImmutableClassesGiraphConfiguration<LongWritable, DistanceMapWritable, NullWritable>(
                giraphConf);

        BaseConfiguration baseConfig = GiraphToTitanGraphFactory.generateTitanConfiguration(conf,
                GIRAPH_TITAN.get(giraphConf));
        GraphDatabaseConfiguration titanConfig = new GraphDatabaseConfiguration(baseConfig);
        graph = new TitanTestGraph(titanConfig);
    }

    @Ignore
    @Test
    public void VertexInputFormatLongDistanceMapNullTest() throws Exception {
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

        graph.makeLabel("edge").make();

        com.tinkerpop.blueprints.Vertex n0 = graph.addVertex(null);
        com.tinkerpop.blueprints.Vertex n1 = graph.addVertex(null);
        com.tinkerpop.blueprints.Vertex n2 = graph.addVertex(null);
        com.tinkerpop.blueprints.Vertex n3 = graph.addVertex(null);
        com.tinkerpop.blueprints.Vertex n4 = graph.addVertex(null);
        n0.addEdge("edge", n1);
        n0.addEdge("edge", n3);
        n1.addEdge("edge", n2);
        n1.addEdge("edge", n3);
        n2.addEdge("edge", n0);
        n2.addEdge("edge", n1);
        n2.addEdge("edge", n4);
        n3.addEdge("edge", n4);
        n4.addEdge("edge", n2);
        n4.addEdge("edge", n3);

        graph.commit();

        Integer[][] EXPECT_OUTPUT = {{4, 8}, {4, 7}, {4, 6}, {4, 5}, {4, 6}};

        Iterable<String> results = InternalVertexRunner.run(conf, new String[0], new String[0]);
        Assert.assertNotNull(results);

        Iterator<String> result = results.iterator();
        while (result.hasNext()) {
            String resultLine = result.next();
            LOG.info(" got: " + resultLine);
        }

        // Parse the results
        Map<Long, Integer[]> hopCountMap = parseResults(results);

        // check the results with the expected results
        for (Map.Entry<Long, Integer[]> entry : hopCountMap.entrySet()) {
            Integer[] vertexValue = entry.getValue();
            assertEquals(2, vertexValue.length);
            assertTrue(Arrays.equals(vertexValue, EXPECT_OUTPUT[(int) (entry.getKey().longValue() / TITAN_ID_OFFSET ) - 1]));
        }
    }

    @After
    public void done() throws IOException {
        close();
        LOG.info("***Done with VertexInputFormatLongDistanceMapNullTest****");
    }

    public void close() {
        if (null != graph) {
            graph.shutdown();
        }
    }

    /**
     * @param results String container of output lines.
     * @return Parsed KV pairs stored in Map.
     * @brief Parse the output.
     */
    private Map<Long, Integer[]> parseResults(Iterable<String> results) {
        Map<Long, Integer[]> hopCountResults = Maps.newHashMapWithExpectedSize(Iterables.size(results));
        for (String line : results) {
            Long key;
            Integer[] values = new Integer[2];

            // split
            String[] key_values = line.split("\\s+");

            // make sure line has three values
            assertEquals(key_values.length, 3);

            // get the key and values
            key = Long.parseLong(key_values[0]);
            values[0] = Integer.parseInt(key_values[1]);
            values[1] = Integer.parseInt(key_values[2]);

            // make sure key is unique
            assertFalse(hopCountResults.containsKey(key));

            // add KV to the map
            hopCountResults.put(key, values);
        }
        return hopCountResults;
    }

}
