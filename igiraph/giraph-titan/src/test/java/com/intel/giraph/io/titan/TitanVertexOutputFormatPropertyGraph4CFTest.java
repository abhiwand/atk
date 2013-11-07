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

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.intel.giraph.algorithms.als.AlternatingLeastSquaresComputation;
import com.intel.giraph.algorithms.als.AlternatingLeastSquaresComputation.AlternatingLeastSquaresMasterCompute;
import com.intel.giraph.algorithms.als.AlternatingLeastSquaresComputation.SimpleAggregatorWriter;
import com.intel.giraph.io.titan.hbase.TitanHBaseVertexInputFormatPropertyGraph4CF;
import com.intel.giraph.io.EdgeDataWritable;
import com.intel.giraph.io.MessageDataWritable;
import com.intel.giraph.io.EdgeDataWritable.EdgeType;
import com.intel.giraph.io.VertexDataWritable;
import com.intel.giraph.io.VertexDataWritable.VertexType;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.transaction.StandardTransactionBuilder;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.util.ElementHelper;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.io.LongWritable;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.*;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

/**
 * Test TitanVertexOutputFormatPropertyGraph4CF
 * <p/>
 * Each vertex is with
 * <code>Long</code> id and <code>VertexData</code> values.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public class TitanVertexOutputFormatPropertyGraph4CFTest {
    public TitanTestGraph graph = null;
    public TitanTransaction tx = null;
    private GiraphConfiguration giraphConf;
    private GraphDatabaseConfiguration titanConfig;
    private ImmutableClassesGiraphConfiguration<LongWritable, VertexDataWritable, EdgeDataWritable> conf;

    @Before
    public void setUp() throws Exception {
        giraphConf = new GiraphConfiguration();
        giraphConf.setComputationClass(AlternatingLeastSquaresComputation.class);
        giraphConf.setMasterComputeClass(AlternatingLeastSquaresMasterCompute.class);
        giraphConf.setAggregatorWriterClass(SimpleAggregatorWriter.class);
        giraphConf.setVertexInputFormatClass(TitanHBaseVertexInputFormatPropertyGraph4CF.class);
        giraphConf.setVertexOutputFormatClass(TitanVertexOutputFormatPropertyGraph4CF.class);
        giraphConf.set("als.maxSupersteps", "6");
        giraphConf.set("als.featureDimension", "3");
        giraphConf.set("als.lambda", "0.05");
        giraphConf.set("als.convergenceThreshold", "0");

        GIRAPH_TITAN_STORAGE_BACKEND.set(giraphConf, "hbase");
        GIRAPH_TITAN_STORAGE_HOSTNAME.set(giraphConf, "localhost");
        GIRAPH_TITAN_STORAGE_TABLENAME.set(giraphConf, "titan");
        GIRAPH_TITAN_STORAGE_PORT.set(giraphConf, "2181");
        GIRAPH_TITAN_STORAGE_READ_ONLY.set(giraphConf, "false");
        GIRAPH_TITAN_AUTOTYPE.set(giraphConf, "none");
        GIRAPH_TITAN.set(giraphConf, "giraph.titan.input");
        INPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "default");
        INPUT_EDGE_PROPERTY_KEY_LIST.set(giraphConf, "weight");
        INPUT_EDGE_LABEL_LIST.set(giraphConf, "edge");
        VERTEX_TYPE_PROPERTY_KEY.set(giraphConf, "vertex_type");
        EDGE_TYPE_PROPERTY_KEY.set(giraphConf, "edge_type");
        OUTPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "result_p0,result_p1,result_p2");

        HBaseAdmin hbaseAdmin = new HBaseAdmin(giraphConf);
        if (hbaseAdmin.isTableAvailable(GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf))) {
            hbaseAdmin.disableTable(GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf));
            hbaseAdmin.deleteTable(GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf));
        }

        conf = new ImmutableClassesGiraphConfiguration<LongWritable, VertexDataWritable, EdgeDataWritable>(
                giraphConf);

        BaseConfiguration baseConfig = GiraphToTitanGraphFactory.generateTitanConfiguration(conf,
                GIRAPH_TITAN.get(giraphConf));
        titanConfig = new GraphDatabaseConfiguration(baseConfig);
        open();
    }

    //@Ignore
    @Test
    public void TitanVertexOutputFormatPropertyGraph4CFTest() throws Exception {
        /*
        String[] graph = new String[] {
                "[0,[],[l],[[2,1,[tr]],[3,2,[te]]]]",
                "[1,[],[l],[[2,5,[tr]],[4,3,[va]]]]",
                "[2,[],[r],[[0,1,[tr]],[1,5,[tr]]]]",
                "[3,[],[r],[[0,2,[te]]]]",
                "[4,[],[r],[[1,3,[va]]]]"
        };
        */

        double[][] expectedValues = new double[][]{
                {0.22733103186672185, 0.16592728476946825, 0.06253175723477887},
                {1.136655159333612, 0.8296364238473429, 0.3126587861738814},
                {2.7235109462314817, 1.9878710470306467, 0.7491538832804335},
                {0, 0, 0},
                {0, 0, 0}
        };

        TitanKey vertex_type = tx.makeKey("vertex_type").dataType(String.class).make();
        TitanKey edge_type = tx.makeKey("edge_type").dataType(String.class).make();
        TitanKey weight = tx.makeKey("weight").dataType(String.class).make();
        TitanLabel edge = tx.makeLabel("edge").make();

        TitanVertex[] nodes;
        nodes = new TitanVertex[5];
        nodes[0] = tx.addVertex();
        nodes[0].addProperty(vertex_type, "l");
        nodes[1] = tx.addVertex();
        nodes[1].addProperty(vertex_type, "l");
        nodes[2] = tx.addVertex();
        nodes[2].addProperty(vertex_type, "r");
        nodes[3] = tx.addVertex();
        nodes[3].addProperty(vertex_type, "r");
        nodes[4] = tx.addVertex();
        nodes[4].addProperty(vertex_type, "r");

        TitanEdge[] edges;
        edges = new TitanEdge[8];
        edges[0] = nodes[0].addEdge(edge, nodes[2]);
        edges[0].setProperty(weight, "1.0");
        edges[0].setProperty(edge_type, "tr");
        edges[1] = nodes[0].addEdge(edge, nodes[3]);
        edges[1].setProperty(weight, "2.0");
        edges[1].setProperty(edge_type, "te");
        edges[2] = nodes[1].addEdge(edge, nodes[2]);
        edges[2].setProperty(weight, "5.0");
        edges[2].setProperty(edge_type, "tr");
        edges[3] = nodes[1].addEdge(edge, nodes[4]);
        edges[3].setProperty(weight, "3.0");
        edges[3].setProperty(edge_type, "va");
        edges[4] = nodes[2].addEdge(edge, nodes[0]);
        edges[4].setProperty(weight, "1.0");
        edges[4].setProperty(edge_type, "tr");
        edges[5] = nodes[2].addEdge(edge, nodes[1]);
        edges[5].setProperty(weight, "5.0");
        edges[5].setProperty(edge_type, "tr");
        edges[6] = nodes[3].addEdge(edge, nodes[0]);
        edges[6].setProperty(weight, "2.0");
        edges[6].setProperty(edge_type, "te");
        edges[7] = nodes[4].addEdge(edge, nodes[1]);
        edges[7].setProperty(weight, "3.0");
        edges[7].setProperty(edge_type, "va");

        tx.commit();


        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0]);
        Assert.assertNotNull(results);
        Iterator<String> result = results.iterator();
        while (result.hasNext()) {
            String resultLine = result.next();
            System.out.println(" got: " + resultLine);
        }

        //verify results
        Map<Long, Double[]> vertexValues = parseVertexValues(results);
        assertNotNull(vertexValues);
        assertEquals(5, vertexValues.size());
        for (Map.Entry<Long, Double[]> entry : vertexValues.entrySet()) {
            Double[] vertexValue = entry.getValue();
            assertEquals(4, vertexValue.length);
            for (int j = 0; j < 3; j++) {
                assertEquals(expectedValues[(int) (entry.getKey().longValue()) / 4 - 1][j], vertexValue[j].doubleValue(), 0.01d);
            }
        }


        //verify data is written to Titan
        clopen();
        long[] nid;
        TitanKey[] resultKey;
        String[] keyName;
        nid = new long[5];
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

        for (int i = 0; i < 5; i++) {
            nid[i] = nodes[i].getID();
            assertTrue(tx.containsVertex(nid[0]));
            nodes[i] = tx.getVertex(nid[i]);

            for (int j = 0; j < 3; j++) {
                assertEquals(expectedValues[i][j], Double.parseDouble(nodes[i].getProperty(resultKey[j]).toString()), 0.01d);
            }
        }
    }

    @After
    public void done() throws IOException {
        close();
        System.out.println("***Done with TitanVertexOutputFormatPropertyGraph4CFTest****");
    }

    private void open() {
        graph = new TitanTestGraph(titanConfig);
        tx = graph.newTransaction();
    }

    private void close() {
        if (null != tx && tx.isOpen())
            tx.commit();

        if (null != graph)
            graph.shutdown();
    }

    private void clopen() {
        close();
        open();
    }

    private Map<Long, Double[]> parseVertexValues(Iterable<String> results) {
        Map<Long, Double[]> vertexValues = Maps.newHashMapWithExpectedSize(Iterables.size(results));
        for (String line : results) {
            try {
                JSONArray jsonVertex = new JSONArray(line);
                if (jsonVertex.length() != 4) {
                    throw new IllegalArgumentException("Wrong vertex output format! got " + jsonVertex.length());
                }
                // get vertex id
                long id = jsonVertex.getLong(0);
                // get vertex bias
                JSONArray biasArray = jsonVertex.getJSONArray(1);
                if (biasArray.length() != 1) {
                    throw new IllegalArgumentException("Wrong vertex bias value output value format!");
                }
                double bias = biasArray.getDouble(0);
                JSONArray valueArray = jsonVertex.getJSONArray(2);
                if (valueArray.length() != 3) {
                    throw new IllegalArgumentException("Wrong vertex vector output value format!");
                }
                Double[] values = new Double[4];
                values[3] = bias;
                for (int i = 0; i < 3; i++) {
                    values[i] = valueArray.getDouble(i);
                }
                vertexValues.put(id, values);
                // get vertex type
                JSONArray typeArray = jsonVertex.getJSONArray(3);
                if (typeArray.length() != 1) {
                    throw new IllegalArgumentException("Wrong vertex type output value format!");
                }
            } catch (JSONException e) {
                throw new IllegalArgumentException("Couldn't get vertex from line " + line, e);
            }
        }
        return vertexValues;
    }
}
