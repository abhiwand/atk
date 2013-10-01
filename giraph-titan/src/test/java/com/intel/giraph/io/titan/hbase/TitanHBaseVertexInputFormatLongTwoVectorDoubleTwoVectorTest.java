package com.intel.giraph.io.titan.hbase;

import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_BACKEND;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_HOSTNAME;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_TABLENAME;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_PORT;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.VERTEX_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.EDGE_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.EDGE_LABEL_LIST;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_READ_ONLY;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.GIRAPH_TITAN_AUTOTYPE;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;

import com.intel.giraph.io.titan.hbase.TitanHBaseVertexInputFormatLongTwoVectorDoubleTwoVector;
import com.intel.giraph.io.titan.TitanTestGraph;
import com.intel.giraph.io.titan.GiraphToTitanGraphFactory;
import com.intel.giraph.io.formats.JsonLongIDTwoVectorValueOutputFormat;
import com.intel.giraph.algorithms.lbp.LoopyBeliefPropagationComputation;

import com.intel.mahout.math.TwoVectorWritable;
import com.intel.mahout.math.DoubleWithTwoVectorWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.commons.configuration.BaseConfiguration;

import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.transaction.TransactionConfig;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.tinkerpop.blueprints.Direction;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import org.json.JSONException;
import org.json.JSONArray;

import java.io.IOException;
import java.util.Map;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;
import org.junit.After;
import org.junit.Assert;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;


/**
 * This test firstly load a graph to Titan/HBase, then read out the graph from
 * TitanHBaseVertexInputFormat. Then run algorithm with input data.
 */
public class TitanHBaseVertexInputFormatLongTwoVectorDoubleTwoVectorTest {
    static final byte[] EDGE_STORE_FAMILY = Bytes.toBytes(Backend.EDGESTORE_NAME);
    public TitanTestGraph graph;
    public StandardTitanTx tx;
    protected String[] EXPECT_JSON_OUTPUT;
    private GiraphConfiguration giraphConf;
    private ImmutableClassesGiraphConfiguration<LongWritable, TwoVectorWritable, DoubleWithTwoVectorWritable> conf;

    @Before
    public void setUp() throws Exception {
        giraphConf = new GiraphConfiguration();
        giraphConf.setComputationClass(LoopyBeliefPropagationComputation.class);
        giraphConf.setVertexInputFormatClass(TitanHBaseVertexInputFormatLongTwoVectorDoubleTwoVector.class);
        giraphConf.setVertexOutputFormatClass(JsonLongIDTwoVectorValueOutputFormat.class);
        giraphConf.set("lbp.maxSupersteps", "5");

        GIRAPH_TITAN_STORAGE_BACKEND.set(giraphConf, "hbase");
        GIRAPH_TITAN_STORAGE_HOSTNAME.set(giraphConf, "localhost");
        GIRAPH_TITAN_STORAGE_TABLENAME.set(giraphConf, "titan");
        GIRAPH_TITAN_STORAGE_PORT.set(giraphConf, "2181");
        GIRAPH_TITAN_STORAGE_READ_ONLY.set(giraphConf, "false");
        GIRAPH_TITAN_AUTOTYPE.set(giraphConf, "none");
        VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "red,blue,yellow");
        EDGE_PROPERTY_KEY_LIST.set(giraphConf, "weight");
        EDGE_LABEL_LIST.set(giraphConf, "friend");

        HBaseAdmin hbaseAdmin = new HBaseAdmin(giraphConf);
        if (hbaseAdmin.isTableAvailable(GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf))) {
            hbaseAdmin.disableTable(GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf));
            hbaseAdmin.deleteTable(GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf));
        }

        conf = new ImmutableClassesGiraphConfiguration<LongWritable, TwoVectorWritable, DoubleWithTwoVectorWritable>(
                giraphConf);

        BaseConfiguration baseConfig = GiraphToTitanGraphFactory.generateTitanConfiguration(conf,
                "giraph.titan.input");
        GraphDatabaseConfiguration titanConfig = new GraphDatabaseConfiguration(baseConfig);
        graph = new TitanTestGraph(titanConfig);
        tx = graph.newTransaction(new TransactionConfig(titanConfig, false));

    }

    @Test
    public void TitanHBaseVertexInputLongDoubleFloatTest() throws Exception {
        // a small four vertex graph
        String[] graph = new String[] { "[0,[1,0.1,0.1],[[1,1],[3,3]]]", "[1,[0.2,2,2],[[0,1],[2,2],[3,1]]]",
                "[2,[0.3,0.3,3],[[1,2],[4,4]]]", "[3,[0.4,4,0.4],[[0,3],[1,1],[4,4]]]",
                "[4,[5,5,0.5],[[3,4],[2,4]]]" };

        TitanKey red = tx.makeType().name("red").unique(Direction.OUT).dataType(Double.class)
                .makePropertyKey();
        TitanKey blue = tx.makeType().name("blue").unique(Direction.OUT).dataType(Double.class)
                .makePropertyKey();
        TitanKey yellow = tx.makeType().name("yellow").unique(Direction.OUT).dataType(Double.class)
                .makePropertyKey();
        TitanKey weight = tx.makeType().name("weight").dataType(Double.class).unique(Direction.OUT)
                .makePropertyKey();
        TitanLabel friend = tx.makeType().name("friend").makeEdgeLabel();

        TitanVertex n0 = tx.addVertex();
        n0.addProperty(red, 1d);
        n0.addProperty(blue, 0.1d);
        n0.addProperty(yellow, 0.1d);
        TitanVertex n1 = tx.addVertex();
        n1.addProperty(red, 0.2d);
        n1.addProperty(blue, 2d);
        n1.addProperty(yellow, 2d);
        TitanVertex n2 = tx.addVertex();
        n2.addProperty(red, 0.3d);
        n2.addProperty(blue, 0.3d);
        n2.addProperty(yellow, 3d);
        TitanVertex n3 = tx.addVertex();
        n3.addProperty(red, 0.4d);
        n3.addProperty(blue, 4d);
        n3.addProperty(yellow, 0.4d);
        TitanVertex n4 = tx.addVertex();
        n4.addProperty(red, 5d);
        n4.addProperty(blue, 5d);
        n4.addProperty(yellow, 0.5d);

        TitanEdge e0 = n0.addEdge(friend, n1);
        e0.setProperty(weight, 1.0d);
        TitanEdge e1 = n0.addEdge(friend, n3);
        e1.setProperty(weight, 3.0d);
        TitanEdge e2 = n1.addEdge(friend, n0);
        e2.setProperty(weight, 1.0d);
        TitanEdge e3 = n1.addEdge(friend, n2);
        e3.setProperty(weight, 2.0d);
        TitanEdge e4 = n1.addEdge(friend, n3);
        e4.setProperty(weight, 1.0d);
        TitanEdge e5 = n2.addEdge(friend, n1);
        e5.setProperty(weight, 2.0d);
        TitanEdge e6 = n2.addEdge(friend, n4);
        e6.setProperty(weight, 4.0d);
        TitanEdge e7 = n3.addEdge(friend, n0);
        e7.setProperty(weight, 3.0d);
        TitanEdge e8 = n3.addEdge(friend, n1);
        e8.setProperty(weight, 1.0d);
        TitanEdge e9 = n3.addEdge(friend, n4);
        e9.setProperty(weight, 4.0d);
        TitanEdge e10 = n4.addEdge(friend, n3);
        e10.setProperty(weight, 4.0d);
        TitanEdge e11 = n4.addEdge(friend, n2);
        e11.setProperty(weight, 4.0d);

        tx.commit();

        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0], new String[0]);
        Assert.assertNotNull(results);
        Iterator<String> result = results.iterator();
        while (result.hasNext()) {
            String resultLine = result.next();
            System.out.println(" got: " + resultLine);
        }

        Map<Long, Double[]> vertexValues = parseVertexValues(results);
        assertEquals(5, vertexValues.size());
        for (Map.Entry<Long, Double[]> entry : vertexValues.entrySet()) {
            Double[] vertexValue = entry.getValue();
            assertEquals(3, vertexValue.length);
            assertEquals(1.0, vertexValue[1].doubleValue(), 0.05d);
        }


    }

    @After
    public void done() throws IOException {
        close();
        System.out.println("***Done with TitanHBaseVertexInputFormatLongTwoVectorDoubleTwoVectorTest****");
    }

    public void close() {
        if (null != tx && tx.isOpen())
            tx.rollback();

        if (null != graph)
            graph.shutdown();
    }

    private Map<Long, Double[]> parseVertexValues(Iterable<String> results) {
        Map<Long, Double[]> vertexValues = Maps.newHashMapWithExpectedSize(Iterables.size(results));
        for (String line : results) {
            try {
                JSONArray jsonVertex = new JSONArray(line);
                if (jsonVertex.length() != 2) {
                    System.err.println("Wrong vertex output format!");
                    System.exit(-1);
                }
                long id = jsonVertex.getLong(0);
                JSONArray valueArray = jsonVertex.getJSONArray(1);
                if (valueArray.length() != 3) {
                    System.err.println("Wrong vertex output value format!");
                    System.exit(-1);
                }
                Double[] values = new Double[3];
                for (int i = 0; i < 3; i++) {
                    values[i] = valueArray.getDouble(i);
                }
                vertexValues.put(id, values);
            } catch (JSONException e) {
                throw new IllegalArgumentException("Couldn't get vertex from line " + line, e);
            }
        }
        return vertexValues;
    }

}
