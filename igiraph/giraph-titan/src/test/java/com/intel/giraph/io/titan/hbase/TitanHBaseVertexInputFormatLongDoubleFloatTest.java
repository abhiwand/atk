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
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;

import com.intel.giraph.io.titan.hbase.TitanHBaseVertexInputFormatLongDoubleFloat;
import com.intel.giraph.io.titan.TitanTestGraph;
import com.intel.giraph.io.titan.GiraphToTitanGraphFactory;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.commons.configuration.BaseConfiguration;

import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.transaction.TransactionConfig;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.tinkerpop.blueprints.Direction;

import java.io.IOException;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;
import org.junit.After;
import org.junit.Assert;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;




/**
 * This test firstly load a graph to Titan/HBase, then read out the graph from
 * TitanHBaseVertexInputFormat. No special preparation needed before the test.
 */
public class TitanHBaseVertexInputFormatLongDoubleFloatTest {
    static final byte[] EDGE_STORE_FAMILY = Bytes.toBytes(Backend.EDGESTORE_NAME);
    public TitanTestGraph graph;
    public StandardTitanTx tx;
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
        VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "age");
        EDGE_PROPERTY_KEY_LIST.set(giraphConf, "time");
        EDGE_LABEL_LIST.set(giraphConf, "battled");

        HBaseAdmin hbaseAdmin = new HBaseAdmin(giraphConf);
        if (hbaseAdmin.isTableAvailable(GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf))) {
            hbaseAdmin.disableTable(GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf));
            hbaseAdmin.deleteTable(GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf));
        }

        conf = new ImmutableClassesGiraphConfiguration<LongWritable, DoubleWritable, DoubleWritable>(
                giraphConf);

        BaseConfiguration baseConfig = GiraphToTitanGraphFactory.generateTitanConfiguration(conf,
                "giraph.titan.input");
        GraphDatabaseConfiguration titanConfig = new GraphDatabaseConfiguration(baseConfig);
        graph = new TitanTestGraph(titanConfig);
        tx = graph.newTransaction(new TransactionConfig(titanConfig, false));

    }

    @Test
    public void TitanHBaseVertexInputLongDoubleFloatTest() throws Exception {

        TitanKey age = tx.makeType().name("age").unique(Direction.OUT).dataType(String.class).makePropertyKey();
        TitanKey time = tx.makeType().name("time").dataType(String.class).unique(Direction.OUT)
                .makePropertyKey();
        TitanLabel battled = tx.makeType().name("battled").makeEdgeLabel();

        TitanVertex n1 = tx.addVertex();
        TitanProperty p1 = n1.addProperty(age, "1000");
        TitanVertex n2 = tx.addVertex();
        TitanProperty p2 = n2.addProperty(age, "2000");
        TitanEdge e1 = n1.addEdge(battled, n2);
        e1.setProperty(time, "333");

        tx.commit();

        EXPECT_JSON_OUTPUT = new String[] { "[8,2000,[]]", "[4,1000,[[8,333]]]" };

        Iterable<String> results = InternalVertexRunner.run(conf, new String[0], new String[0]);
        Assert.assertNotNull(results);

        Iterator<String> result = results.iterator();
        int i = 0;
        while (i < EXPECT_JSON_OUTPUT.length && result.hasNext()) {
            String expectedLine = EXPECT_JSON_OUTPUT[i];
            String resultLine = result.next();
            System.out.println("expected: " + expectedLine + ", got: " + resultLine);
            assertEquals(resultLine, expectedLine);
            i++;
        }

    }

    @After
    public void done() throws IOException {
        close();
        System.out.println("***Done with TitanHBaseVertexInputLongDoubleFloatTest****");
    }

    public void close() {
        if (null != tx && tx.isOpen())
            tx.rollback();

        if (null != graph)
            graph.shutdown();
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
