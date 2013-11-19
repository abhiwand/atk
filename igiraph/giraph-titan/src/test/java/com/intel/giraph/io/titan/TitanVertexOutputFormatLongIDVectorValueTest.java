package com.intel.giraph.io.titan;

import com.intel.giraph.algorithms.lbp.LoopyBeliefPropagationComputation;
import com.intel.giraph.io.titan.hbase.TitanHBaseVertexInputFormatLongTwoVectorDoubleTwoVector;
import com.intel.mahout.math.DoubleWithTwoVectorWritable;
import com.intel.mahout.math.TwoVectorWritable;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.hbase.client.HBaseAdmin;
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
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.OUTPUT_VERTEX_PROPERTY_KEY_LIST;
import org.apache.log4j.Logger;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 * This class is for testing TitanVertexOutputFormatLongIDVectorValue
 * The test contains the following steps:
 * firstly load a graph to Titan/HBase,
 * then read out the graph from TitanHBaseVertexInputFormat,
 * then run algorithm with input data,
 * finally write back results to Titan.
 */
public class TitanVertexOutputFormatLongIDVectorValueTest {
    /**
     * LOG class
     */
    private static final Logger LOG = Logger
            .getLogger(TitanVertexOutputFormatLongIDVectorValueTest.class);

    public TitanTestGraph graph = null;
    public TitanTransaction tx = null;
    private GiraphConfiguration giraphConf;
    private GraphDatabaseConfiguration titanConfig;
    private ImmutableClassesGiraphConfiguration<LongWritable, TwoVectorWritable, DoubleWithTwoVectorWritable> conf;

    @Before
    public void setUp() throws Exception {
        giraphConf = new GiraphConfiguration();
        giraphConf.setComputationClass(LoopyBeliefPropagationComputation.class);
        giraphConf.setVertexInputFormatClass(TitanHBaseVertexInputFormatLongTwoVectorDoubleTwoVector.class);
        giraphConf.setVertexOutputFormatClass(TitanVertexOutputFormatLongIDVectorValue.class);
        giraphConf.set("lbp.maxSupersteps", "5");

        GIRAPH_TITAN_STORAGE_BACKEND.set(giraphConf, "hbase");
        GIRAPH_TITAN_STORAGE_HOSTNAME.set(giraphConf, "localhost");
        GIRAPH_TITAN_STORAGE_TABLENAME.set(giraphConf, "titan");
        GIRAPH_TITAN_STORAGE_PORT.set(giraphConf, "2181");
        GIRAPH_TITAN_STORAGE_READ_ONLY.set(giraphConf, "false");
        GIRAPH_TITAN_AUTOTYPE.set(giraphConf, "none");
        GIRAPH_TITAN.set(giraphConf, "giraph.titan.input");
        INPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "red,blue,yellow");
        INPUT_EDGE_PROPERTY_KEY_LIST.set(giraphConf, "weight");
        INPUT_EDGE_LABEL_LIST.set(giraphConf, "friend");
        OUTPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "result_red,result_blue,result_yellow");

        HBaseAdmin hbaseAdmin = new HBaseAdmin(giraphConf);
        if (hbaseAdmin.isTableAvailable(GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf))) {
            hbaseAdmin.disableTable(GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf));
            hbaseAdmin.deleteTable(GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf));
        }

        conf = new ImmutableClassesGiraphConfiguration<LongWritable, TwoVectorWritable, DoubleWithTwoVectorWritable>(
                giraphConf);

        BaseConfiguration baseConfig = GiraphToTitanGraphFactory.generateTitanConfiguration(conf,
                GIRAPH_TITAN.get(giraphConf));
        titanConfig = new GraphDatabaseConfiguration(baseConfig);
        open();
    }

    //@Ignore
    @Test
    public void VertexOutputFormatLongIDVectorValueTest() throws Exception {
        /* a small four vertex graph
        String[] graph = new String[] { "[0,[1,0.1,0.1],[[1,1],[3,3]]]", "[1,[0.2,2,2],[[0,1],[2,2],[3,1]]]",
                "[2,[0.3,0.3,3],[[1,2],[4,4]]]", "[3,[0.4,4,0.4],[[0,3],[1,1],[4,4]]]",
                "[4,[5,5,0.5],[[3,4],[2,4]]]" };
         */

        TitanKey red = tx.makeKey("red").dataType(String.class).make();
        TitanKey blue = tx.makeKey("blue").dataType(String.class).make();
        TitanKey yellow = tx.makeKey("yellow").dataType(String.class).make();
        TitanKey weight = tx.makeKey("weight").dataType(String.class).make();
        TitanLabel friend = tx.makeLabel("friend").make();

        TitanVertex n0 = tx.addVertex();
        n0.addProperty(red, "1");
        n0.addProperty(blue, "0.1");
        n0.addProperty(yellow, "0.1");
        TitanVertex n1 = tx.addVertex();
        n1.addProperty(red, "0.2");
        n1.addProperty(blue, "2");
        n1.addProperty(yellow, "2");
        TitanVertex n2 = tx.addVertex();
        n2.addProperty(red, "0.3");
        n2.addProperty(blue, "0.3");
        n2.addProperty(yellow, "3");
        TitanVertex n3 = tx.addVertex();
        n3.addProperty(red, "0.4");
        n3.addProperty(blue, "4");
        n3.addProperty(yellow, "0.4");
        TitanVertex n4 = tx.addVertex();
        n4.addProperty(red, "5");
        n4.addProperty(blue, "5");
        n4.addProperty(yellow, "0.5");

        TitanEdge e0 = n0.addEdge(friend, n1);
        e0.setProperty(weight, "1.0");
        TitanEdge e1 = n0.addEdge(friend, n3);
        e1.setProperty(weight, "3.0");
        TitanEdge e2 = n1.addEdge(friend, n0);
        e2.setProperty(weight, "1.0");
        TitanEdge e3 = n1.addEdge(friend, n2);
        e3.setProperty(weight, "2.0");
        TitanEdge e4 = n1.addEdge(friend, n3);
        e4.setProperty(weight, "1.0");
        TitanEdge e5 = n2.addEdge(friend, n1);
        e5.setProperty(weight, "2.0");
        TitanEdge e6 = n2.addEdge(friend, n4);
        e6.setProperty(weight, "4.0");
        TitanEdge e7 = n3.addEdge(friend, n0);
        e7.setProperty(weight, "3.0");
        TitanEdge e8 = n3.addEdge(friend, n1);
        e8.setProperty(weight, "1.0");
        TitanEdge e9 = n3.addEdge(friend, n4);
        e9.setProperty(weight, "4.0");
        TitanEdge e10 = n4.addEdge(friend, n3);
        e10.setProperty(weight, "4.0");
        TitanEdge e11 = n4.addEdge(friend, n2);
        e11.setProperty(weight, "4.0");

        tx.commit();

        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0], new String[0]);
        Assert.assertNotNull(results);

        //verify data is written to Titan
        clopen();
        long nid = n0.getID();
        assertTrue(tx.containsVertex(nid));
        assertTrue(tx.containsType("result_blue"));
        TitanKey result_blue = tx.getPropertyKey("result_blue");
        assertEquals(result_blue.getDataType(), String.class);
        assertEquals(result_blue.getName(), "result_blue");
        n0 = tx.getVertex(nid);
        assertEquals(1.0, Double.parseDouble(n0.getProperty(result_blue).toString()), 0.05d);

        Iterator<String> result = results.iterator();
        while (result.hasNext()) {
            String resultLine = result.next();
            LOG.info(" got: " + resultLine);
        }
    }


    private void open() {
        graph = new TitanTestGraph(titanConfig);
        tx = graph.newTransaction();
        if (tx == null) {
            LOG.error("IGIRAPH ERROR: Unable to create Titan transaction! ");
        }
    }

    public void close() {
        if (null != tx && tx.isOpen()){
            tx.rollback();
        }

        if (null != graph){
            graph.shutdown();
        }
    }

    private void clopen() {
        close();
        open();
    }

    @After
    public void done() throws IOException {
        close();
        LOG.info("***Done with VertexOutputFormatLongIDVectorValueTest****");
    }
}