package com.intel.giraph.io.titan;

import com.intel.giraph.algorithms.lbp.LoopyBeliefPropagationComputation;
import com.intel.giraph.algorithms.lp.LabelPropagationComputation;
import com.intel.giraph.io.titan.hbase.TitanHBaseVertexInputFormatPropertyGraph4LBP;
import com.intel.giraph.io.titan.hbase.TitanHBaseVertexInputFormatPropertyGraph4LP;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanVertex;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Ignore;
import java.util.HashMap;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_LABEL_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_VALUE_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_VERTEX_VALUE_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.OUTPUT_VERTEX_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.VECTOR_VALUE;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.VERTEX_TYPE_PROPERTY_KEY;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.VERTEX_TYPE_TEST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.VERTEX_TYPE_TRAIN;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.VERTEX_TYPE_VALIDATE;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 * This class is for testing following formats:
 * TitanHBaseVertexInputFormatPropertyGraph4LBP,
 * TitanVertexOutputFormatPropertyGraph4LBP,
 * TitanHBaseVertexInputFormatPropertyGraph4LP,
 * TitanVertexOutputFormatPropertyGraph4LP
 *
 * The test contains the following steps:
 * firstly load a graph to Titan/HBase,
 * then read out the graph from TitanHBaseVertexInputFormat,
 * then run algorithm with input data,
 * finally write back results to Titan.
 */
public class TitanVertexFormatPropertyGraph4LBPLPTest
    extends TitanTestBase {
    private int numKeys = 3;
    private int numVertices = 5;
    private TitanVertex[] nodes = new TitanVertex[numVertices];
    /*test graph
    String[] graph = new String[] {
        "[0,[1,0.1,0.1],[],[[1,1,[]],[3,3,[]]]]",
        "[1,[0.2,2,2],[\"TR\"],[[0,1,[]],[2,2,[]],[3,1,[]]]]",
        "[2,[0.3,0.3,3],[\"TR\"],[[1,2,[]],[4,4,[]]]]",
        "[3,[0.4,4,0.4],[\"TE\"],[[0,3,[]],[1,1,[]],[4,4,[]]]]",
        "[4,[5,5,0.5],[\"VA\"],[[3,4,[]],[2,4,[]]]]"
    };
    */
    private HashMap<Long, Double[]> expectedLbpValues = new HashMap<Long, Double[]>();
    private HashMap<Long, Double[]> expectedLpValues = new HashMap<Long, Double[]>();


    @Override
    protected void configure() throws Exception {
        expectedLbpValues.put(4L, new Double[]{0.562,0.088,0.350});
        expectedLbpValues.put(8L, new Double[]{0.042,0.102,0.855});
        expectedLbpValues.put(12L, new Double[]{0.038,0.087,0.874});
        expectedLbpValues.put(16L, new Double[]{0.228,0.048,0.724});
        expectedLbpValues.put(20L, new Double[]{0.039,0.088,0.874});

        expectedLpValues.put(4L, new Double[]{0.833,0.083,0.083});
        expectedLpValues.put(8L, new Double[]{0.271,0.271,0.458});
        expectedLpValues.put(12L, new Double[]{0.083,0.083,0.833});
        expectedLpValues.put(16L, new Double[]{0.083,0.833,0.083});
        expectedLpValues.put(20L, new Double[]{0.083,0.458,0.458});

        TitanKey red = tx.makeKey("red").dataType(String.class).make();
        TitanKey blue = tx.makeKey("blue").dataType(String.class).make();
        TitanKey yellow = tx.makeKey("yellow").dataType(String.class).make();
        TitanKey weight = tx.makeKey("weight").dataType(String.class).make();
        TitanKey vertexType = tx.makeKey("vertexType").dataType(String.class).make();
        TitanKey prior = tx.makeKey("prior").dataType(String.class).make();
        TitanLabel friend = tx.makeLabel("friend").make();

        for (int i = 0; i < numVertices; i++) {
            nodes[i] = tx.addVertex();
        }

        nodes[0].addProperty(red, "1");
        nodes[0].addProperty(blue, "0.1");
        nodes[0].addProperty(yellow, "0.1");
        nodes[0].addProperty(vertexType, VERTEX_TYPE_TRAIN);
        nodes[0].addProperty(prior, "1 0.1 0.1");
        nodes[1].addProperty(red, "0.2");
        nodes[1].addProperty(blue, "2");
        nodes[1].addProperty(yellow, "2");
        nodes[1].addProperty(vertexType, VERTEX_TYPE_TRAIN);
        nodes[1].addProperty(prior, "0.2, 2, 2");
        nodes[2].addProperty(red, "0.3");
        nodes[2].addProperty(blue, "0.3");
        nodes[2].addProperty(yellow, "3");
        nodes[2].addProperty(vertexType, VERTEX_TYPE_TRAIN);
        nodes[2].addProperty(prior, "0.3, 0.3, 3");
        nodes[3].addProperty(red, "0.4");
        nodes[3].addProperty(blue, "4");
        nodes[3].addProperty(yellow, "0.4");
        nodes[3].addProperty(vertexType, VERTEX_TYPE_TEST);
        nodes[3].addProperty(prior, "0.4, 4, 0.4");
        nodes[4].addProperty(red, "5");
        nodes[4].addProperty(blue, "5");
        nodes[4].addProperty(yellow, "0.5");
        nodes[4].addProperty(vertexType, VERTEX_TYPE_VALIDATE);
        nodes[4].addProperty(prior, "5, 5, 0.5");

        TitanEdge[] edges = new TitanEdge[12];
        edges[0] = nodes[0].addEdge(friend, nodes[1]);
        edges[0].setProperty(weight, "1.0");
        edges[1] = nodes[0].addEdge(friend, nodes[3]);
        edges[1].setProperty(weight, "3.0");
        edges[2] = nodes[1].addEdge(friend, nodes[0]);
        edges[2].setProperty(weight, "1.0");
        edges[3] = nodes[1].addEdge(friend, nodes[2]);
        edges[3].setProperty(weight, "2.0");
        edges[4] = nodes[1].addEdge(friend, nodes[3]);
        edges[4].setProperty(weight, "1.0");
        edges[5] = nodes[2].addEdge(friend, nodes[1]);
        edges[5].setProperty(weight, "2.0");
        edges[6] = nodes[2].addEdge(friend, nodes[4]);
        edges[6].setProperty(weight, "4.0");
        edges[7] = nodes[3].addEdge(friend, nodes[0]);
        edges[7].setProperty(weight, "3.0");
        edges[8] = nodes[3].addEdge(friend, nodes[1]);
        edges[8].setProperty(weight, "1.0");
        edges[9] = nodes[3].addEdge(friend, nodes[4]);
        edges[9].setProperty(weight, "4.0");
        edges[10] = nodes[4].addEdge(friend, nodes[3]);
        edges[10].setProperty(weight, "4.0");
        edges[11] = nodes[4].addEdge(friend, nodes[2]);
        edges[11].setProperty(weight, "4.0");

        tx.commit();
    }

    //@Ignore
    @Test
    public void PropertyGraph4LBPTest() throws Exception {
        giraphConf.setComputationClass(LoopyBeliefPropagationComputation.class);
        giraphConf.setMasterComputeClass(LoopyBeliefPropagationComputation.
            LoopyBeliefPropagationMasterCompute.class);
        giraphConf.setAggregatorWriterClass(LoopyBeliefPropagationComputation.
            LoopyBeliefPropagationAggregatorWriter.class);
        giraphConf.setVertexInputFormatClass(TitanHBaseVertexInputFormatPropertyGraph4LBP.class);
        giraphConf.setVertexOutputFormatClass(TitanVertexOutputFormatPropertyGraph4LBP.class);
        giraphConf.set("lbp.maxSupersteps", "5");

        INPUT_VERTEX_VALUE_PROPERTY_KEY_LIST.set(giraphConf, "red,blue,yellow");
        INPUT_EDGE_VALUE_PROPERTY_KEY_LIST.set(giraphConf, "weight");
        INPUT_EDGE_LABEL_LIST.set(giraphConf, "friend");
        VERTEX_TYPE_PROPERTY_KEY.set(giraphConf, "vertexType");
        VECTOR_VALUE.set(giraphConf, "false");
        OUTPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "lbp_red,lbp_blue,lbp_yellow");

        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0]);
        Assert.assertNotNull(results);
        for (String resultLine : results) {
            LOG.info(" got: " + resultLine);
        }

        //verify data is written to Titan
        startNewTransaction();
        long[] nid = new long[numVertices];
        assertTrue(tx.containsType("lbp_red"));
        assertTrue(tx.containsType("lbp_blue"));
        assertTrue(tx.containsType("lbp_yellow"));
        TitanKey result_blue = tx.getPropertyKey("lbp_blue");
        assertEquals(result_blue.getDataType(), String.class);
        assertEquals(result_blue.getName(), "lbp_blue");
        for (int i = 0; i < numVertices; i++) {
            nid[i] = nodes[i].getID();
            assertTrue(tx.containsVertex(nid[i]));
            nodes[i] = tx.getVertex(nid[i]);
            assertEquals(expectedLbpValues.get(nid[i])[1],
                Double.parseDouble(nodes[i].getProperty(result_blue).toString()), 0.01d);
        }
    }

    //@Ignore
    @Test
    public void PropertyGraph4LBPVectorTest() throws Exception {
        giraphConf.setComputationClass(LoopyBeliefPropagationComputation.class);
        giraphConf.setMasterComputeClass(LoopyBeliefPropagationComputation.
            LoopyBeliefPropagationMasterCompute.class);
        giraphConf.setAggregatorWriterClass(LoopyBeliefPropagationComputation.
            LoopyBeliefPropagationAggregatorWriter.class);
        giraphConf.setVertexInputFormatClass(TitanHBaseVertexInputFormatPropertyGraph4LBP.class);
        giraphConf.setVertexOutputFormatClass(TitanVertexOutputFormatPropertyGraph4LBP.class);
        giraphConf.set("lbp.maxSupersteps", "5");

        INPUT_VERTEX_VALUE_PROPERTY_KEY_LIST.set(giraphConf, "prior");
        INPUT_EDGE_VALUE_PROPERTY_KEY_LIST.set(giraphConf, "weight");
        INPUT_EDGE_LABEL_LIST.set(giraphConf, "friend");
        VERTEX_TYPE_PROPERTY_KEY.set(giraphConf, "vertexType");
        VECTOR_VALUE.set(giraphConf, "true");
        OUTPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "lbp_results");

        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0]);
        Assert.assertNotNull(results);
        for (String resultLine : results) {
            LOG.info(" got: " + resultLine);
        }

        startNewTransaction();
        //check keys are generated for Titan
        String keyName = "lbp_results";
        assertTrue(tx.containsType(keyName));
        TitanKey resultKey = tx.getPropertyKey(keyName);
        assertEquals(resultKey.getName(), keyName);
        assertEquals(resultKey.getDataType(), String.class);

        //verify data is written to Titan
        for (int i = 0; i < numVertices; i++) {
            long nid = nodes[i].getID();
            assertTrue(tx.containsVertex(nid));
            nodes[i] = tx.getVertex(nid);

            //split by comma
            String lbpResult = nodes[i].getProperty(resultKey).toString();
            String[] valueString = lbpResult.split(",");
            for (int j = 0; j < numKeys; j++) {
                 assertEquals(expectedLbpValues.get(nid)[j], Double.parseDouble(valueString[j]), 0.01d);
            }
        }
    }

    @Test
    public void PropertyGraph4LPTest() throws Exception {
        giraphConf.setComputationClass(LabelPropagationComputation.class);
        giraphConf.setMasterComputeClass(LabelPropagationComputation.
            LabelPropagationMasterCompute.class);
        giraphConf.setAggregatorWriterClass(LabelPropagationComputation.
            LabelPropagationAggregatorWriter.class);
        giraphConf.setVertexInputFormatClass(TitanHBaseVertexInputFormatPropertyGraph4LP.class);
        giraphConf.setVertexOutputFormatClass(TitanVertexOutputFormatPropertyGraph4LP.class);
        giraphConf.set("lp.maxSupersteps", "5");
        giraphConf.set("lp.convergenceThreshold", "0.1");
        giraphConf.set("lp.anchorThreshold", "0.8");
        giraphConf.set("lp.lambda", "0");
        giraphConf.set("lp.bidirectionalCheck", "true");

        INPUT_VERTEX_VALUE_PROPERTY_KEY_LIST.set(giraphConf, "red,blue,yellow");
        INPUT_EDGE_VALUE_PROPERTY_KEY_LIST.set(giraphConf, "weight");
        INPUT_EDGE_LABEL_LIST.set(giraphConf, "friend");
        VECTOR_VALUE.set(giraphConf, "false");
        OUTPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "lp_red,lp_blue,lp_yellow");

        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0]);
        Assert.assertNotNull(results);
        for (String resultLine : results) {
            LOG.info(" got: " + resultLine);
        }

        //verify data is written to Titan
        startNewTransaction();
        assertTrue(tx.containsType("lp_red"));
        assertTrue(tx.containsType("lp_blue"));
        assertTrue(tx.containsType("lp_yellow"));
        TitanKey result_blue = tx.getPropertyKey("lp_blue");
        assertEquals(result_blue.getDataType(), String.class);
        assertEquals(result_blue.getName(), "lp_blue");
        for (int i = 0; i < numVertices; i++) {
            long nid = nodes[i].getID();
            assertTrue(tx.containsVertex(nid));
            nodes[i] = tx.getVertex(nid);
            assertEquals(expectedLpValues.get(nid)[1],
                Double.parseDouble(nodes[i].getProperty(result_blue).toString()), 0.01d);
        }
    }

    @Test
    public void PropertyGraph4LPVectorTest() throws Exception {
        giraphConf.setComputationClass(LabelPropagationComputation.class);
        giraphConf.setMasterComputeClass(LabelPropagationComputation.
            LabelPropagationMasterCompute.class);
        giraphConf.setAggregatorWriterClass(LabelPropagationComputation.
            LabelPropagationAggregatorWriter.class);
        giraphConf.setVertexInputFormatClass(TitanHBaseVertexInputFormatPropertyGraph4LP.class);
        giraphConf.setVertexOutputFormatClass(TitanVertexOutputFormatPropertyGraph4LP.class);
        giraphConf.set("lp.maxSupersteps", "5");
        giraphConf.set("lp.convergenceThreshold", "0.1");
        giraphConf.set("lp.anchorThreshold", "0.8");
        giraphConf.set("lp.lambda", "0");
        giraphConf.set("lp.bidirectionalCheck", "true");

        INPUT_VERTEX_VALUE_PROPERTY_KEY_LIST.set(giraphConf, "prior");
        INPUT_EDGE_VALUE_PROPERTY_KEY_LIST.set(giraphConf, "weight");
        INPUT_EDGE_LABEL_LIST.set(giraphConf, "friend");
        VECTOR_VALUE.set(giraphConf, "true");
        OUTPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "lp_results");

        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0]);
        Assert.assertNotNull(results);
        for (String resultLine : results) {
            LOG.info(" got: " + resultLine);
        }

        //verify data is written to Titan
        startNewTransaction();
        //check keys are generated for Titan
        String keyName = "lp_results";
        assertTrue(tx.containsType(keyName));
        TitanKey resultKey = tx.getPropertyKey(keyName);
        assertEquals(resultKey.getName(), keyName);
        assertEquals(resultKey.getDataType(), String.class);

        //verify data is written to Titan
        for (int i = 0; i < numVertices; i++) {
            long nid = nodes[i].getID();
            assertTrue(tx.containsVertex(nid));
            nodes[i] = tx.getVertex(nid);

            //split by comma
            String lpResult = nodes[i].getProperty(resultKey).toString();
            String[] valueString = lpResult.split(",");
            for (int j = 0; j < numKeys; j++) {
                assertEquals(expectedLpValues.get(nid)[j], Double.parseDouble(valueString[j]), 0.01d);
            }
        }
    }
}
