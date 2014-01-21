package com.intel.giraph.io.titan;

import com.intel.giraph.algorithms.lbp.LoopyBeliefPropagationComputation;
import com.intel.giraph.io.titan.hbase.TitanHBaseVertexInputFormatLongTwoVectorDoubleTwoVector;
import com.intel.mahout.math.DoubleWithTwoVectorWritable;
import com.intel.mahout.math.TwoVectorWritable;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanVertex;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.io.LongWritable;
import org.junit.Assert;
import org.junit.Test;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_LABEL_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_EDGE_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.INPUT_VERTEX_PROPERTY_KEY_LIST;
import static com.intel.giraph.io.titan.common.GiraphTitanConstants.OUTPUT_VERTEX_PROPERTY_KEY_LIST;
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
public class TitanVertexFormatLongTwoVectorDoubleTwoVectorInLongIDVectorValueOutTest 
    extends TitanTestBase<LongWritable, TwoVectorWritable, DoubleWithTwoVectorWritable> {

    @Override
    protected void configure() throws Exception {
        giraphConf.setComputationClass(LoopyBeliefPropagationComputation.class);
        giraphConf.setVertexInputFormatClass(TitanHBaseVertexInputFormatLongTwoVectorDoubleTwoVector.class);
        giraphConf.setVertexOutputFormatClass(TitanVertexOutputFormatLongIDVectorValue.class);
        giraphConf.set("lbp.maxSupersteps", "5");

        INPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "red,blue,yellow");
        INPUT_EDGE_PROPERTY_KEY_LIST.set(giraphConf, "weight");
        INPUT_EDGE_LABEL_LIST.set(giraphConf, "friend");
        OUTPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "result_red,result_blue,result_yellow");

    }

    @Test
    public void VertexFormatLongTwoVectorDoubleTwoVectorInLongIDVectorValueOutTest() throws Exception {
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

        int numVertices = 5;
        TitanVertex[] nodes = new TitanVertex[numVertices];
        for (int i = 0; i < numVertices; i++) {
            nodes[i] = tx.addVertex();
        }

        nodes[0].addProperty(red, "1");
        nodes[0].addProperty(blue, "0.1");
        nodes[0].addProperty(yellow, "0.1");
        nodes[1].addProperty(red, "0.2");
        nodes[1].addProperty(blue, "2");
        nodes[1].addProperty(yellow, "2");
        nodes[2].addProperty(red, "0.3");
        nodes[2].addProperty(blue, "0.3");
        nodes[2].addProperty(yellow, "3");
        nodes[3].addProperty(red, "0.4");
        nodes[3].addProperty(blue, "4");
        nodes[3].addProperty(yellow, "0.4");
        nodes[4].addProperty(red, "5");
        nodes[4].addProperty(blue, "5");
        nodes[4].addProperty(yellow, "0.5");

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

        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0], new String[0]);
        Assert.assertNotNull(results);

        //verify data is written to Titan
        startNewTransaction();
        long nid = nodes[0].getID();
        assertTrue(tx.containsVertex(nid));
        assertTrue(tx.containsType("result_blue"));
        TitanKey result_blue = tx.getPropertyKey("result_blue");
        assertEquals(result_blue.getDataType(), String.class);
        assertEquals(result_blue.getName(), "result_blue");
        nodes[0] = tx.getVertex(nid);
        assertEquals(1.0, Double.parseDouble(nodes[0].getProperty(result_blue).toString()), 0.05d);

        for (String resultLine : results) {
            LOG.info(" got: " + resultLine);
        }
    }
}
