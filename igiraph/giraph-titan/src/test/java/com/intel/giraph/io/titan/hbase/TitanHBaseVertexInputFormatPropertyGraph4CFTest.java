package com.intel.giraph.io.titan.hbase;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.intel.giraph.algorithms.als.AlternatingLeastSquaresComputation;
import com.intel.giraph.algorithms.als.AlternatingLeastSquaresComputation.AlternatingLeastSquaresMasterCompute;
import com.intel.giraph.algorithms.als.AlternatingLeastSquaresComputation.SimpleAggregatorWriter;
import com.intel.giraph.io.formats.JsonPropertyGraph4CFOutputFormat;
import com.intel.giraph.io.EdgeDataWritable;
import com.intel.giraph.io.MessageDataWritable;
import com.intel.giraph.io.EdgeDataWritable.EdgeType;
import com.intel.giraph.io.VertexDataWritable;
import com.intel.giraph.io.VertexDataWritable.VertexType;
import com.intel.giraph.io.titan.GiraphToTitanGraphFactory;
import com.intel.giraph.io.titan.TitanTestGraph;
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
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.GIRAPH_TITAN;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_TABLENAME;

/**
 * Test TitanHBaseVertexInputFormatPropertyGraph4CF which loads vertex
 * with  <code>VertexData</code> vertex values and
 * <code>EdgeData</code> out-edge info.
 * <p/>
 * Each vertex follows this format:
 * (<vertex id>, <vertex valueVector>, <vertex property>,
 * ((<dest vertex id>, <edge value>, <edge property>), ...))
 * <p/>
 * Here is an example of left-side vertex, with vertex id 1,
 * vertex value 4,3 marked as "l", and two edges.
 * First edge has a destination vertex 2, edge value 2.1, marked as "tr".
 * Second edge has a destination vertex 3, edge value 0.7,marked as "va".
 * [1,[4,3],[l],[[2,2.1,[tr]],[3,0.7,[va]]]]
 */
public class TitanHBaseVertexInputFormatPropertyGraph4CFTest {
    public TitanTestGraph graph = null;
    public TitanTransaction tx = null;
    private GiraphConfiguration giraphConf;
    private ImmutableClassesGiraphConfiguration<LongWritable, VertexDataWritable, EdgeDataWritable> conf;

    @Before
    public void setUp() throws Exception {
        giraphConf = new GiraphConfiguration();
        giraphConf.setComputationClass(AlternatingLeastSquaresComputation.class);
        giraphConf.setMasterComputeClass(AlternatingLeastSquaresMasterCompute.class);
        giraphConf.setAggregatorWriterClass(SimpleAggregatorWriter.class);
        giraphConf.setVertexInputFormatClass(TitanHBaseVertexInputFormatPropertyGraph4CF.class);
        giraphConf.setVertexOutputFormatClass(JsonPropertyGraph4CFOutputFormat.class);
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
        //       INPUT_VERTEX_PROPERTY_KEY_LIST.set(giraphConf, "");
        INPUT_EDGE_PROPERTY_KEY_LIST.set(giraphConf, "weight");
        INPUT_EDGE_LABEL_LIST.set(giraphConf, "edge");
        VERTEX_TYPE_PROPERTY_KEY.set(giraphConf, "vertexType");
        EDGE_TYPE_PROPERTY_KEY.set(giraphConf, "edgeType");

        HBaseAdmin hbaseAdmin = new HBaseAdmin(giraphConf);
        if (hbaseAdmin.isTableAvailable(GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf))) {
            hbaseAdmin.disableTable(GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf));
            hbaseAdmin.deleteTable(GIRAPH_TITAN_STORAGE_TABLENAME.get(giraphConf));
        }

        conf = new ImmutableClassesGiraphConfiguration<LongWritable, VertexDataWritable, EdgeDataWritable>(
                giraphConf);

        BaseConfiguration baseConfig = GiraphToTitanGraphFactory.generateTitanConfiguration(conf,
                GIRAPH_TITAN.get(giraphConf));
        GraphDatabaseConfiguration titanConfig = new GraphDatabaseConfiguration(baseConfig);
        graph = new TitanTestGraph(titanConfig);
        tx = graph.newTransaction();

    }

    //@Ignore
    @Test
    public void TitanHBaseVertexInputFormatPropertyGraph4CFTest() throws Exception {
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

        TitanKey vertexType = tx.makeKey("vertexType").dataType(String.class).make();
        TitanKey edgeType = tx.makeKey("edgeType").dataType(String.class).make();
        TitanKey weight = tx.makeKey("weight").dataType(String.class).make();
        TitanLabel edge = tx.makeLabel("edge").make();

        TitanVertex n0 = tx.addVertex();
        n0.addProperty(vertexType, "l");
        TitanVertex n1 = tx.addVertex();
        n1.addProperty(vertexType, "l");
        TitanVertex n2 = tx.addVertex();
        n2.addProperty(vertexType, "r");
        TitanVertex n3 = tx.addVertex();
        n3.addProperty(vertexType, "r");
        TitanVertex n4 = tx.addVertex();
        n4.addProperty(vertexType, "r");

        TitanEdge e0 = n0.addEdge(edge, n2);
        e0.setProperty(weight, "1.0");
        e0.setProperty(edgeType, "tr");
        TitanEdge e1 = n0.addEdge(edge, n3);
        e1.setProperty(weight, "2.0");
        e1.setProperty(edgeType, "te");
        TitanEdge e2 = n1.addEdge(edge, n2);
        e2.setProperty(weight, "5.0");
        e2.setProperty(edgeType, "tr");
        TitanEdge e3 = n1.addEdge(edge, n4);
        e3.setProperty(weight, "3.0");
        e3.setProperty(edgeType, "va");
        TitanEdge e4 = n2.addEdge(edge, n0);
        e4.setProperty(weight, "1.0");
        e4.setProperty(edgeType, "tr");
        TitanEdge e5 = n2.addEdge(edge, n1);
        e5.setProperty(weight, "5.0");
        e5.setProperty(edgeType, "tr");
        TitanEdge e6 = n3.addEdge(edge, n0);
        e6.setProperty(weight, "2.0");
        e6.setProperty(edgeType, "te");
        TitanEdge e7 = n4.addEdge(edge, n1);
        e7.setProperty(weight, "3.0");
        e7.setProperty(edgeType, "va");

        tx.commit();


        Iterable<String> results = InternalVertexRunner.run(giraphConf, new String[0]);
        Assert.assertNotNull(results);
        Iterator<String> result = results.iterator();
        while (result.hasNext()) {
            String resultLine = result.next();
            System.out.println(" got: " + resultLine);
        }

        // verify results
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
    }

    @After
    public void done() throws IOException {
        close();
        System.out.println("***Done with TitanHBaseVertexInputFormatPropertyGraph4CFTest****");
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
                if (jsonVertex.length() != 4) {
                    throw new IllegalArgumentException("Wrong vertex output format!");
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
