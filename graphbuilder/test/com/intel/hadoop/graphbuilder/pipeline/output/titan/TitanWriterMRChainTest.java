package com.intel.hadoop.graphbuilder.pipeline.output.titan;


import com.intel.hadoop.graphbuilder.graphelements.SerializedPropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.SerializedPropertyGraphElementLongTypeVids;
import com.intel.hadoop.graphbuilder.pipeline.TestMapReduceDriverUtils;
import com.intel.hadoop.graphbuilder.pipeline.input.hbase.GBHTableConfiguration;
import com.intel.hadoop.graphbuilder.types.LongType;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.thinkaurelius.titan.core.TitanElement;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Matchers.any;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.support.membermodification.MemberMatcher.method;

@RunWith(PowerMockRunner.class)
@PrepareForTest(TitanElement.class)
public class TitanWriterMRChainTest extends TestMapReduceDriverUtils {
    SerializedPropertyGraphElementLongTypeVids graphElement;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        graphElement = new SerializedPropertyGraphElementLongTypeVids();
    }

    @After
    public void tearDown(){
        super.tearDown();
        graphElement = null;
    }

    @Test
    public void test_hbase_to_vertices_to_titan_MR() throws Exception {
        Result result = sampleData();

        Pair<ImmutableBytesWritable, Result> alice = new Pair<ImmutableBytesWritable, Result>(new ImmutableBytesWritable(Bytes.toBytes("row1")), sampleData());

        Pair<ImmutableBytesWritable, Result>[] pairs = new Pair[]{alice};

        PowerMockito.doReturn(titanGraph).when(spiedVerticesIntoTitanReducer).tribecaGraphFactoryOpen(any(Reducer.Context.class));

        com.tinkerpop.blueprints.Vertex  bpVertex = vertexMock();

        PowerMockito.doReturn(bpVertex).when(titanGraph).addVertex(null);

        PowerMockito.doReturn(900L).doReturn(901L).doReturn(902L).when(spiedTitanMergedGraphElementWrite).getVertexId
                (any(com.tinkerpop.blueprints.Vertex.class));

        List<Pair<IntWritable,SerializedPropertyGraphElement>> run =
                runVertexHbaseMR(pairs);

        assertTrue("check the number of writables", run.size() == 3);
        assertEquals("check the number of counters", gbMapReduceDriver.getCounters().countCounters(), 4);
        assertEquals("check HTABLE_COLS_READ counter", gbMapReduceDriver.getCounters().findCounter
                (GBHTableConfiguration.Counters.HTABLE_COLS_READ).getValue(), 5);
        assertEquals("check HTABLE_ROWS_READ counter", gbMapReduceDriver.getCounters().findCounter
                (GBHTableConfiguration.Counters.HTABLE_ROWS_READ).getValue(), 1);
        assertEquals("check NUM_EDGES counter", gbMapReduceDriver.getCounters().findCounter
                (verticesIntoTitanReducer.getEdgeCounter()).getValue(), 1);
        assertEquals("check NUM_VERTICES counter", gbMapReduceDriver.getCounters().findCounter
                (verticesIntoTitanReducer.getVertexCounter()).getValue(), 2);

        //set up our matching vertex to test against
        com.intel.hadoop.graphbuilder.graphelements.Vertex<StringType> vertex =
                new com.intel.hadoop.graphbuilder.graphelements.Vertex<StringType>(new StringType("Alice"));
        vertex.setProperty("cf:dept", new StringType("GAO123"));
        vertex.setProperty("cf:age", new StringType("43"));
        vertex.setProperty("TitanID", new LongType(900L));

        graphElement.init(vertex);

        verifyPairSecond(run.get(0), graphElement);

        //set up our matching edge to test against
        com.intel.hadoop.graphbuilder.graphelements.Edge<StringType> edge = new com.intel.hadoop.graphbuilder.graphelements.Edge<StringType>(new StringType("Alice"),
                new StringType("GAO123"),
                new StringType("worksAt"));
        edge.setProperty("srcTitanID", new LongType(900L));

        graphElement.init(edge);

        verifyPairSecond(run.get(1), graphElement);


        vertex = new com.intel.hadoop.graphbuilder.graphelements.Vertex<>(new StringType("GAO123"));
        vertex.setProperty("TitanID", new LongType(901L));

        graphElement.init(vertex);

        verifyPairSecond(run.get(2), graphElement);
    }

    @Test
    public void test_hbase_vertices_to_titan_MR_two_hbase_results() throws Exception {

        printSampleData(sampleData());
        printSampleData(sampleDataBob());
        Pair<ImmutableBytesWritable, Result> alice = new Pair<ImmutableBytesWritable, Result>
                (new ImmutableBytesWritable(Bytes.toBytes("row1")), sampleData());
        Pair<ImmutableBytesWritable, Result> bob = new Pair<ImmutableBytesWritable, Result>
                (new ImmutableBytesWritable(Bytes.toBytes("row2")), sampleDataBob());
        Pair<ImmutableBytesWritable, Result>[] pairs = new Pair[]{alice, bob};

        PowerMockito.doReturn(titanGraph).when(spiedVerticesIntoTitanReducer).tribecaGraphFactoryOpen(any(Reducer.Context.class));

        com.tinkerpop.blueprints.Vertex  bpVertex = vertexMock();

        PowerMockito.doReturn(bpVertex).when(titanGraph).addVertex(null);

        PowerMockito.doReturn(900L).doReturn(901L).doReturn(902L).doReturn(903L).doReturn(904L)
                .when(spiedTitanMergedGraphElementWrite).getVertexId(any(com.tinkerpop.blueprints.Vertex.class));

        List<Pair<IntWritable,SerializedPropertyGraphElement>> run = runVertexHbaseMR(pairs);

        assertTrue("check the number of writables", run.size() == 6);
        assertEquals("check the number of counters", gbMapReduceDriver.getCounters().countCounters(), 4);
        assertEquals("check HTABLE_COLS_READ counter", gbMapReduceDriver.getCounters().findCounter
                (GBHTableConfiguration.Counters.HTABLE_COLS_READ).getValue(), 10);
        assertEquals("check HTABLE_ROWS_READ counter", gbMapReduceDriver.getCounters().findCounter
                (GBHTableConfiguration.Counters.HTABLE_ROWS_READ).getValue(), 2);
        assertEquals("check NUM_EDGES counter", gbMapReduceDriver.getCounters().findCounter
                (verticesIntoTitanReducer.getEdgeCounter()).getValue(), 2);
        assertEquals("check NUM_VERTICES counter", gbMapReduceDriver.getCounters().findCounter
                (verticesIntoTitanReducer.getVertexCounter()).getValue(), 4);

        //set up our matching vertex to test against
        com.intel.hadoop.graphbuilder.graphelements.Vertex<StringType> vertex =
                new com.intel.hadoop.graphbuilder.graphelements.Vertex<StringType>(new StringType("INTELLABS"));
        vertex.setProperty("TitanID", new LongType(900L));

        graphElement.init(vertex);

        verifyPairSecond(run.get(0), graphElement);

        vertex = new com.intel.hadoop.graphbuilder.graphelements.Vertex<StringType>(new StringType("Bob"));
        vertex.setProperty("cf:dept", new StringType("INTELLABS"));
        vertex.setProperty("cf:age", new StringType("45"));
        vertex.setProperty("TitanID", new LongType(901L));

        graphElement.init(vertex);

        verifyPairSecond(run.get(1), graphElement);

        //set up our matching edge to test against
        com.intel.hadoop.graphbuilder.graphelements.Edge<StringType> edge =
                new com.intel.hadoop.graphbuilder.graphelements.Edge<StringType>(new StringType("Bob"),
                new StringType("INTELLABS"),
                new StringType("worksAt"));
        edge.setProperty("srcTitanID", new LongType(901L));

        graphElement.init(edge);

        verifyPairSecond(run.get(2), graphElement);


        vertex = new com.intel.hadoop.graphbuilder.graphelements.Vertex<StringType>(new StringType("Alice"));
        vertex.setProperty("cf:dept", new StringType("GAO123"));
        vertex.setProperty("cf:age", new StringType("43"));
        vertex.setProperty("TitanID", new LongType(902L));

        graphElement.init(vertex);

        verifyPairSecond(run.get(3), graphElement);

        edge = new com.intel.hadoop.graphbuilder.graphelements.Edge<StringType>(new StringType("Alice"),
                new StringType("GAO123"),
                new StringType("worksAt"));
        edge.setProperty("srcTitanID", new LongType(902L));

        graphElement.init(edge);

        verifyPairSecond(run.get(4), graphElement);


        vertex = new com.intel.hadoop.graphbuilder.graphelements.Vertex<>(new StringType("GAO123"));
        vertex.setProperty("TitanID", new LongType(903L));

        graphElement.init(vertex);

        verifyPairSecond(run.get(5), graphElement);
    }

    @Test
    public void test_hbase_edge_to_titan_MR(){

    }

}
