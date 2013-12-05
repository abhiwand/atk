package com.intel.hadoop.graphbuilder.pipeline.output.titan;


import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.SerializedPropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.SerializedPropertyGraphElementLongTypeVids;
import com.intel.hadoop.graphbuilder.graphelements.SerializedPropertyGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.pipeline.TestMapReduceDriverUtils;
import com.intel.hadoop.graphbuilder.pipeline.input.BaseMapper;
import com.intel.hadoop.graphbuilder.pipeline.input.hbase.GBHTableConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.input.hbase.HBaseReaderMapper;
import com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.propertygraphelement.TitanMergedGraphElementWrite;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.SourceVertexKeyFunction;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseGraphBuildingRule;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseTokenizer;
import com.intel.hadoop.graphbuilder.types.LongType;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.transaction.StandardTransactionBuilder;
import com.thinkaurelius.titan.graphdb.vertices.StandardVertex;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.mockito.Matchers.any;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.support.membermodification.MemberMatcher.method;

@RunWith(PowerMockRunner.class)
@PrepareForTest(TitanElement.class)
public class TitanWriterMRChainTest extends TestMapReduceDriverUtils {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown(){
        super.tearDown();
    }

    @Test
    public void test() throws Exception {
        Result result = sampleData();

        Pair<ImmutableBytesWritable, Result> alice = new Pair<ImmutableBytesWritable, Result>(new ImmutableBytesWritable(Bytes.toBytes("row1")), sampleData());

        Pair<ImmutableBytesWritable, Result>[] pairs = new Pair[]{alice};

        PowerMockito.doReturn(titanGraph).when(spiedVerticesIntoTitanReducer).tribecaGraphFactoryOpen(any(Reducer.Context.class));

        com.tinkerpop.blueprints.Vertex  bpVertex = newStandardVertexMock();

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

        SerializedPropertyGraphElementLongTypeVids graphElement = new SerializedPropertyGraphElementLongTypeVids();


        //set up our matching vertex to test against
        com.intel.hadoop.graphbuilder.graphelements.Vertex<StringType> vertex = new com.intel.hadoop.graphbuilder.graphelements.Vertex<StringType>(new StringType("Alice"));
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

        //verifyPairSecond(run.get(1), "EDGE", edge, null);


        vertex = new com.intel.hadoop.graphbuilder.graphelements.Vertex<>(new StringType("GAO123"));
        vertex.setProperty("TitanID", new LongType(901L));

        //verifyPairSecond(run.get(2), "VERTEX", null, vertex);
    }

    @Test
    public void test2() throws Exception {

        printSampleData(sampleData());
        printSampleData(sampleDataBob());
        Pair<ImmutableBytesWritable, Result> alice = new Pair<ImmutableBytesWritable, Result>(new ImmutableBytesWritable(Bytes.toBytes("row1")), sampleData());
        Pair<ImmutableBytesWritable, Result> bob = new Pair<ImmutableBytesWritable, Result>(new ImmutableBytesWritable
                (Bytes.toBytes("row2")), sampleDataBob());
        Pair<ImmutableBytesWritable, Result>[] pairs = new Pair[]{alice, bob};

        PowerMockito.doReturn(titanGraph).when(spiedVerticesIntoTitanReducer).tribecaGraphFactoryOpen(any(Reducer.Context.class));

        com.tinkerpop.blueprints.Vertex  bpVertex = newStandardVertexMock();

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

        //verifyPairSecond(run.get(0), "VERTEX", null, vertex);

        vertex = new com.intel.hadoop.graphbuilder.graphelements.Vertex<StringType>(new StringType("Alice"));
        vertex.setProperty("cf:dept", new StringType("GAO123"));
        vertex.setProperty("cf:age", new StringType("43"));
        vertex.setProperty("TitanID", new LongType(900L));

        //verifyPairSecond(run.get(0), "VERTEX", null, vertex);

        //set up our matching edge to test against
        com.intel.hadoop.graphbuilder.graphelements.Edge<StringType> edge = new com.intel.hadoop.graphbuilder.graphelements.Edge<StringType>(new StringType("Alice"),
                new StringType("GAO123"),
                new StringType("worksAt"));
        edge.setProperty("srcTitanID", new LongType(900L));

        //verifyPairSecond(run.get(1), "EDGE", edge, null);


        vertex = new com.intel.hadoop.graphbuilder.graphelements.Vertex<>(new StringType("GAO123"));
        vertex.setProperty("TitanID", new LongType(901L));

        //verifyPairSecond(run.get(2), "VERTEX", null, vertex);
    }

    public com.tinkerpop.blueprints.Vertex newStandardVertexMock(){
        StandardTitanTx standardTitanTx = mock(StandardTitanTx.class);
        return mock(StandardVertex.class);
    }
}
