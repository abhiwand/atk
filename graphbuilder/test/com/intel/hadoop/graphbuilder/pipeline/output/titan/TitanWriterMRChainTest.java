package com.intel.hadoop.graphbuilder.pipeline.output.titan;


import com.intel.hadoop.graphbuilder.graphelements.SerializedPropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.SerializedPropertyGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.pipeline.TestMapReduceDriverUtils;
import com.intel.hadoop.graphbuilder.pipeline.input.BaseMapper;
import com.intel.hadoop.graphbuilder.pipeline.input.hbase.HBaseReaderMapper;
import com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.propertygraphelement.TitanMergedGraphElementWrite;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.SourceVertexKeyFunction;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseGraphBuildingRule;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseTokenizer;
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

import java.util.List;

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
        ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes("row1"));

        PowerMockito.doReturn(900L).when(spiedTitanMergedGraphElementWrite).getVertexId(any(com.tinkerpop.blueprints.Vertex.class));

        PowerMockito.doReturn(titanGraph).when(spiedVerticesIntoTitanReducer).tribecaGraphFactoryOpen(any(Reducer.Context.class));

        com.tinkerpop.blueprints.Vertex  bpVertex = newStandardVertexMock();

        PowerMockito.doReturn(bpVertex).when(titanGraph).addVertex(null);

        List<Pair<IntWritable,SerializedPropertyGraphElement>> run = runVertexHbaseMR(key, result);

        System.out.println("done: " + run.size());
    }

    public com.tinkerpop.blueprints.Vertex newStandardVertexMock(){
        StandardTitanTx standardTitanTx = mock(StandardTitanTx.class);
        return mock(StandardVertex.class);
    }
}
