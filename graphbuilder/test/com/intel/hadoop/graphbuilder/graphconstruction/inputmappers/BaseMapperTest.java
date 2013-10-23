package com.intel.hadoop.graphbuilder.graphconstruction.inputmappers;

import com.intel.hadoop.graphbuilder.demoapps.tabletotextgraph.BasicHBaseTokenizer;
import com.intel.hadoop.graphbuilder.graphconstruction.keyfunction.SourceVertexKeyFunction;
import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.*;
import static org.powermock.api.support.membermodification.MemberMatcher.method;

/**
 * unit test for basemapper with mockito and the PowerMock mockito plugin allows me to mock static methods,
 * private methods and variables.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(BaseMapper.class)
public class BaseMapperTest {
    Configuration conf = new Configuration();
    Mapper.Context mapperContextMock;
    //Mapper.Context spiedMapperContext;
    Logger loggerMock;
    BaseMapper baseMapper;
    BaseMapper spiedBaseMapper;
    PropertyGraphElement mapVal;

    @BeforeClass
    public static final void beforeClass(){
        //this is to suppress the log 4j errors during the tests
        //we should be moving to the new context logger
        System.setProperty("log4j.ignoreTCL","true");
    }

    @Before
    public void setUp() throws Exception {
        mapVal = new PropertyGraphElementStringTypeVids();

        mapperContextMock = PowerMockito.mock(Mapper.Context.class);

        conf = new Configuration();
        //this is the config if we were loading titan from hbase
        conf.set("GraphTokenizer", BasicHBaseTokenizer.class.getName());
        conf.set("KeyFunction", SourceVertexKeyFunction.class.getName());
        //sample command line paramaters
        //-d
        conf.set(GBHTableConfig.DECN_CONF_NAME, "cf:name,cf:dept,worksAt");
        //-v
        conf.set(GBHTableConfig.VCN_CONF_NAME, "cf:name=cf:age,cf:dept");

        //mapper context mocks
        Class valClass = PropertyGraphElementStringTypeVids.class;
        PowerMockito.when(mapperContextMock.getMapOutputValueClass()).thenReturn(valClass);

        //wil use later to verify log calls
        loggerMock = mock(Logger.class);

        //stub the new BaseMapper(context, conf, log)
        baseMapper = new BaseMapper(mapperContextMock, conf, loggerMock);
        spiedBaseMapper = spy(baseMapper);
        PowerMockito.whenNew(BaseMapper.class).withAnyArguments().thenReturn(spiedBaseMapper);

        spiedBaseMapper.setValClass(PropertyGraphElementStringTypeVids.class);
        //ignore any other calls to set the valClass
        doNothing().when(spiedBaseMapper).setValClass(any(Class.class));

        spiedBaseMapper.setMapVal(PropertyGraphElementStringTypeVids.class.newInstance());
        //ignore any other calls to set map val
        doNothing().when(spiedBaseMapper).setMapVal(any(PropertyGraphElement.class));
    }

    @After
    public void tearDown() {
        mapVal = null;
        mapperContextMock = null;
        conf = null;
        loggerMock = null;
        baseMapper = null;
        spiedBaseMapper = null;
    }


    @Test
    public final void verify_edge_erro_counter_increment() {
        //sample broken edge
        Edge<StringType> brokenEdge = new Edge<StringType>(new StringType("Alice"), new StringType(null),
                new StringType("worksAt"));


        mapVal.init(PropertyGraphElement.GraphElementType.EDGE, brokenEdge);

        org.apache.hadoop.mapreduce.Counter EDGE = mock(org.apache.hadoop.mapreduce.Counter.class);

        //mock our context.getCounter() call so context.getCounter().increment(1) don't throw null
        PowerMockito.when(mapperContextMock.getCounter(BaseMapper.getEdgeWriteErrorCounter()))
                .thenReturn(EDGE);

        spiedBaseMapper.incrementErrorCounter(mapperContextMock, mapVal);

        //verify context.getCounter(Counter).increment(1) Edge error Counter got called
        Mockito.verify(EDGE).increment(1);
    }

    @Test
    public final void verify_vertex_error_counter_increment() {
        Vertex<StringType> brokenVertex = new Vertex<StringType>(new StringType("tese"));

        mapVal.init(PropertyGraphElement.GraphElementType.VERTEX, brokenVertex);

        org.apache.hadoop.mapreduce.Counter VERTEX = mock(org.apache.hadoop.mapreduce.Counter.class);

        //mock our context.getCounter() call so context.getCounter().increment(1) don't throw null
        PowerMockito.when(mapperContextMock.getCounter(BaseMapper.getVertexWriteErrorCounter()))
                .thenReturn(VERTEX);

        spiedBaseMapper.incrementErrorCounter(mapperContextMock, mapVal);

        //verify context.getCounter(Counter).increment(1) vertex error Counter got called
        Mockito.verify(VERTEX).increment(1);
    }

    @Test
    public final void verify_contextWrite_logs_error_and_incrementErrorCounter_on_IOException()
            throws IOException, InterruptedException {
        //mock our exception call
        PowerMockito.doThrow(new IOException()).
                when(mapperContextMock).write(any(IntWritable.class),
                any(PropertyGraphElement.class));

        //this will stop the real call to incrementErrorCounter from happening
        //but the mock will still log the  incrementErrorCounter for later verification
        PowerMockito.doNothing().when(spiedBaseMapper).incrementErrorCounter(any(Mapper.Context.class),
                any(PropertyGraphElement.class));

        //any dummy vertex to call context write
        Vertex<StringType> dummyVertex = new Vertex<StringType>(new StringType("test"));
        mapVal.init(PropertyGraphElement.GraphElementType.VERTEX, dummyVertex);

        spiedBaseMapper.contextWrite(mapperContextMock, new IntWritable(1), mapVal);

        verify(spiedBaseMapper).incrementErrorCounter(any(Mapper.Context.class),
                any(PropertyGraphElement.class));
        verify(loggerMock).error(any(Object.class), any(IOException.class));
    }

    @Test
    public final void verify_contextWrite_logs_error_and_incrementErrorCounter_on_InterruptedException()
            throws IOException, InterruptedException {

        PowerMockito.doThrow(new InterruptedException()).
                when(mapperContextMock).write(any(IntWritable.class),
                any(PropertyGraphElement.class));

        PowerMockito.doNothing().when(spiedBaseMapper).incrementErrorCounter(any(Mapper.Context.class),
                any(PropertyGraphElement.class));

        Vertex<StringType> dummyVertex = new Vertex<StringType>(new StringType("test"));
        mapVal.init(PropertyGraphElement.GraphElementType.VERTEX, dummyVertex);

        spiedBaseMapper.contextWrite(mapperContextMock, new IntWritable(1), mapVal);

        verify(spiedBaseMapper).incrementErrorCounter(any(Mapper.Context.class),
                any(PropertyGraphElement.class));
        verify(loggerMock).error(any(Object.class), any(IOException.class));
    }


    @Test
    public final void verify_setUp_calls_fatal_on_InstantiationException() throws Exception {
        //power mockito setting up a mock to a possible private variable
        //in this case  initializeTokenizer is protected but should also work if it's private
        PowerMockito.doThrow(new InstantiationException()).when(spiedBaseMapper,
                method(BaseMapper.class, "initializeTokenizer",
                        Configuration.class))
                .withArguments(conf);

        //ignore the to systemExit because it will kill all our test due to the System.exit
        PowerMockito.doNothing().when(spiedBaseMapper, method(BaseMapper.class, "systemExit", null))
                .withNoArguments();


        spiedBaseMapper.setUp(conf);


        verify(loggerMock).fatal(any(String.class), any(InstantiationException.class));
    }

    @Test
    public final void verify_setUp_calls_fatal_on_IllegalAccessException() throws Exception {
        //power mockito setting up a mock to a possible private variable
        //in this case  initializeTokenizer is protected but should also work if it's private
        PowerMockito.doThrow(new IllegalAccessException()).when(spiedBaseMapper,
                method(BaseMapper.class, "initializeTokenizer",
                        Configuration.class))
                .withArguments(conf);

        //ignore the to systemExit because it will kill all our test due to the System.exit
        PowerMockito.doNothing().when(spiedBaseMapper, method(BaseMapper.class, "systemExit", null))
                .withNoArguments();

        spiedBaseMapper.setUp(conf);

        verify(loggerMock).fatal(any(String.class), any(IllegalAccessException.class));
    }

    @Test
    public final void verify_setUp_calls_fatal_on_ClassNotFoundException() throws Exception {
        //power mockito setting up a mock to a possible private variable
        //in this case  initializeTokenizer is protected but should also work if it's private
        PowerMockito.doThrow(new ClassNotFoundException()).when(spiedBaseMapper,
                method(BaseMapper.class, "initializeTokenizer",
                        Configuration.class))
                .withArguments(conf);

        //ignore the to systemExit because it will kill all our test due to the System.exit
        PowerMockito.doNothing().when(spiedBaseMapper, method(BaseMapper.class, "systemExit", null))
                .withNoArguments();

        spiedBaseMapper.setUp(conf);

        verify(loggerMock).fatal(any(String.class), any(ClassNotFoundException.class));
    }
}
