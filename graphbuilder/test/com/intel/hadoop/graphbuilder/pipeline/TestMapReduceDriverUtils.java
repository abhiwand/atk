package com.intel.hadoop.graphbuilder.pipeline;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.SerializedPropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.SerializedPropertyGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.pipeline.input.BaseMapper;
import com.intel.hadoop.graphbuilder.pipeline.input.hbase.HBaseReaderMapper;
import com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.propertygraphelement.PropertyGraphElements;
import com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.propertygraphelement.TitanMergedGraphElementWrite;
import com.intel.hadoop.graphbuilder.pipeline.output.titan.VerticesIntoTitanReducer;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.SourceVertexKeyFunction;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseGraphBuildingRule;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseTokenizer;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.commons.collections.iterators.ArrayIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.PipelineMapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 * An abstract class that can be extended that will hold most of the testing setup need for GB.
 */

@RunWith(PowerMockRunner.class)
@PrepareForTest({PropertyGraphElements.class, VerticesIntoTitanReducer.class, HBaseReaderMapper.class})
public abstract class TestMapReduceDriverUtils {
    protected Configuration conf;
    protected Logger loggerMock;

    protected Mapper.Context mapContext;
    protected HBaseReaderMapper hBaseReaderMapper;
    protected HBaseReaderMapper spiedHBaseReaderMapper;
    protected MapDriver<ImmutableBytesWritable, Result, IntWritable, SerializedPropertyGraphElement> mapDriver;


    protected Reducer.Context reduceContext;
    protected VerticesIntoTitanReducer verticesIntoTitanReducer;
    protected VerticesIntoTitanReducer spiedVerticesIntoTitanReducer;
    protected ReduceDriver<IntWritable, SerializedPropertyGraphElement, IntWritable, SerializedPropertyGraphElement> reduceDriver;

    protected MapReduceDriver<ImmutableBytesWritable, Result, IntWritable, SerializedPropertyGraphElement, IntWritable, SerializedPropertyGraphElement> mapReduceDriver;
    protected GBMapReduceDriver<ImmutableBytesWritable, Result, IntWritable, SerializedPropertyGraphElement, IntWritable, SerializedPropertyGraphElement> gbMapReduceDriver;
    protected PipelineMapReduceDriver<ImmutableBytesWritable, Result, IntWritable, SerializedPropertyGraphElement> pipelineMapReduceDriver;

    protected Mapper.Context mapperContextMock;
    protected Reducer.Context reducerContextMock;
    protected BaseMapper baseMapper;
    protected BaseMapper spiedBaseMapper;
    protected PropertyGraphElements propertyGraphElements;
    protected PropertyGraphElements spiedPropertyGraphElements;
    protected TitanMergedGraphElementWrite titanMergedGraphElementWrite;
    protected TitanMergedGraphElementWrite spiedTitanMergedGraphElementWrite;
    protected TitanGraph titanGraph;

    Class klass = SerializedPropertyGraphElementStringTypeVids.class;
    Class valClass = SerializedPropertyGraphElementStringTypeVids.class;

    @BeforeClass
    public static void beforeClass(){
        //this is to suppress the log 4j errors during the tests
        //we should be moving to the new context logger
        System.setProperty("log4j.ignoreTCL","true");
    }

    @Before
    public void setUp() throws Exception {
        init();
    }

    @After
    public void tearDown(){
        conf = null;
        loggerMock = null;

        hBaseReaderMapper = null;
        spiedHBaseReaderMapper = null;
        mapDriver = null;

        reduceDriver = null;
        verticesIntoTitanReducer = null;
        spiedVerticesIntoTitanReducer = null;

        mapReduceDriver = null;
        mapperContextMock = null;
        reducerContextMock = null;
        baseMapper = null;
        spiedBaseMapper = null;

        propertyGraphElements = null;
        spiedPropertyGraphElements = null;
        titanMergedGraphElementWrite = null;
        spiedTitanMergedGraphElementWrite = null;
        titanGraph = null;
    }

    protected VerticesIntoTitanReducer newVerticesIntoTitanReducer(){
        if(spiedVerticesIntoTitanReducer == null){
            verticesIntoTitanReducer = new VerticesIntoTitanReducer();
            spiedVerticesIntoTitanReducer = spy(verticesIntoTitanReducer);
        }

        return spiedVerticesIntoTitanReducer;
    }

    protected HBaseReaderMapper newHBaseReaderMapper(){
        if(spiedHBaseReaderMapper == null){
            hBaseReaderMapper = new HBaseReaderMapper();
            spiedHBaseReaderMapper = spy(hBaseReaderMapper);
        }
        return spiedHBaseReaderMapper;
    }

    protected ReduceDriver newVertexReducer(){
        newVerticesIntoTitanReducer();

        reduceDriver = ReduceDriver.newReduceDriver(spiedVerticesIntoTitanReducer);

        reduceContext = reduceDriver.getContext();

        PowerMockito.when(reduceContext.getMapOutputValueClass()).thenReturn(klass);

        return reduceDriver;
    }

    protected MapDriver newHbaseMapper(){
        newHBaseReaderMapper();

        mapDriver = MapDriver.newMapDriver(spiedHBaseReaderMapper);

        mapContext =  mapDriver.getContext();

        PowerMockito.when(mapContext.getMapOutputValueClass()).thenReturn(klass);

        return mapDriver;
    }

    protected MapReduceDriver newVertexHbaseMR(){
        newVerticesIntoTitanReducer();
        newHBaseReaderMapper();
        newConfiguration();

        gbMapReduceDriver = new GBMapReduceDriver(mapDriver, reduceDriver);

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(spiedHBaseReaderMapper, spiedVerticesIntoTitanReducer);
        mapReduceDriver.setOutputSerializationConfiguration(conf);
        return mapReduceDriver;
    }

    protected List<Pair<IntWritable,SerializedPropertyGraphElement>> runVertexHbaseMR(
            Pair<ImmutableBytesWritable,Result>[] pairs) throws IOException {

        gbMapReduceDriver = new GBMapReduceDriver(mapDriver, reduceDriver);

        gbMapReduceDriver.withConfiguration(conf);

        for(Pair<ImmutableBytesWritable, Result> kv: pairs){
            gbMapReduceDriver.withInput(kv);
        }

        List<Pair<IntWritable, SerializedPropertyGraphElement >> ran = gbMapReduceDriver.run();
        return ran;
    }


    protected Logger newLoggerMock(){
        if( loggerMock == null){
            loggerMock = mock(Logger.class);
            Whitebox.setInternalState(HBaseReaderMapper.class, "LOG", loggerMock);
        }
        return loggerMock;
    }

    protected Configuration newConfiguration(){
        if(conf == null){
            conf = new Configuration();
            conf.set("GraphTokenizer", HBaseTokenizer.class.getName());
            conf.set("KeyFunction", SourceVertexKeyFunction.class.getName());

        }
        return conf;
    }

    protected void newGraphBuildingRules(){
        //sample vertex and edge generation rules
        String[] vertexRules = new String[1];
        vertexRules[0] = "cf:name=cf:age,cf:dept";
        HBaseGraphBuildingRule.packVertexRulesIntoConfiguration(conf, vertexRules);

        String[] edgeRules = new String[0];
        HBaseGraphBuildingRule.packEdgeRulesIntoConfiguration(conf, edgeRules);

        String[] directedEdgeRules = new String[1];
        directedEdgeRules[0] =  "cf:name,cf:dept,worksAt";
        HBaseGraphBuildingRule.packDirectedEdgeRulesIntoConfiguration(conf, directedEdgeRules);
    }

    protected Mapper.Context newMapperContext(){
        if(mapperContextMock == null){
            mapperContextMock = mock(Mapper.Context.class);
            valClass = SerializedPropertyGraphElementStringTypeVids.class;
            PowerMockito.when(mapperContextMock.getMapOutputValueClass()).thenReturn(valClass);
        }
        return mapperContextMock;
    }

    protected Reducer.Context newReducerContext(){
        if(reducerContextMock == null){
            reducerContextMock = mock(Reducer.Context.class);
            valClass = SerializedPropertyGraphElementStringTypeVids.class;
            PowerMockito.when(reducerContextMock.getMapOutputValueClass()).thenReturn(valClass);
        }
        return reducerContextMock;
    }

    protected BaseMapper newBaseMapper() throws Exception {
        if(spiedBaseMapper == null){
            baseMapper = new BaseMapper(mapperContextMock, conf, loggerMock);
            spiedBaseMapper = spy(baseMapper);
            PowerMockito.whenNew(BaseMapper.class).withAnyArguments().thenReturn(spiedBaseMapper);
        }
        return spiedBaseMapper;
    }

    protected PropertyGraphElements newPropertyGraphElements() throws Exception {
        newVerticesIntoTitanReducer();

        if(propertyGraphElements == null){
            titanMergedGraphElementWrite = new TitanMergedGraphElementWrite();
            spiedTitanMergedGraphElementWrite = spy(titanMergedGraphElementWrite);

            propertyGraphElements = new PropertyGraphElements(spiedTitanMergedGraphElementWrite, null, null,
                    reduceContext, titanGraph,
                    (SerializedPropertyGraphElement)valClass.newInstance(), verticesIntoTitanReducer.getEdgeCounter(),
                    verticesIntoTitanReducer.getVertexCounter() );

            spiedPropertyGraphElements = spy(propertyGraphElements);

            PowerMockito.whenNew(PropertyGraphElements.class).withAnyArguments().thenReturn(spiedPropertyGraphElements);

            //verticesIntoTitanReducer.setPropertyGraphElements(spiedPropertyGraphElements);
        }
        return spiedPropertyGraphElements;
    }

    protected Object newMock(Object inst, Class klass){
        if(inst == null){
            inst = mock(klass);
        }
         return inst;
    }

    protected TitanGraph newTitanGraphMock(){
        titanGraph = (TitanGraph) newMock(titanGraph, TitanGraph.class);
        return titanGraph;
    }

    protected void init() throws Exception {
        newTitanGraphMock();



        newVertexReducer();

        newPropertyGraphElements();

        newHbaseMapper();
        newVertexHbaseMR();
        newLoggerMock();
        newConfiguration();
        newGraphBuildingRules();
        newMapperContext();
        newReducerContext();
        newBaseMapper();

    }




    /**
     * setup our sample data for our Results column list
     *
     * @return Result column list
     */
    public static final Result sampleData() {
        ArrayList<KeyValue> list = new ArrayList<KeyValue>();
        //alice
        list.add(newKeyValue("row1", "cf", "age", "43", "1381447886360"));
        list.add(newKeyValue("row1", "cf", "dept", "GAO123", "1381447886375"));
        list.add(newKeyValue("row1", "cf", "id", "0001", "1381447886305"));
        list.add(newKeyValue("row1", "cf", "manager", "Zed", "1381447886386"));
        list.add(newKeyValue("row1", "cf", "name", "Alice", "1381447886328"));
        list.add(newKeyValue("row1", "cf", "underManager", "5yrs", "1381447886400"));

        return new Result(list);
    }

    /**
     * setup our sample data for our Results column list
     *
     * @return Result column list
     */
    public static final Result sampleDataBob() {
        ArrayList<KeyValue> list = new ArrayList<KeyValue>();
        //alice
        list.add(newKeyValue("row2", "cf", "age", "45", "1381447886360"));
        list.add(newKeyValue("row2", "cf", "dept", "INTELLABS", "1381447886375"));
        list.add(newKeyValue("row2", "cf", "id", "00002", "1381447886305"));
        list.add(newKeyValue("row2", "cf", "manager", "Zed", "1381447886386"));
        list.add(newKeyValue("row2", "cf", "name", "Bob", "1381447886328"));
        list.add(newKeyValue("row2", "cf", "underManager", "1yrs", "1381447886400"));

        return new Result(list);
    }

    /**
     * help debug the wierdness with Hbase.Client.Results. prints all the sample data to see if any values are coming
     * back null
     *
     * @throws UnsupportedEncodingException
     */
    public final void printSampleData(Result result) throws UnsupportedEncodingException {
        printSampleRow(result, "cf", "age");
        printSampleRow(result, "cf", "dept");
        printSampleRow(result, "cf", "id");
        printSampleRow(result, "cf", "manager");
        printSampleRow(result, "cf", "name");
        printSampleRow(result, "cf", "underManager");
    }

    public final void printSampleRow(Result result, String cf, String qualifier) throws UnsupportedEncodingException {
        System.out.println((cf + ":" + qualifier + " value: " +
                Bytes.toString(result.getValue(cf.getBytes(HConstants.UTF8_ENCODING),
                        qualifier.getBytes(HConstants.UTF8_ENCODING)))));
    }

    /**
     * small wrapper method to help us create KeyValues. All the input values are strings that will be converted to
     * bytes with String.getBytes()
     *
     * @param row       the row key string
     * @param cf        the column family string
     * @param qualifier the qualifier string
     * @param value     the value string
     * @param time      the timestamp for the row as a string
     * @return
     */
    public static final KeyValue newKeyValue(String row, String cf, String qualifier, String value, String time) {
        KeyValue k1;
        try {
            k1 = new KeyValue(row.getBytes(HConstants.UTF8_ENCODING),
                    cf.getBytes(HConstants.UTF8_ENCODING),
                    qualifier.getBytes(HConstants.UTF8_ENCODING), Long.valueOf(time), KeyValue.Type.Put,
                    value.getBytes(HConstants.UTF8_ENCODING));
        } catch (UnsupportedEncodingException e) {
            return null;
        }
        return k1;
    }

    /**
     * verify the value of the context.write(key, value). we don't care about the int writable because it's a key and
     * will change no matter what our input data is
     *
     * @param pair             the writable pair from the mapper
     * @param edge             the edge object to verify against
     * @param vertex           the vertex object to verify against
     */
    public final void verifyPairSecond(Pair<IntWritable, SerializedPropertyGraphElement> pair,
                                       SerializedPropertyGraphElement graphElement)
            throws IllegalAccessException, InstantiationException {

        assertEquals("graph types must match", pair.getSecond().graphElement().getType(),
                graphElement.graphElement().getType());

        Edge edge = null;
        Vertex vertex = null;
        graphElement.graphElement().get();
        if(graphElement.graphElement().isEdge()){
            edge = (Edge) graphElement.graphElement().get();
        }else if(graphElement.graphElement().isVertex()){
            vertex = (Vertex) graphElement.graphElement().get();
        }else {
            //either null or unrecognized type
            fail("null or unrecognized graph type");
        }

        graphElement.graphElement();

        assertTrue(pair.getSecond().graphElement().getSrc().equals(edge.getSrc()));
        assertTrue(pair.getSecond().graphElement().getDst().equals(edge.getDst()));
        assertTrue(pair.getSecond().graphElement().getLabel().equals(edge.getEdgeLabel()));

        if (pair.getSecond().graphElement().isEdge()) {
            Edge edgeFromPair = (Edge) pair.getSecond().graphElement();
            assertTrue(edgeFromPair.getSrc().equals(edge.getSrc()));
            assertTrue(edgeFromPair.getDst().equals(edge.getDst()));
            assertTrue(edgeFromPair.getEdgeLabel().equals(edge.getEdgeLabel()));
            for (Writable writable : edgeFromPair.getProperties().getPropertyKeys()) {
                String key = ((StringType) writable).get();
                Object value = edgeFromPair.getProperty(key);
                assertTrue(String.format("Look for %s:%s pair in our baseline object ", key, value),
                        edge.getProperty(key).equals(value));
            }
        } else if (pair.getSecond().graphElement().isVertex()) {
            Vertex vertexFromPair = (Vertex) pair.getSecond().graphElement();
            assertTrue("", vertexFromPair.getVertexId().equals(vertex.getVertexId()));
            for (Writable writable : vertexFromPair.getProperties().getPropertyKeys()) {
                String key = ((StringType) writable).get();
                Object value = vertexFromPair.getProperty(key);
                assertTrue(String.format("Look for %s:%s pair in our baseline object ", key, value.toString()),
                        vertex.getProperty(key).equals(value) );
            }
        }
    }
}
