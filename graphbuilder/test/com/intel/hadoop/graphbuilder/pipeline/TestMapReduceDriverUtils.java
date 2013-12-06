package com.intel.hadoop.graphbuilder.pipeline;

import com.intel.hadoop.graphbuilder.graphelements.*;
import com.intel.hadoop.graphbuilder.pipeline.input.BaseMapper;
import com.intel.hadoop.graphbuilder.pipeline.input.hbase.HBaseReaderMapper;
import com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.propertygraphelement.PropertyGraphElements;
import com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.propertygraphelement.TitanMergedGraphElementWrite;
import com.intel.hadoop.graphbuilder.pipeline.output.titan.EdgesIntoTitanReducer;
import com.intel.hadoop.graphbuilder.pipeline.output.titan.VerticesIntoTitanReducer;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.SourceVertexKeyFunction;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseGraphBuildingRule;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseTokenizer;
import com.intel.hadoop.graphbuilder.types.LongType;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.vertices.StandardVertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
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
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.*;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.support.membermodification.MemberMatcher.method;

/**
 * An abstract class that can be extended that will hold most of the testing setup needed for output pipeline
 * grealty reducing the setup needed to test the hbase->vertices to titan mr pipeline and edges to titan reducer.
 * All the external depencies like titan, hbase are mocked out but otherwise this will run the entire pipeline from
 * command line parsing
 * rules, tokenizer to writting to titan.
 *
 * @see com.intel.hadoop.graphbuilder.pipeline.output.titan.TitanWriterMRChain
 * @see GBMapReduceDriver
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({PropertyGraphElements.class, EdgesIntoTitanReducer.class, VerticesIntoTitanReducer.class, HBaseReaderMapper.class})
public abstract class TestMapReduceDriverUtils {

    protected Configuration conf;
    protected Logger loggerMock;

    protected Mapper.Context mapContext;

    protected HBaseReaderMapper spiedHBaseReaderMapper;
    protected MapDriver<ImmutableBytesWritable, Result, IntWritable, SerializedPropertyGraphElement> mapDriver;

    protected Reducer.Context reduceContext;

    protected VerticesIntoTitanReducer spiedVerticesIntoTitanReducer;
    protected ReduceDriver<IntWritable, SerializedPropertyGraphElement, IntWritable, SerializedPropertyGraphElement> verticesReduceDriver;

    protected EdgesIntoTitanReducer spiedEdgesIntoTitanReducer;
    protected ReduceDriver<IntWritable, SerializedPropertyGraphElement, IntWritable, SerializedPropertyGraphElement> edgesReduceDriver;

    protected GBMapReduceDriver<ImmutableBytesWritable, Result, IntWritable, SerializedPropertyGraphElement, IntWritable, SerializedPropertyGraphElement> gbVertexMapReduceDriver;
    protected GBMapReduceDriver<ImmutableBytesWritable, Result, IntWritable, SerializedPropertyGraphElement, IntWritable, SerializedPropertyGraphElement> gbEdgeMapReduceDriver;

    protected Mapper.Context mapperContextMock;
    protected Reducer.Context reducerContextMock;
    protected BaseMapper baseMapper;
    protected BaseMapper spiedBaseMapper;
    protected PropertyGraphElements propertyGraphElements;
    protected PropertyGraphElements spiedVertexPropertyGraphElements;
    protected PropertyGraphElements spiedEdgePropertyGraphElements;
    protected TitanMergedGraphElementWrite titanMergedGraphElementWrite;
    protected TitanMergedGraphElementWrite spiedTitanMergedGraphElementWrite;
    protected TitanGraph titanGraph;

    protected static final Class klass = SerializedPropertyGraphElementStringTypeVids.class;
    protected static final Class valClass = SerializedPropertyGraphElementStringTypeVids.class;
    protected static final String tribecaGraphFactoryOpen = "tribecaGraphFactoryOpen";

    @BeforeClass
    public static void beforeClass(){
        //this is to suppress the log 4j errors during the tests
        //System.setProperty("log4j.ignoreTCL","true");
    }

    @Before
    public void setUp() throws Exception {
        init();
    }

    @After
    public void tearDown(){
        conf = null;
        loggerMock = null;

        mapContext = null;

        spiedHBaseReaderMapper = null;
        mapDriver = null;

        reduceContext = null;
        spiedVerticesIntoTitanReducer = null;
        verticesReduceDriver = null;

        spiedEdgesIntoTitanReducer = null;
        edgesReduceDriver = null;

        gbEdgeMapReduceDriver = null;
        gbVertexMapReduceDriver = null;

        mapperContextMock = null;
        reducerContextMock = null;
        baseMapper = null;
        spiedBaseMapper = null;

        propertyGraphElements = null;
        spiedVertexPropertyGraphElements = null;
        spiedEdgePropertyGraphElements = null;
        titanMergedGraphElementWrite = null;
        spiedTitanMergedGraphElementWrite = null;
        titanGraph = null;
    }

    protected EdgesIntoTitanReducer newEdgesIntoTitanReducer(){
        spiedEdgesIntoTitanReducer = (EdgesIntoTitanReducer) newSpy(spiedEdgesIntoTitanReducer, EdgesIntoTitanReducer.class);
        try {
            PowerMockito.doReturn(titanGraph).when(spiedEdgesIntoTitanReducer, method(EdgesIntoTitanReducer.class, tribecaGraphFactoryOpen, Reducer.Context.class))
                    .withArguments(any(Reducer.Context.class));
        } catch (Exception e) {
            fail("couldn't stub tribecaGraphFactoryOpen");
        }
        return spiedEdgesIntoTitanReducer;
    }

    protected VerticesIntoTitanReducer newVerticesIntoTitanReducer() {
        spiedVerticesIntoTitanReducer = (VerticesIntoTitanReducer) newSpy(spiedVerticesIntoTitanReducer, VerticesIntoTitanReducer.class);
        try {
            PowerMockito.doReturn(titanGraph).when(spiedVerticesIntoTitanReducer, method(VerticesIntoTitanReducer.class, tribecaGraphFactoryOpen, Reducer.Context.class))
                    .withArguments(any(Reducer.Context.class));
        } catch (Exception e) {
            fail("couldn't stub tribecaGraphFactoryOpen");
        }
        return spiedVerticesIntoTitanReducer;
    }

    protected HBaseReaderMapper newHBaseReaderMapper(){
        spiedHBaseReaderMapper = (HBaseReaderMapper) newSpy(spiedHBaseReaderMapper, HBaseReaderMapper.class);
        return spiedHBaseReaderMapper;
    }

    protected ReduceDriver newEdgeReducer(){
        newEdgesIntoTitanReducer();

        edgesReduceDriver = newReduceDriver(spiedEdgesIntoTitanReducer, "reduceContext");

        PowerMockito.when(reduceContext.getMapOutputValueClass()).thenReturn(klass);

        return edgesReduceDriver;
    }

    protected ReduceDriver newVertexReducer() {
        newVerticesIntoTitanReducer();

        verticesReduceDriver = newReduceDriver(spiedVerticesIntoTitanReducer, "reduceContext");

        PowerMockito.when(reduceContext.getMapOutputValueClass()).thenReturn(klass);

        return verticesReduceDriver;
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

        gbVertexMapReduceDriver = new GBMapReduceDriver(mapDriver, verticesReduceDriver);

        return gbVertexMapReduceDriver;
    }

    protected List<Pair<IntWritable,SerializedPropertyGraphElement>> runMapReduceDriver( GBMapReduceDriver mapReduceDriver,
            Pair<ImmutableBytesWritable,Result>[] pairs) throws IOException {

        mapReduceDriver.withConfiguration(conf);

        for(Pair<ImmutableBytesWritable, Result> kv: pairs){
            mapReduceDriver.withInput(kv);
        }

        return mapReduceDriver.run();
    }

    protected List<Pair<IntWritable,SerializedPropertyGraphElement>> runVertexHbaseMR(
            Pair<ImmutableBytesWritable,Result>[] pairs) throws IOException {

        gbVertexMapReduceDriver = new GBMapReduceDriver(mapDriver, verticesReduceDriver);

        return runMapReduceDriver(gbVertexMapReduceDriver, pairs);
    }

    protected List<Pair<IntWritable,SerializedPropertyGraphElement>> runEdgeR(
            Pair<IntWritable, com.intel.hadoop.graphbuilder.graphelements.SerializedPropertyGraphElement[]>[] pairs) throws IOException{

        edgesReduceDriver.withConfiguration(conf);

        for(Pair<IntWritable, com.intel.hadoop.graphbuilder.graphelements.SerializedPropertyGraphElement[]> kv: pairs){
            edgesReduceDriver.withInput(kv.getFirst(), Arrays.asList(kv.getSecond()) );
        }

        return edgesReduceDriver.run();
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
            PowerMockito.when(mapperContextMock.getMapOutputValueClass()).thenReturn(valClass);
        }
        return mapperContextMock;
    }

    protected Reducer.Context newReducerContext(){
        if(reducerContextMock == null){
            reducerContextMock = mock(Reducer.Context.class);
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

    protected TitanMergedGraphElementWrite newTitanMergedGraphElementWrite(){
        spiedTitanMergedGraphElementWrite = (TitanMergedGraphElementWrite)newSpy(spiedTitanMergedGraphElementWrite,
                TitanMergedGraphElementWrite.class);
        return spiedTitanMergedGraphElementWrite;
    }

    protected void newPropertyGraphElements() throws Exception {
        newVerticesIntoTitanReducer();
        newTitanMergedGraphElementWrite();

        if(spiedVertexPropertyGraphElements == null){

            spiedVertexPropertyGraphElements = spy(new PropertyGraphElements(spiedTitanMergedGraphElementWrite, null, null,
                    reduceContext, titanGraph,
                    (SerializedPropertyGraphElement)valClass.newInstance(), spiedVerticesIntoTitanReducer.getEdgeCounter(),
                    spiedVerticesIntoTitanReducer.getVertexCounter()));

            PowerMockito.whenNew(PropertyGraphElements.class).withAnyArguments().thenReturn(spiedVertexPropertyGraphElements);
        }

        if(spiedEdgePropertyGraphElements == null){

            spiedEdgePropertyGraphElements = spy(new PropertyGraphElements(spiedTitanMergedGraphElementWrite, null, null,
                    reduceContext, titanGraph,
                    (SerializedPropertyGraphElement)valClass.newInstance(), spiedEdgesIntoTitanReducer.getEdgeCounter(),
                    null));

            spiedEdgesIntoTitanReducer.getEdgePropertiesCounter();

            PowerMockito.whenNew(PropertyGraphElements.class).withAnyArguments().thenReturn(spiedEdgePropertyGraphElements);
        }
    }

    protected TitanGraph newTitanGraphMock(){
        titanGraph = (TitanGraph) newMock(titanGraph, TitanGraph.class);
        return titanGraph;
    }

    protected ReduceDriver newReduceDriver(org.apache.hadoop.mapreduce.Reducer reducer, String contextFieldName){
        ReduceDriver newDriver = ReduceDriver.newReduceDriver(reducer);

        Field field;
        try {
            field = TestMapReduceDriverUtils.class.getDeclaredField(contextFieldName);
            field.setAccessible(true);
            try {
                field.set(this, newDriver.getContext());
            } catch (IllegalAccessException e) {
                fail("couldn't set context");
            }
        } catch (NoSuchFieldException e) {
            fail("couldn't find context field: " + contextFieldName);
        }

        return newDriver;
    }

    protected Object newMock(Object inst, Class klass){
        if(inst == null){
            inst = mock(klass);
        }
        return inst;
    }

    protected Object newSpy(Object object, Class klass){
        if(object == null){
            try {
                object = klass.newInstance();
            } catch (InstantiationException e) {
                fail("couldn't spy: " + klass.getName());
            } catch (IllegalAccessException e) {
                fail("couldn't spy: " + klass.getName());
            }
            object = spy(object);
        }
        return object;
    }

    public com.tinkerpop.blueprints.Vertex vertexMock(){
        return mock(StandardVertex.class);
    }

    protected void init() throws Exception {
        newTitanGraphMock();

        newEdgeReducer();
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


    public static final Vertex<StringType> newVertex(String vertexId, HashMap<String, WritableComparable> properties){
        com.intel.hadoop.graphbuilder.graphelements.Vertex vertex =
                new com.intel.hadoop.graphbuilder.graphelements.Vertex<StringType>(new StringType(vertexId));
        for(Map.Entry<String, WritableComparable> entry: properties.entrySet()){
            vertex.setProperty(entry.getKey(), entry.getValue());
        }
        return vertex;
    }

    public static final Vertex<StringType> newVertex(String vertexId, PropertyMap propertyMap){
        com.intel.hadoop.graphbuilder.graphelements.Vertex vertex =
                new com.intel.hadoop.graphbuilder.graphelements.Vertex<StringType>(new StringType(vertexId), propertyMap);
        return vertex;
    }

    public static final Edge<StringType> newEdge(String src, String dst, String label, PropertyMap propertyMap){
        com.intel.hadoop.graphbuilder.graphelements.Edge edge =
                new Edge<StringType>(new StringType(src), new StringType(dst), new StringType(label), propertyMap);;
        return edge;
    }

    public static final Edge<StringType> newEdge(String src, String dst, String label, HashMap<String, WritableComparable> properties){
        com.intel.hadoop.graphbuilder.graphelements.Edge edge =
                new Edge<StringType>(new StringType(src), new StringType(dst), new StringType(label));
        for(Map.Entry<String, WritableComparable> entry: properties.entrySet()){
            edge.setProperty(entry.getKey(), entry.getValue());
        }
        return edge;
    }

    /**
     * setup our sample data for our Results column list
     *
     * @return Result column list
     */
    public static final Result sampleDataAlice() {
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
     *
     */
    public final void verifyPairSecond(Pair<IntWritable, SerializedPropertyGraphElement> pair,
                                       SerializedPropertyGraphElement graphElement){


        assertEquals("graph types must match", pair.getSecond().graphElement().getType(),
                graphElement.graphElement().getType());

        //assign to local variables to reduce line length;
        PropertyGraphElement jobGraphElement = pair.getSecond().graphElement();
        PropertyGraphElement givenGraphElement = graphElement.graphElement();


        assertTrue("match src", (jobGraphElement.getSrc() == null && givenGraphElement.getSrc() == null)
                || jobGraphElement.getSrc().equals(givenGraphElement.getSrc()));
        assertTrue("match dst", (jobGraphElement.getDst() == null && givenGraphElement.getDst() == null)
                || jobGraphElement.getDst().equals(givenGraphElement.getDst()));
        assertTrue("match label", (jobGraphElement.getLabel() == null && givenGraphElement.getLabel() == null)
                || jobGraphElement.getLabel().equals(givenGraphElement.getLabel()));
        assertTrue("match id", (jobGraphElement.getId() == null && givenGraphElement.getId() == null)
                || jobGraphElement.getId().equals(givenGraphElement.getId()));

        for (Writable writable : jobGraphElement.getProperties().getPropertyKeys()) {
            String key = ((StringType) writable).get();
            Object value = jobGraphElement.getProperty(key);
            assertTrue(String.format("Look for %s:%s pair in our baseline object ", key, value.toString()),
                    givenGraphElement.getProperty(key).equals(value));
        }

    }
}
