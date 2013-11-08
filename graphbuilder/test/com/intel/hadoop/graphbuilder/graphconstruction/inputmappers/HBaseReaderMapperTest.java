package com.intel.hadoop.graphbuilder.graphconstruction.inputmappers;


import com.intel.hadoop.graphbuilder.demoapps.tabletotextgraph.BasicHBaseGraphBuildingRule;
import com.intel.hadoop.graphbuilder.demoapps.tabletotextgraph.BasicHBaseTokenizer;
import com.intel.hadoop.graphbuilder.graphconstruction.keyfunction.SourceVertexKeyFunction;
import com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.RecordTypeHBaseRow;
import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import javax.xml.parsers.ParserConfigurationException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.*;
import static org.powermock.api.support.membermodification.MemberMatcher.method;

@RunWith(PowerMockRunner.class)
@PrepareForTest(HBaseReaderMapper.class)
public class HBaseReaderMapperTest {
    private MapDriver<ImmutableBytesWritable, Result, IntWritable, PropertyGraphElement> mapDriver;
    private HBaseReaderMapper hBaseReaderMapper;
    private HBaseReaderMapper spiedHBaseReaderMapper;
    private Configuration conf = new Configuration();
    private Mapper.Context mapperContextMock;
    private PropertyGraphElementStringTypeVids valueClass;
    private Logger loggerMock;
    private ImmutableBytesWritable key;
    private RecordTypeHBaseRow recordTypeHBaseRow;
    private Result result;

    BaseMapper baseMapper;
    BaseMapper spiedBaseMapper;

    @After
    public final void tearDown() {
        loggerMock = null;
        valueClass = null;
        mapperContextMock = null;
        conf = null;
        hBaseReaderMapper = null;
        spiedHBaseReaderMapper = null;
        mapDriver = null;

        baseMapper = null;
        spiedBaseMapper = null;

        key = null;
        result = null;
        recordTypeHBaseRow = null;
    }

    @BeforeClass
    public static final void beforeClass(){
        //this is to suppress the log 4j errors during the tests
        //we should be moving to the new context logger
        System.setProperty("log4j.ignoreTCL","true");
    }

    @Before
    public final void setUp() throws Exception {
        loggerMock = mock(Logger.class);
        Whitebox.setInternalState(HBaseReaderMapper.class, "LOG", loggerMock);

        valueClass = mock(PropertyGraphElementStringTypeVids.class);
        mapperContextMock = mock(Mapper.Context.class);

        conf = new Configuration();
        conf.set("GraphTokenizer", BasicHBaseTokenizer.class.getName());
        conf.set("KeyFunction", SourceVertexKeyFunction.class.getName());

        //sample vertex and edge generation rules

        String[] vertexRules = new String[1];
        vertexRules[0] = "cf:name=cf:age,cf:dept";
        BasicHBaseGraphBuildingRule.packVertexRulesIntoConfiguration(conf, vertexRules);

        String[] edgeRules = new String[0];
        BasicHBaseGraphBuildingRule.packEdgeRulesIntoConfiguration(conf, edgeRules);

        String[] directedEdgeRules = new String[1];
        directedEdgeRules[0] =  "cf:name,cf:dept,worksAt";
        BasicHBaseGraphBuildingRule.packDirectedEdgeRulesIntoConfiguration(conf, directedEdgeRules);

        //mapper context mocks
        Class valClass = PropertyGraphElementStringTypeVids.class;
        PowerMockito.when(mapperContextMock.getMapOutputValueClass()).thenReturn(valClass);

        baseMapper = new BaseMapper(mapperContextMock, conf, loggerMock);
        spiedBaseMapper = spy(baseMapper);
        PowerMockito.whenNew(BaseMapper.class).withAnyArguments().thenReturn(spiedBaseMapper);

        spiedBaseMapper.setValClass(PropertyGraphElementStringTypeVids.class);
        doNothing().when(spiedBaseMapper).setValClass(any(Class.class));

        spiedBaseMapper.setMapVal(PropertyGraphElementStringTypeVids.class.newInstance());
        doNothing().when(spiedBaseMapper).setMapVal(any(PropertyGraphElement.class));

        //set up the spied HbaseReaderMapper
        hBaseReaderMapper = new HBaseReaderMapper();
        spiedHBaseReaderMapper = spy(hBaseReaderMapper);
        spiedHBaseReaderMapper.setBaseMapper(spiedBaseMapper);

        //row key is trow away nothing is done with it
        key = new ImmutableBytesWritable(Bytes.toBytes("row1"));

        //set up some sample Hbase.client result
        result = sampleData();

        recordTypeHBaseRow = new RecordTypeHBaseRow(key, result);


    }

    @Test
    public final void verify_writes_with_good_input() throws Exception {
        //set the return value on the getRecordTypeHBaseRow private method
        when(spiedHBaseReaderMapper, method(HBaseReaderMapper.class, "getRecordTypeHBaseRow",
                ImmutableBytesWritable.class, Result.class))
                .withArguments(any(ImmutableBytesWritable.class), any(Result.class))
                .thenReturn(recordTypeHBaseRow);

        //set the map driver once we are done with all the stubbing/mocking
        mapDriver = MapDriver.newMapDriver(spiedHBaseReaderMapper);

        //set our config and input for the mapper
        mapDriver.withConfiguration(conf).withInput(key, result);

        //run test
        List<Pair<IntWritable, PropertyGraphElement>> writables = mapDriver.run();

        //check the output of the mapper in this case with the input we should get 4 writes
        assertTrue("check for four writes", writables.size() == 4);
        assertEquals("verify context column counter", 5,
                mapDriver.getCounters().findCounter(GBHTableConfig.Counters.HTABLE_COLS_READ).getValue());
        assertEquals("verify context row counter", 1,
                mapDriver.getCounters().findCounter(GBHTableConfig.Counters.HTABLE_ROWS_READ).getValue());

        //make sure the error counters are 0
        assertEquals("verify context edge error counter", 0,
                mapDriver.getCounters().findCounter(BaseMapper.getEdgeWriteErrorCounter()).getValue());
        assertEquals("verify context vertex error counter", 0,
                mapDriver.getCounters().findCounter(BaseMapper.getVertexWriteErrorCounter()).getValue());

        //only the map values is validated not the key since it can change all the time

        //set up our matching edge to test against
        Edge<StringType> edge = new Edge<StringType>(new StringType("Alice"), new StringType("GAO123"),
                new StringType("worksAt"));
        //verify the 1st map value is an edge and matches our edge object
        verifyPairSecond(writables.get(0), "EDGE", edge, null);

        //set up our matching vertex to test against
        Vertex<StringType> vertex = new Vertex<StringType>(new StringType("Alice"));
        vertex.setProperty("cf:dept", new StringType("GAO123"));
        vertex.setProperty("cf:age", new StringType("43"));

        //verify the second map value is a vertex and matest our vertex
        verifyPairSecond(writables.get(1), "VERTEX", null, vertex);

        vertex = new Vertex<StringType>(new StringType("Alice"));
        verifyPairSecond(writables.get(2), "VERTEX", null, vertex);

        vertex = new Vertex<StringType>(new StringType("GAO123"));
        verifyPairSecond(writables.get(3), "VERTEX", null, vertex);
    }

    @Test
    public final void verify_null_src_in_edge_logs_null_pointer_exception() throws Exception {
        //set the return value on the getRecordTypeHBaseRow private method
        when(spiedHBaseReaderMapper, method(HBaseReaderMapper.class, "getRecordTypeHBaseRow",
                ImmutableBytesWritable.class, Result.class))
                .withArguments(any(ImmutableBytesWritable.class), any(Result.class))
                .thenReturn(recordTypeHBaseRow);

        BasicHBaseTokenizer spiedBasicHBaseTokenizer = getTokenizer();

        //create the broken edge list to return
        Edge<StringType> brokenEdge = new Edge<StringType>(new StringType(null), new StringType("GAO123"),
                new StringType("worksAt"));
        ArrayList<Edge<StringType>> brokenEdgeList = new ArrayList<Edge<StringType>>(Arrays.asList(brokenEdge));
        //mocked edgelist return
        when(spiedBasicHBaseTokenizer.getEdges()).thenReturn(brokenEdgeList.iterator());

        //set private method with our spied tokenizer
        Whitebox.setInternalState(spiedBaseMapper, "tokenizer", spiedBasicHBaseTokenizer);

        mapDriver = MapDriver.newMapDriver(spiedHBaseReaderMapper);
        mapDriver.withConfiguration(conf).withInput(key, result);
        mapDriver.run();

        //verify the error call with null exception
        verify(loggerMock).error(any(String.class), any(NullPointerException.class));
        //verify our edge error counter
        assertEquals("verify the edge error counter is equal to one", 1,
                mapDriver.getCounters().findCounter(BaseMapper.getEdgeWriteErrorCounter()).getValue());
    }


    @Test
    public final void verify_null_dst_in_edge_logs_null_pointer_exception() throws Exception {
        //set the return value on the getRecordTypeHBaseRow private method
        when(spiedHBaseReaderMapper, method(HBaseReaderMapper.class, "getRecordTypeHBaseRow",
                ImmutableBytesWritable.class, Result.class))
                .withArguments(any(ImmutableBytesWritable.class), any(Result.class))
                .thenReturn(recordTypeHBaseRow);

        BasicHBaseTokenizer spiedBasicHBaseTokenizer = getTokenizer();


        Edge<StringType> brokenEdge = new Edge<StringType>(new StringType("Alice"), new StringType(null),
                new StringType("worksAt"));
        ArrayList<Edge<StringType>> brokenEdgeList = new ArrayList<Edge<StringType>>(Arrays.asList(brokenEdge));
        //mock edge list return
        when(spiedBasicHBaseTokenizer.getEdges()).thenReturn(brokenEdgeList.iterator());

        Whitebox.setInternalState(spiedBaseMapper, "tokenizer", spiedBasicHBaseTokenizer);

        mapDriver = MapDriver.newMapDriver(spiedHBaseReaderMapper);
        mapDriver.withConfiguration(conf).withInput(key, result);
        mapDriver.run();

        verify(loggerMock).error(any(String.class), any(NullPointerException.class));
        assertEquals("verify the edge error counter is equal to one", 1,
                mapDriver.getCounters().findCounter(BaseMapper.getEdgeWriteErrorCounter()).getValue());
    }


    @Test
    public final void test_null_label_in_edge_logs_null_pointer_exception() throws Exception {
        //set the return value on the getRecordTypeHBaseRow private method
        when(spiedHBaseReaderMapper, method(HBaseReaderMapper.class, "getRecordTypeHBaseRow",
                ImmutableBytesWritable.class, Result.class))
                .withArguments(any(ImmutableBytesWritable.class), any(Result.class))
                .thenReturn(recordTypeHBaseRow);

        BasicHBaseTokenizer spiedBasicHBaseTokenizer = getTokenizer();

        Edge<StringType> brokenEdge = new Edge<StringType>(new StringType("Alice"), new StringType("GAO123"),
                new StringType(null));
        ArrayList<Edge<StringType>> brokenEdgeList = new ArrayList<Edge<StringType>>(Arrays.asList(brokenEdge));
        when(spiedBasicHBaseTokenizer.getEdges()).thenReturn(brokenEdgeList.iterator());

        Whitebox.setInternalState(spiedBaseMapper, "tokenizer", spiedBasicHBaseTokenizer);

        mapDriver = MapDriver.newMapDriver(spiedHBaseReaderMapper);
        mapDriver.withConfiguration(conf).withInput(key, result);
        mapDriver.run();

        verify(loggerMock).error(any(String.class), any(NullPointerException.class));
        assertEquals("verify the edge error counter is equal to one", 1,
                mapDriver.getCounters().findCounter(BaseMapper.getEdgeWriteErrorCounter()).getValue());
    }

    @Test
    public final void test_null_vertex_id_logs_null_pointer_exception() throws Exception {
        //set the return value on the getRecordTypeHBaseRow private method
        when(spiedHBaseReaderMapper, method(HBaseReaderMapper.class, "getRecordTypeHBaseRow",
                ImmutableBytesWritable.class, Result.class))
                .withArguments(any(ImmutableBytesWritable.class), any(Result.class))
                .thenReturn(recordTypeHBaseRow);

        BasicHBaseTokenizer spiedBasicHBaseTokenizer = getTokenizer();

        Vertex<StringType> brokenVertex = new Vertex<StringType>(new StringType(null));
        ArrayList<Vertex<StringType>> brokenVertexList = new ArrayList<Vertex<StringType>>(Arrays.asList
                (brokenVertex));

        when(spiedBasicHBaseTokenizer.getVertices()).thenReturn(brokenVertexList.iterator());

        Whitebox.setInternalState(spiedBaseMapper, "tokenizer", spiedBasicHBaseTokenizer);

        mapDriver = MapDriver.newMapDriver(spiedHBaseReaderMapper);
        mapDriver.withConfiguration(conf).withInput(key, result);
        mapDriver.run();

        verify(loggerMock).error(any(String.class), any(NullPointerException.class));
        assertEquals("verify the vertex error counter is equal to one", 1,
                mapDriver.getCounters().findCounter(BaseMapper.getVertexWriteErrorCounter()).getValue());
    }

    /**
     * setup our sample data for our Results column list
     *
     * @return Result column list
     */
    public final Result sampleData() {
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
     * help debug the wierdness with Hbase.Client.Results. prints all the sample data to see if any values are coming
     * back null
     *
     * @throws UnsupportedEncodingException
     */
    public final void printSampleData() throws UnsupportedEncodingException {
        Result result = sampleData();
        printSampleRow(result, "cf", "age");
        printSampleRow(result, "cf", "dept");
        printSampleRow(result, "cf", "id");
        printSampleRow(result, "cf", "manager");
        printSampleRow(result, "cf", "name");
        printSampleRow(result, "cf", "group");
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
    public final KeyValue newKeyValue(String row, String cf, String qualifier, String value, String time) {
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
     * @param graphElementType the type of graph element edge/vertex
     * @param edge             the edge object to verify against
     * @param vertex           the vertex object to verify against
     */
    public final void verifyPairSecond(Pair<IntWritable, PropertyGraphElement> pair, String graphElementType,
                                       Edge<StringType> edge, Vertex<StringType> vertex) {

        assertEquals("Veryfiy Type", graphElementType, pair.getSecond().graphElementType().toString());
        if (pair.getSecond().graphElementType().toString().equals("EDGE")) {
            assertTrue(pair.getSecond().edge().getSrc().equals(edge.getSrc()));
            assertTrue(pair.getSecond().edge().getDst().equals(edge.getDst()));
            assertTrue(pair.getSecond().edge().getEdgeLabel().equals(edge.getEdgeLabel()));
            for (Writable writable : pair.getSecond().edge().getProperties().getPropertyKeys()) {
                String key = ((StringType) writable).get();
                String value = ((StringType) pair.getSecond().edge().getProperty(key)).get();
                assertTrue(String.format("Look for %s:%s pair in our baseline object ", key, value),
                        ((StringType) edge.getProperty(key)).get().equals(value));
            }
        } else if (pair.getSecond().graphElementType().toString().equals("VERTEX")) {
            assertTrue("", pair.getSecond().vertex().getVertexId().equals(vertex.getVertexId()));
            for (Writable writable : pair.getSecond().vertex().getProperties().getPropertyKeys()) {
                String key = ((StringType) writable).get();
                String value = ((StringType) pair.getSecond().vertex().getProperty(key)).get();
                assertTrue(String.format("Look for %s:%s pair in our baseline object ", key, value),
                        ((StringType) vertex.getProperty(key)).get().equals(value));
            }
        }
    }

    /**
     * creates a spied tokenizer we can use in other mocks
     *
     * @return spied tokenizer
     * @throws ParserConfigurationException
     */
    public final BasicHBaseTokenizer getTokenizer() throws ParserConfigurationException {
        BasicHBaseTokenizer basicHBaseTokenizer = new BasicHBaseTokenizer();
        BasicHBaseTokenizer spiedBasicHBaseTokenizer = spy(basicHBaseTokenizer);
        spiedBasicHBaseTokenizer.configure(conf);
        return spiedBasicHBaseTokenizer;
    }
}
