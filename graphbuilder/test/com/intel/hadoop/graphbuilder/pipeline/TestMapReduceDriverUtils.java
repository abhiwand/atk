package com.intel.hadoop.graphbuilder.pipeline;

import com.intel.hadoop.graphbuilder.graphelements.SerializedPropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.SerializedPropertyGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.pipeline.input.BaseMapper;
import com.intel.hadoop.graphbuilder.pipeline.input.hbase.HBaseReaderMapper;
import com.intel.hadoop.graphbuilder.pipeline.output.titan.VerticesIntoTitanReducer;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.SourceVertexKeyFunction;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseGraphBuildingRule;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IntWritable;
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
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 * An abstract class that can be extended that will hold most of the testing setup need for GB.
 */
public abstract class TestMapReduceDriverUtils {
    protected Configuration conf;
    protected Logger loggerMock;

    protected HBaseReaderMapper hBaseReaderMapper;
    protected HBaseReaderMapper spiedHBaseReaderMapper;
    protected MapDriver<ImmutableBytesWritable, Result, IntWritable, SerializedPropertyGraphElement> mapDriver;

    protected ReduceDriver<IntWritable, SerializedPropertyGraphElement, IntWritable, SerializedPropertyGraphElement> reduceDriver;
    protected VerticesIntoTitanReducer verticesIntoTitanReducer;
    protected VerticesIntoTitanReducer spiedVerticesIntoTitanReducer;

    protected MapReduceDriver<ImmutableBytesWritable, Result, IntWritable, SerializedPropertyGraphElement, IntWritable, SerializedPropertyGraphElement> mapReduceDriver;
    protected GBMapReduceDriver<ImmutableBytesWritable, Result, IntWritable, SerializedPropertyGraphElement, IntWritable, SerializedPropertyGraphElement> gbMapReduceDriver;
    protected PipelineMapReduceDriver<ImmutableBytesWritable, Result, IntWritable, SerializedPropertyGraphElement> pipelineMapReduceDriver;

    protected Mapper.Context mapperContextMock;
    protected Reducer.Context reducerContextMock;
    protected BaseMapper baseMapper;
    protected BaseMapper spiedBaseMapper;

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

        reduceDriver = ReduceDriver.newReduceDriver(verticesIntoTitanReducer);
        return reduceDriver;
    }

    protected MapDriver newHbaseMapper(){
        newHBaseReaderMapper();

        mapDriver = MapDriver.newMapDriver(spiedHBaseReaderMapper);
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

    protected List<Pair<IntWritable,SerializedPropertyGraphElement>> runVertexHbaseMR(ImmutableBytesWritable key, Result result) throws IOException {


        Mapper.Context mc =  mapDriver.getContext();

        Reducer.Context rc = reduceDriver.getContext();

        Class klass = SerializedPropertyGraphElementStringTypeVids.class;

        PowerMockito.when(rc.getMapOutputValueClass()).thenReturn(klass);
        PowerMockito.when(mc.getMapOutputValueClass()).thenReturn(klass);

        /*List<Pair<Mapper,Reducer>> pipeline = new ArrayList<Pair<Mapper,Reducer>>();
        //pipeline.add(new Pair<Mapper, Reducer>(mapDriver,reduceDriver));
        pipelineMapReduceDriver = PipelineMapReduceDriver.newPipelineMapReduceDriver();
        pipelineMapReduceDriver.*/

        gbMapReduceDriver = new GBMapReduceDriver(mapDriver, reduceDriver);

        gbMapReduceDriver.withConfiguration(conf).withInput(key, result);



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
            Class valClass = SerializedPropertyGraphElementStringTypeVids.class;
            PowerMockito.when(mapperContextMock.getMapOutputValueClass()).thenReturn(valClass);
        }
        return mapperContextMock;
    }

    protected Reducer.Context newReducerContext(){
        if(reducerContextMock == null){
            reducerContextMock = mock(Reducer.Context.class);
            Class valClass = SerializedPropertyGraphElementStringTypeVids.class;
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

    protected void init() throws Exception {
        newVertexReducer();
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
}
