package com.intel.hadoop.graphbuilder.pipeline.output.titan;


import com.intel.hadoop.graphbuilder.graphelements.SerializedPropertyGraphElement;
import com.intel.hadoop.graphbuilder.pipeline.input.hbase.HBaseReaderMapper;
import com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.propertygraphelement.PropertyGraphElements;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseGraphBuildingRule;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseTokenizer;
import com.intel.hadoop.graphbuilder.pipeline.input.BaseMapper;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.SourceVertexKeyFunction;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.RecordTypeHBaseRow;
import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.SerializedPropertyGraphElementStringTypeVids;
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
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
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
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.support.membermodification.MemberMatcher.method;

@RunWith(PowerMockRunner.class)
@PrepareForTest(PropertyGraphElements.class)
public class VerticesIntoTitanReducerTest {
     ReduceDriver<IntWritable, SerializedPropertyGraphElement, IntWritable, SerializedPropertyGraphElement> reduceDriver;
     VerticesIntoTitanReducer verticesIntoTitanReducer;
     VerticesIntoTitanReducer spiedVerticesIntoTitanReducer;

    @Before
    public void setUp(){
        verticesIntoTitanReducer = new VerticesIntoTitanReducer();
        spiedVerticesIntoTitanReducer = spy(verticesIntoTitanReducer);

        reduceDriver = ReduceDriver.newReduceDriver(verticesIntoTitanReducer);


    }

    @After
    public void tearDown(){

    }

    @Test
    public void test(){

    }
}
