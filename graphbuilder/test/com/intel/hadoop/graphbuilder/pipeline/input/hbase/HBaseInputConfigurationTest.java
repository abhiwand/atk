package com.intel.hadoop.graphbuilder.pipeline.input.hbase;

import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.HBaseUtils;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.IOException;

import static junit.framework.Assert.assertSame;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HBaseInputConfiguration.class,HBaseUtils.class, GraphBuilderExit.class})
public class HBaseInputConfigurationTest {

    Logger     loggerMock;
    HBaseUtils hBaseUtilsMock;
    Scan       scanMock;

    @BeforeClass
    public static final void beforeClass(){
        //this is to suppress the log 4j errors during the tests
        //we should be moving to the new context logger
        System.setProperty("log4j.ignoreTCL","true");
    }

    @Before
    public final void setupHBaseForTest() throws Exception {
        loggerMock = mock(Logger.class);
        Whitebox.setInternalState(HBaseInputConfiguration.class, "LOG", loggerMock);
    }

    @Test
    public void testSimpleUseCase() throws Exception {

        String tableName = "fakeyTable";
        hBaseUtilsMock = mock(HBaseUtils.class);

        mockStatic(HBaseUtils.class);

        when(HBaseUtils.getInstance()).thenReturn(hBaseUtilsMock);
        when(hBaseUtilsMock.tableExists(tableName)).thenReturn(true);

        HBaseInputConfiguration hbic = new HBaseInputConfiguration(tableName);


        assert(hbic.usesHBase());
        assertSame(hbic.getMapperClass(), HBaseReaderMapper.class);

        // conceivably you could vary this, but you don't want to violate it accidentally
        assert(hbic.getDescription().contains(tableName));
    }


}
