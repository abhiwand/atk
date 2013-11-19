package com.intel.hadoop.graphbuilder.pipeline.input.hbase;

import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.util.HBaseUtils;
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

import static junit.framework.Assert.assertSame;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.support.SuppressCode.suppressConstructor;

@RunWith(PowerMockRunner.class)
@PrepareForTest(HBaseInputConfiguration.class)
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

        mockStatic(HBaseUtils.class);

        hBaseUtilsMock = mock(HBaseUtils.class);
        scanMock       = mock(Scan.class);
    }

    // nls todo : this is a total WIP, i just pushed to the server so Rene could see my other changes
    @Ignore
    @Test
    public void testSimpleUseCase() throws Exception {

        String tableName = "fakeyTable";
        HBaseInputConfiguration hbic = new HBaseInputConfiguration("fakeyTable");

        assert(hbic.usesHBase());
        assertSame(hbic.getMapperClass(), HBaseReaderMapper.class);

        // conceivably you could vary this, but you don't want to violate it accidentally
        assert(hbic.getDescription().contains(tableName));
    }
}
