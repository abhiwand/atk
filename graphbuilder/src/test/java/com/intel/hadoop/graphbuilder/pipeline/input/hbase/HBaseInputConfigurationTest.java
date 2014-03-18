/**
 * Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more about this software visit:
 *     http://www.01.org/GraphBuilder
 */
package com.intel.hadoop.graphbuilder.pipeline.input.hbase;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.*;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.HBaseUtils;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HBaseInputConfiguration.class,HBaseUtils.class, GraphBuilderExit.class})
public class HBaseInputConfigurationTest {

    @BeforeClass
    public static final void beforeClass(){
        //this is to suppress the log 4j errors during the tests
        //we should be moving to the new context logger
        System.setProperty("log4j.ignoreTCL","true");
    }

    @Before
    public final void setupHBaseForTest() throws Exception {
        Logger loggerMock = mock(Logger.class);
        Whitebox.setInternalState(HBaseInputConfiguration.class, "LOG", loggerMock);
    }

    @Test
    public void testSimpleUseCase() throws Exception {

        String tableName = "fakeyTable";
        HBaseUtils hBaseUtilsMock = mock(HBaseUtils.class);

        mockStatic(HBaseUtils.class);

        when(HBaseUtils.getInstance()).thenReturn(hBaseUtilsMock);
        when(hBaseUtilsMock.tableExists(tableName)).thenReturn(true);

        HBaseInputConfiguration hbic = new HBaseInputConfiguration(tableName);

		assertTrue(hbic.usesHBase());
        assertSame(hbic.getMapperClass(), HBaseReaderMapper.class);

        // conceivably you could vary this, but you don't want to violate it accidentally
		assertTrue(hbic.getDescription().contains(tableName));

    }

	@Test
	public void HBaseInputConfiguration_table_does_not_exist_is_a_fatal_exit() throws Exception {
        String expectedMessage = "expected exception message from mock";
		String tableName = "fakeyTable";

        HBaseUtils hBaseUtilsMock = mock(HBaseUtils.class);
        when(hBaseUtilsMock.tableExists(tableName)).thenReturn(false);

        mockStatic(HBaseUtils.class);
		when(HBaseUtils.getInstance()).thenReturn(hBaseUtilsMock);

        mockStatic(GraphBuilderExit.class);
        doThrow(new RuntimeException(expectedMessage)).when(GraphBuilderExit.class, "graphbuilderFatalExitNoException",
                any(), any(), any());

		try {
            // invoke method under test
			new HBaseInputConfiguration(tableName);

            fail("Expected exception did not occur");
		} catch (Exception e) {
            assertEquals(expectedMessage, e.getMessage());
		}
	}
}
