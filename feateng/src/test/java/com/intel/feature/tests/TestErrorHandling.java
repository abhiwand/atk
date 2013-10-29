//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2013 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////
package com.intel.feature.tests;

import java.io.File;
import java.io.IOException;

import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.JUnitCore;

/**
 * @author Nezih Yigitbasi
 * @email nezih.yigitbasi@intel.com
 */
public class TestErrorHandling {

	@Before
	public void setup() {
		System.out.println("*** Starting error handling tests ***");
	}

	@Test
	public void testErroneousPythonUDF() throws IOException, ParseException {
		System.out.println("Testing erroneous python udf");

		String[] script = {
				"REGISTER 'pig/tests/erroneous_udfs.py' USING jython as erroneous_udfs",
				"parsed_val = LOAD 'hbase://shaw_table' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('etl-cf:timestamp etl-cf:item_id etl-cf:method etl-cf:src_tms etl-cf:event_type etl-cf:dst_tms etl-cf:duration', '-loadKey true') as (key:chararray, timestamp:chararray, item_id:chararray, method:chararray, src_tms:chararray, event_type:chararray, dst_tms:chararray, duration:chararray);",
				"squared_duration = FOREACH parsed_val GENERATE erroneous_udfs.square(duration);" };

		PigTest test = new PigTest(script);
		// the final field (duration) is empty, which will make the python udf
		// throw an exception
		String[] input = { "1\t1340497060.737806\t\tuser\t1340496000_EP015402660033_219_0_34313_1_2_8837\ttransportControl\t1340496000_EP015402660033_219_0_34313_1_2_8837\t" };
		String[] output = { "()" };
		test.assertOutput("parsed_val", input, "squared_duration", output);
	}

	@Test
	public void testErroneousJavaUDF() throws IOException, ParseException {
		File f = new File("target/Intel-FeatureEngineering-0.0.1-SNAPSHOT.jar");
		if (!f.exists()) {
			String errorMessage = "target/Intel-FeatureEngineering-0.0.1-SNAPSHOT.jar doesn't exist. We need that jar to run this test case. So if you are running the tests "
					+ "with maven, first please run maven with -DskipTests to package the jar and then run the tests.";
			throw new RuntimeException(errorMessage);
		}
		System.out.println("Testing erroneous java udf");

		String[] script = {
				"REGISTER target/Intel-FeatureEngineering-0.0.1-SNAPSHOT.jar",
				"parsed_val = LOAD 'hbase://shaw_table' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('etl-cf:timestamp etl-cf:item_id etl-cf:method etl-cf:src_tms etl-cf:event_type etl-cf:dst_tms etl-cf:duration', '-loadKey true') as (key:chararray, timestamp:chararray, item_id:chararray, method:chararray, src_tms:chararray, event_type:chararray, dst_tms:chararray, duration:chararray);",
				"squared_duration = FOREACH parsed_val GENERATE com.intel.pig.udf.ErroneousEvalFunc(duration);" };

		PigTest test = new PigTest(script);
		// the final field (duration) is empty, which will make the java udf
		// throw an exception
		String[] input = { "1\t1340497060.737806\t\tuser\t1340496000_EP015402660033_219_0_34313_1_2_8837\ttransportControl\t1340496000_EP015402660033_219_0_34313_1_2_8837\t" };
		String[] output = { "()" };// the java udf should catch the exception
									// and return an empty tuple
		test.assertOutput("parsed_val", input, "squared_duration", output);
	}

	@After
	public void done() {
		System.out.println("*** Done with the error handling tests ***");
	}

	public static void main(String[] args) {
		JUnitCore.main("com.tribece.feature.tests.TestErrorHandling");
	}
}
