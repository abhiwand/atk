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
public class TestShawETL {

	@Before
	public void setup() {
		System.out.println("*** Starting Shaw ETL tests. ***");
	}

	@Test
	public void testParsingShawDataset() throws IOException, ParseException {
		System.out.println("Testing pig/parser.pig");
		String[] paramFiles = { "pig/shaw.params" };
		PigTest test = new PigTest("pig/parser.pig", null, paramFiles);
		String[] input = { "1340496217.605411|141|alcservices[1035]: eventType=transportControl srcContext=watch:/?src=tv&id=tv_showing:?tmsid=1340496000_SH002930050000_36_0_16493_1_2_9731&control=unknown&xstate=1&tc=0&cs=001DCE8D3DF1.head0.0..1400667088  destContext=watch:/?src=tv&id=tv_showing:?tmsid=1340496000_SH002930050000_36_0_16493_1_2_9731&control=unknown&xstate=1&tc=0&cs=001DCE8D3DF1.head0.0..1400667088 method=user duration=100" };
		String[] output = { "(1340496217.605411,transportControl,user,100,,1340496000_SH002930050000_36_0_16493_1_2_9731,1340496000_SH002930050000_36_0_16493_1_2_9731)" };
		test.assertOutput("logs", input, "parsed_val", output);
	}

	@Test
	public void testCleaningShawDataset() throws IOException, ParseException {
		System.out.println("Testing pig/cleaner.pig");
		String[] paramFiles = { "pig/shaw.params" };
		PigTest test = new PigTest("pig/cleaner.pig", null, paramFiles);
		// no duration field in the input so it should be filtered out
		String[] inputWithoutDurationField = { "1340496217.605411|141|alcservices[1035]: eventType=transportControl srcContext=watch:/?src=tv&id=tv_showing:?tmsid=1340496000_SH002930050000_36_0_16493_1_2_9731&control=unknown&xstate=1&tc=0&cs=001DCE8D3DF1.head0.0..1400667088  destContext=watch:/?src=tv&id=tv_showing:?tmsid=1340496000_SH002930050000_36_0_16493_1_2_9731&control=unknown&xstate=1&tc=0&cs=001DCE8D3DF1.head0.0..1400667088 method=user" };
		test.assertOutput("logs", inputWithoutDurationField, "cleaned_data",
				new String[] { "" });
		// with the duration field in the input the output should be the same as
		// parsing output
		String[] inputWithDurationField = { "1340496217.605411|141|alcservices[1035]: eventType=transportControl srcContext=watch:/?src=tv&id=tv_showing:?tmsid=1340496000_SH002930050000_36_0_16493_1_2_9731&control=unknown&xstate=1&tc=0&cs=001DCE8D3DF1.head0.0..1400667088  destContext=watch:/?src=tv&id=tv_showing:?tmsid=1340496000_SH002930050000_36_0_16493_1_2_9731&control=unknown&xstate=1&tc=0&cs=001DCE8D3DF1.head0.0..1400667088 method=user duration=100" };
		String[] output = { "(1340496217.605411,transportControl,user,100,,1340496000_SH002930050000_36_0_16493_1_2_9731,1340496000_SH002930050000_36_0_16493_1_2_9731)" };
		test.assertOutput("logs", inputWithDurationField, "cleaned_data",
				output);
	}

	/**
	 * pig/standardizer.pig requires the duration field to be double, but it's chararray
	 * so we removed the below test. We use py-scripts/transform.py for standardization instead of pig/standardizer.pig
	 */
//	@Test
	public void testStandardizeShawDataset() throws IOException, ParseException {
		System.out.println("Testing pig/standardizer.pig.pig");
		String[] paramFiles = { "pig/shaw.params" };
		PigTest test = new PigTest("pig/standardizer.pig", null, paramFiles);
		// standardize based on duration, see shaw.params
		String[] input = { "1340496217.605411|141|alcservices[1035]: eventType=transportControl srcContext=watch:/?src=tv&id=tv_showing:?tmsid=1340496000_SH002930050000_36_0_16493_1_2_9731&control=unknown&xstate=1&tc=0&cs=001DCE8D3DF1.head0.0..1400667088  destContext=watch:/?src=tv&id=tv_showing:?tmsid=1340496000_SH002930050000_36_0_16493_1_2_9731&control=unknown&xstate=1&tc=0&cs=001DCE8D3DF1.head0.0..1400667088 method=user duration=100" };
		String[] output = { "(1340496217.605411,transportControl,user,,,1340496000_SH002930050000_36_0_16493_1_2_9731,1340496000_SH002930050000_36_0_16493_1_2_9731)" };
		/*standardization undefined for a single record*/
		test.assertOutput("logs", input, "standardized_dataset", output);
		

		String[] inputMultiple = {
				"1340496217.605411|141|alcservices[1035]: eventType=transportControl srcContext=watch:/?src=tv&id=tv_showing:?tmsid=1340496000_SH002930050000_36_0_16493_1_2_9731&control=unknown&xstate=1&tc=0&cs=001DCE8D3DF1.head0.0..1400667088  destContext=watch:/?src=tv&id=tv_showing:?tmsid=1340496000_SH002930050000_36_0_16493_1_2_9731&control=unknown&xstate=1&tc=0&cs=001DCE8D3DF1.head0.0..1400667088 method=user duration=100",
				"1340496217.605411|141|alcservices[1035]: eventType=transportControl srcContext=watch:/?src=tv&id=tv_showing:?tmsid=1340496000_SH002930050000_36_0_16493_1_2_9731&control=unknown&xstate=1&tc=0&cs=001DCE8D3DF1.head0.0..1400667088  destContext=watch:/?src=tv&id=tv_showing:?tmsid=1340496000_SH002930050000_36_0_16493_1_2_9731&control=unknown&xstate=1&tc=0&cs=001DCE8D3DF1.head0.0..1400667088 method=user duration=1",
				"1340496217.605411|141|alcservices[1035]: eventType=transportControl srcContext=watch:/?src=tv&id=tv_showing:?tmsid=1340496000_SH002930050000_36_0_16493_1_2_9731&control=unknown&xstate=1&tc=0&cs=001DCE8D3DF1.head0.0..1400667088  destContext=watch:/?src=tv&id=tv_showing:?tmsid=1340496000_SH002930050000_36_0_16493_1_2_9731&control=unknown&xstate=1&tc=0&cs=001DCE8D3DF1.head0.0..1400667088 method=user duration=19" };
		test.assertOutput(
				"logs",
				inputMultiple,
				"standardized_dataset",
				new String[] {
						"(1340496217.605411,transportControl,user,1.3934660285832354,,1340496000_SH002930050000_36_0_16493_1_2_9731,1340496000_SH002930050000_36_0_16493_1_2_9731)",
						"(1340496217.605411,transportControl,user,-0.905752918579103,,1340496000_SH002930050000_36_0_16493_1_2_9731,1340496000_SH002930050000_36_0_16493_1_2_9731)",
						"(1340496217.605411,transportControl,user,-0.4877131100041324,,1340496000_SH002930050000_36_0_16493_1_2_9731,1340496000_SH002930050000_36_0_16493_1_2_9731)" });
	}
	@After
	public void done(){
		System.out.println("*** Done with the Shaw ETL tests ***");
	}

	public static void main(String[] args) {
		JUnitCore.main("com.tribece.feature.tests.TestShawETL");
	}
}
