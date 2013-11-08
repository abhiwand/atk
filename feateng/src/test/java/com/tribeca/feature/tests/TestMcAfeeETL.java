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
package com.tribeca.feature.tests;

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
public class TestMcAfeeETL {

	@Before
	public void setup() {
		System.out.println("*** Starting McAfee ETL tests. ***");
	}

	@Test
	public void testParsingMcAfeeDataset() throws IOException, ParseException {
		System.out.println("Testing pig/parser.pig");
		String[] paramFiles = { "pig/mcafee.params" };
		PigTest test = new PigTest("pig/parser.pig", null, paramFiles);
		String[] input = { "1375054084 sip:1196035343 rpr:1 sn:lvs-gti-rep10 pktsz:384 bufsz:2044 cuuid:1aF1SfB9R868blkuolDbwA== id:928534615 sercert:TUNBRkVFLURJQU1PTkQ= devid:MjIyODEyZDgtZTU4MS00ZWI1LWFiMzYtYjVlZTQ4ZDExMmM5 cliid:MQ== prn:QU1Db3Jl prv:MS4w uchd:0 uchi:0 ct:9 seq:1056964608 catset:0 sid:0 urlcat:UlVMRVMuU0VDVVJFU1RVRElFUy5DT00vT1NTL1JVTEUzMy5BU1A/QlJBTkQ9T1BJTklPTlNRVUFSRSZCSUQ9VEpOU0FWWVVOWFlPQThNMU9KNiRCRSZFWEU9T1BOU1FSLkVYRSZMT0NBTEU9MTAzMyZDT1VOVFJZPVVTJlBMQVRGT1JNPVdJTkRPV1MmT1M9NiZPU01JTk9SPTEmQklUUz02NCZOVU1EQVlTPTEyNSZDVVI9MS4zLjMzNS4zMzc=,,2162688,,107,21,0 url_rgx: rusec:517" };
		String[] output = { "(1196035343,1,lvs-gti-rep10,384,2044,1aF1SfB9R868blkuolDbwA==)" };
		test.assertOutput("logs", input, "parsed_val", output);
	}

	@Test
	public void testCleaningMcAfeeDataset() throws IOException, ParseException {
		System.out.println("Testing pig/cleaner.pig");
		String[] paramFiles = { "pig/mcafee.params" };
		PigTest test = new PigTest("pig/cleaner.pig", null, paramFiles);
		// no sn (server name) field in the input so it should be filtered out
		String[] inputWithoutSnField = { "1375054084 sip:1196035343 rpr:1 pktsz:384 bufsz:2044 cuuid:1aF1SfB9R868blkuolDbwA== id:928534615 sercert:TUNBRkVFLURJQU1PTkQ= devid:MjIyODEyZDgtZTU4MS00ZWI1LWFiMzYtYjVlZTQ4ZDExMmM5 cliid:MQ== prn:QU1Db3Jl prv:MS4w uchd:0 uchi:0 ct:9 seq:1056964608 catset:0 sid:0 urlcat:UlVMRVMuU0VDVVJFU1RVRElFUy5DT00vT1NTL1JVTEUzMy5BU1A/QlJBTkQ9T1BJTklPTlNRVUFSRSZCSUQ9VEpOU0FWWVVOWFlPQThNMU9KNiRCRSZFWEU9T1BOU1FSLkVYRSZMT0NBTEU9MTAzMyZDT1VOVFJZPVVTJlBMQVRGT1JNPVdJTkRPV1MmT1M9NiZPU01JTk9SPTEmQklUUz02NCZOVU1EQVlTPTEyNSZDVVI9MS4zLjMzNS4zMzc=,,2162688,,107,21,0 url_rgx: rusec:517" };
		test.assertOutput("logs", inputWithoutSnField, "cleaned_data",
				new String[] { "" });
		// with the sn field in the input the output should be the same as
		// parsing output
		String[] inputWithSnField = { "1375054084 sip:1196035343 rpr:1 sn:lvs-gti-rep10 pktsz:384 bufsz:2044 cuuid:1aF1SfB9R868blkuolDbwA== id:928534615 sercert:TUNBRkVFLURJQU1PTkQ= devid:MjIyODEyZDgtZTU4MS00ZWI1LWFiMzYtYjVlZTQ4ZDExMmM5 cliid:MQ== prn:QU1Db3Jl prv:MS4w uchd:0 uchi:0 ct:9 seq:1056964608 catset:0 sid:0 urlcat:UlVMRVMuU0VDVVJFU1RVRElFUy5DT00vT1NTL1JVTEUzMy5BU1A/QlJBTkQ9T1BJTklPTlNRVUFSRSZCSUQ9VEpOU0FWWVVOWFlPQThNMU9KNiRCRSZFWEU9T1BOU1FSLkVYRSZMT0NBTEU9MTAzMyZDT1VOVFJZPVVTJlBMQVRGT1JNPVdJTkRPV1MmT1M9NiZPU01JTk9SPTEmQklUUz02NCZOVU1EQVlTPTEyNSZDVVI9MS4zLjMzNS4zMzc=,,2162688,,107,21,0 url_rgx: rusec:517" };
		String[] output = { "(1196035343,1,lvs-gti-rep10,384,2044,1aF1SfB9R868blkuolDbwA==)" };
		test.assertOutput("logs", inputWithSnField, "cleaned_data", output);
	}

	@Test
	public void testStandardizeMcAfeeDataset() throws IOException,
			ParseException {
		System.out.println("Testing pig/standardizer.pig.pig");
		String[] paramFiles = { "pig/mcafee.params" };
		PigTest test = new PigTest("pig/standardizer.pig", null, paramFiles);
		// standardize based on duration, see mcafee.params
		String[] input = { "1375054084 sip:1196035343 rpr:1 sn:lvs-gti-rep10 pktsz:384 bufsz:2044 cuuid:1aF1SfB9R868blkuolDbwA== id:928534615 sercert:TUNBRkVFLURJQU1PTkQ= devid:MjIyODEyZDgtZTU4MS00ZWI1LWFiMzYtYjVlZTQ4ZDExMmM5 cliid:MQ== prn:QU1Db3Jl prv:MS4w uchd:0 uchi:0 ct:9 seq:1056964608 catset:0 sid:0 urlcat:UlVMRVMuU0VDVVJFU1RVRElFUy5DT00vT1NTL1JVTEUzMy5BU1A/QlJBTkQ9T1BJTklPTlNRVUFSRSZCSUQ9VEpOU0FWWVVOWFlPQThNMU9KNiRCRSZFWEU9T1BOU1FSLkVYRSZMT0NBTEU9MTAzMyZDT1VOVFJZPVVTJlBMQVRGT1JNPVdJTkRPV1MmT1M9NiZPU01JTk9SPTEmQklUUz02NCZOVU1EQVlTPTEyNSZDVVI9MS4zLjMzNS4zMzc=,,2162688,,107,21,0 url_rgx: rusec:517" };
		String[] output = { "(1196035343,1,lvs-gti-rep10,,2044,1aF1SfB9R868blkuolDbwA==)" };
		/* standardization undefined for a single record */
		test.assertOutput("logs", input, "standardized_dataset", output);

		String[] inputMultiple = {
				"1375054084 sip:1196035343 rpr:1 sn:lvs-gti-rep10 pktsz:100 bufsz:2044 cuuid:1aF1SfB9R868blkuolDbwA== id:928534615 sercert:TUNBRkVFLURJQU1PTkQ= devid:MjIyODEyZDgtZTU4MS00ZWI1LWFiMzYtYjVlZTQ4ZDExMmM5 cliid:MQ== prn:QU1Db3Jl prv:MS4w uchd:0 uchi:0 ct:9 seq:1056964608 catset:0 sid:0 urlcat:UlVMRVMuU0VDVVJFU1RVRElFUy5DT00vT1NTL1JVTEUzMy5BU1A/QlJBTkQ9T1BJTklPTlNRVUFSRSZCSUQ9VEpOU0FWWVVOWFlPQThNMU9KNiRCRSZFWEU9T1BOU1FSLkVYRSZMT0NBTEU9MTAzMyZDT1VOVFJZPVVTJlBMQVRGT1JNPVdJTkRPV1MmT1M9NiZPU01JTk9SPTEmQklUUz02NCZOVU1EQVlTPTEyNSZDVVI9MS4zLjMzNS4zMzc=,,2162688,,107,21,0 url_rgx: rusec:517",
				"1375054084 sip:1196035343 rpr:1 sn:lvs-gti-rep10 pktsz:1 bufsz:2044 cuuid:1aF1SfB9R868blkuolDbwA== id:928534615 sercert:TUNBRkVFLURJQU1PTkQ= devid:MjIyODEyZDgtZTU4MS00ZWI1LWFiMzYtYjVlZTQ4ZDExMmM5 cliid:MQ== prn:QU1Db3Jl prv:MS4w uchd:0 uchi:0 ct:9 seq:1056964608 catset:0 sid:0 urlcat:UlVMRVMuU0VDVVJFU1RVRElFUy5DT00vT1NTL1JVTEUzMy5BU1A/QlJBTkQ9T1BJTklPTlNRVUFSRSZCSUQ9VEpOU0FWWVVOWFlPQThNMU9KNiRCRSZFWEU9T1BOU1FSLkVYRSZMT0NBTEU9MTAzMyZDT1VOVFJZPVVTJlBMQVRGT1JNPVdJTkRPV1MmT1M9NiZPU01JTk9SPTEmQklUUz02NCZOVU1EQVlTPTEyNSZDVVI9MS4zLjMzNS4zMzc=,,2162688,,107,21,0 url_rgx: rusec:517",
				"1375054084 sip:1196035343 rpr:1 sn:lvs-gti-rep10 pktsz:19 bufsz:2044 cuuid:1aF1SfB9R868blkuolDbwA== id:928534615 sercert:TUNBRkVFLURJQU1PTkQ= devid:MjIyODEyZDgtZTU4MS00ZWI1LWFiMzYtYjVlZTQ4ZDExMmM5 cliid:MQ== prn:QU1Db3Jl prv:MS4w uchd:0 uchi:0 ct:9 seq:1056964608 catset:0 sid:0 urlcat:UlVMRVMuU0VDVVJFU1RVRElFUy5DT00vT1NTL1JVTEUzMy5BU1A/QlJBTkQ9T1BJTklPTlNRVUFSRSZCSUQ9VEpOU0FWWVVOWFlPQThNMU9KNiRCRSZFWEU9T1BOU1FSLkVYRSZMT0NBTEU9MTAzMyZDT1VOVFJZPVVTJlBMQVRGT1JNPVdJTkRPV1MmT1M9NiZPU01JTk9SPTEmQklUUz02NCZOVU1EQVlTPTEyNSZDVVI9MS4zLjMzNS4zMzc=,,2162688,,107,21,0 url_rgx: rusec:517" };
		test.assertOutput(
				"logs",
				inputMultiple,
				"standardized_dataset",
				new String[] {
						"(1196035343,1,lvs-gti-rep10,1.3934660285832354,2044,1aF1SfB9R868blkuolDbwA==)",
						"(1196035343,1,lvs-gti-rep10,-0.905752918579103,2044,1aF1SfB9R868blkuolDbwA==)",
						"(1196035343,1,lvs-gti-rep10,-0.4877131100041324,2044,1aF1SfB9R868blkuolDbwA==)" });
	}

	@After
	public void done() {
		System.out.println("*** Done with the McAfee ETL tests ***");
	}

	public static void main(String[] args) {
		JUnitCore.main("com.tribece.feature.tests.TestMcAfeeETL");
	}
}
