/* Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * For more about this software visit:
 *      http://www.01.org/GraphBuilder
 */
package com.intel.hadoop.graphbuilder.pig;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.JUnitCore;

public class TestExtractJSON {
	EvalFunc<?> testFn;
	String testJson = "{ \"Name\": \"T-Shirt 2\", \"Sizes\": [ { \"Size\": \"Large\", \"Price\": 20.00 }, { \"Size\": \"Medium\", \"Price\": 11.00 }, { \"Size\": \"Small\", \"Price\": 5.00 } ], \"Colors\": [ \"Black\", \"White\" ]}";

	@Before
	public void setup() throws Exception {
		System.out.println("*** Starting ExtractJSON tests. ***");
		testFn = (EvalFunc<?>) PigContext
				.instantiateFuncFromSpec("com.intel.pig.udf.eval.ExtractJSON");
		System.out.println(testJson);
	}

	@Test
	public void testSuccessCases() throws IOException {
		System.out.println("Testing success cases");

		String testQuery = "Sizes[0].Price";
		String[] inputTuple = { testJson, testQuery };
		Tuple inTuple = TupleFactory.getInstance().newTuple(
				Arrays.asList(inputTuple));

		String result = ((DataByteArray) testFn.exec(inTuple)).toString();
		assertEquals("Price is not correct!", result,
				"20.0");

		inTuple.set(1, "Sizes.size()");
		result = ((DataByteArray) testFn.exec(inTuple)).toString();
		assertEquals("Size is not correct!", result, "3");

		inTuple.set(1, "Colors[0]");
		result = ((DataByteArray) testFn.exec(inTuple)).toString();
		assertEquals("Color is not correct!", result, "Black");

		inTuple.set(1, "Colors.size()");
		result = ((DataByteArray) testFn.exec(inTuple)).toString();
		assertEquals("Color size is not correct!", result, "2");

		inTuple.set(1, "Sizes.Price.min()");
		result = ((DataByteArray) testFn.exec(inTuple)).toString();
		assertEquals("Cheapest price is not correct!", result, "5.0");

		inTuple.set(1, "Sizes.findAll{Sizes -> Sizes.Price>18}.Size[0]");
		result = ((DataByteArray) testFn.exec(inTuple)).toString();
		assertEquals("Size is not correct!", result, "Large");

	}

	@Test(expected = IllegalArgumentException.class)
	public void testFailureCase1() throws IOException {
		System.out.println("Testing failure cases");

		String testQuery = "Sizes.Price";
		String[] inputTuple = { testJson, testQuery };
		Tuple inTuple = TupleFactory.getInstance().newTuple(
				Arrays.asList(inputTuple));
		testFn.exec(inTuple);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFailureCase2() throws IOException {
		System.out.println("Testing failure cases");

		String testQuery = "Colors";
		String[] inputTuple = { testJson, testQuery };
		Tuple inTuple = TupleFactory.getInstance().newTuple(
				Arrays.asList(inputTuple));
		testFn.exec(inTuple);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFailureCase3() throws IOException {
		System.out.println("Testing failure cases");

		String testQuery = "Sizes.findAll{Sizes -> Sizes.Price>5}";
		String[] inputTuple = { testJson, testQuery };
		Tuple inTuple = TupleFactory.getInstance().newTuple(
				Arrays.asList(inputTuple));
		testFn.exec(inTuple);
	}
	
	@After
	public void done() {
		System.out.println("*** Done with the ExtractJSON tests ***");
	}

	public static void main(String[] args) {
		JUnitCore.main("TestExtractJSON");
	}
}