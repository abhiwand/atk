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

package com.intel.pig.udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.JUnitCore;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * @author Nezih Yigitbasi
 * @email nezih.yigitbasi@intel.com
 */
public class TestExtractJSON {
	EvalFunc<?> testFn;
	String testJson = "{ \"store\": {\"book\": [{ \"category\": \"reference\", \"empty_field\":\"\", \"boolean_field\": true, \"null_field\": null, \"integer_field\": 2,\"author\": \"Nigel Rees\",\"title\": \"Sayings of the Century\", \"price\": 8.95},{ \"category\": \"fiction\",\"author\": \"Evelyn Waugh\", \"title\": \"Sword of Honour\", \"price\": 12.99,\"isbn\": \"0-553-21311-3\"}],\"bicycle\": {\"color\": \"red\",\"price\": 19.95}}}";

	@Before
	public void setup() throws Exception {
		System.out.println("*** Starting ExtractJSON tests. ***");
		testFn = (EvalFunc<?>) PigContext
				.instantiateFuncFromSpec("com.intel.pig.udf.ExtractJSON");
		System.out.println(testJson);
	}

	@Test
	public void testSuccessCases() throws IOException {
		System.out.println("Testing success cases");

		String testQuery = "store.book[0].author";
		String[] inputTuple = { testJson, testQuery };
		Tuple inTuple = TupleFactory.getInstance().newTuple(
				Arrays.asList(inputTuple));

		String result = ((DataByteArray) testFn.exec(inTuple)).toString();
		assertEquals("First book's author does not match!", result,
				"Nigel Rees");

		inTuple.set(1, "store.book[0].empty_field");
		result = ((DataByteArray) testFn.exec(inTuple)).toString();
		assertEquals("Result should have been empty string!", result, "");

		inTuple.set(1, "store.book[0].price");
		result = ((DataByteArray) testFn.exec(inTuple)).toString();
		assertEquals("Result should have been 8.95!", result, "8.95");

		inTuple.set(1, "store.book[0].integer_field");
		result = ((DataByteArray) testFn.exec(inTuple)).toString();
		assertEquals("Result should have been 2!", result, "2");

		inTuple.set(1, "store.book[0].boolean_field");
		result = ((DataByteArray) testFn.exec(inTuple)).toString();
		assertEquals("Result should have been true!", result, "true");

		inTuple.set(1, "store.book.findAll{book -> book.price}[0].price");
		result = ((DataByteArray) testFn.exec(inTuple)).toString();
		assertEquals("Result should have been 8.95!", result, "8.95");

		inTuple.set(1, "store.book.size()");
		result = ((DataByteArray) testFn.exec(inTuple)).toString();
		assertEquals("Result should have been 2!", result, "2");

	}

	public void testFailureCase1() throws IOException {
		System.out.println("Testing failure cases - 1");
		String testQuery = "store.book[*].author";
		String[] inputTuple = { testJson, testQuery };
		Tuple inTuple = TupleFactory.getInstance().newTuple(
				Arrays.asList(inputTuple));
		DataByteArray result = (DataByteArray) testFn.exec(inTuple);
		assertEquals("Result should have been null!", result, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFailureCase2() throws IOException {
		System.out.println("Testing failure cases - 2");

		String testQuery = "store.book.findAll{book -> book.price}";
		String[] inputTuple = { testJson, testQuery };
		Tuple inTuple = TupleFactory.getInstance().newTuple(
				Arrays.asList(inputTuple));
		testFn.exec(inTuple);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFailureCase3() throws IOException {
		System.out.println("Testing failure cases - 3");

		String testQuery = "store.book.findAll{book -> book.author}";
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