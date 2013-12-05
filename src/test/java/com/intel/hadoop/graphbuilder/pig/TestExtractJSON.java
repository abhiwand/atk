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
	String testJson = "{ \"store\": {\"book\": [{ \"category\": \"reference\", \"empty_field\":\"\", \"boolean_field\": true, \"null_field\": null, \"integer_field\": 2,\"author\": \"Nigel Rees\",\"title\": \"Sayings of the Century\", \"price\": 8.95},{ \"category\": \"fiction\",\"author\": \"Evelyn Waugh\", \"title\": \"Sword of Honour\", \"price\": 12.99,\"isbn\": \"0-553-21311-3\"}],\"bicycle\": {\"color\": \"red\",\"price\": 19.95}}}";

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

		//
		// result =
		// with(testJson).get("store.book.findAll{book -> book.price > 10 }[0]").toString();
		// System.out.println(">>> " + result);
		// String expected =
		// "{\"author\":\"Evelyn Waugh\",\"title\":\"Sword of Honour\",\"category\":\"fiction\",\"price\":12.99,\"isbn\":\"0-553-21311-3\"}";
		// System.out.println("<<<" + expected);
		// assertEquals("Result should have been " + expected + " !", result,
		// expected);
		//
		// >>> {author=Evelyn Waugh, title=Sword of Honour, category=fiction,
		// price=12.99, isbn=0-553-21311-3}
		// <<<{"author":"Evelyn Waugh","title":"Sword of Honour","category":"fiction","price":12.99,"isbn":"0-553-21311-3"}

		//
		// result =
		// with(testJson).get("store.book.findAll{book -> book.isbn}").toString();
		// String expected =
		// "{\"author\":\"Evelyn Waugh\",\"title\":\"Sword of Honour\",\"category\":\"fiction\",\"price\":12.99,\"isbn\":\"0-553-21311-3\"}";
		// System.out.println(">>> " + result);
		// System.out.println("<<<" + expected);
		// assertEquals("Result should have been " + expected + " !", result,
		// expected);
		// >>> [{author=Evelyn Waugh, title=Sword of Honour, category=fiction,
		// price=12.99, isbn=0-553-21311-3}]
		// <<<{"author":"Evelyn Waugh","title":"Sword of Honour","category":"fiction","price":12.99,"isbn":"0-553-21311-3"}

		// result = with(testJson).get("store.book.author").toString();
		// assertEquals("Result should have been 8.95!", result, "8.95");

	}

	@Test(expected = IllegalArgumentException.class)
	public void testFailureCase1() throws IOException {
		System.out.println("Testing failure cases");

		String testQuery = "store.book.author";
		String[] inputTuple = { testJson, testQuery };
		Tuple inTuple = TupleFactory.getInstance().newTuple(
				Arrays.asList(inputTuple));
		testFn.exec(inTuple);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFailureCase2() throws IOException {
		System.out.println("Testing failure cases");

		String testQuery = "store.book.findAll{book -> book.price}";
		String[] inputTuple = { testJson, testQuery };
		Tuple inTuple = TupleFactory.getInstance().newTuple(
				Arrays.asList(inputTuple));
		testFn.exec(inTuple);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFailureCase3() throws IOException {
		System.out.println("Testing failure cases");

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