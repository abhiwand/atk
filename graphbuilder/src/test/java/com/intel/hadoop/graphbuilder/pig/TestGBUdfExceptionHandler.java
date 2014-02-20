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

import com.intel.pig.udf.GBUdfException;
import com.intel.pig.udf.GBUdfExceptionHandler;
import org.apache.pig.EvalFunc;
import org.apache.pig.impl.PigContext;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestGBUdfExceptionHandler {
	EvalFunc<?> toRdfUdf;

	@Before
	public void setup() throws Exception {
		toRdfUdf = (EvalFunc<?>) PigContext
				.instantiateFuncFromSpec("com.intel.pig.udf.eval.RDF('OWL')");

		GBUdfExceptionHandler.handleError(toRdfUdf, new NullPointerException());
		GBUdfExceptionHandler.handleError(toRdfUdf, new RuntimeException());
		GBUdfExceptionHandler.handleError(toRdfUdf, new IOException(
				"test_exception"));

	}

	@Test
	public void runTests() throws IOException {

	}

	@Test(expected = RuntimeException.class)
	public void testFailureCase() throws IOException {
		GBUdfExceptionHandler.handleError(toRdfUdf, new IOException(
				new GBUdfException("test_exception")));
	}
}