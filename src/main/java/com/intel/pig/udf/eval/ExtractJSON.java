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
package com.intel.pig.udf.eval;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.intel.pig.udf.GBUdfExceptionHandler;

import static com.jayway.restassured.path.json.JsonPath.*;

/**
 * UDF for extracting fields from (potentially complex & nested) JSON documents
 * with JSONPath expressions See <a
 * href="http://code.google.com/p/json-path/">JSONPath</a>
 */
@MonitoredUDF(errorCallback = GBUdfExceptionHandler.class)
public class ExtractJSON extends EvalFunc<DataByteArray> {

	@Override
	public DataByteArray exec(Tuple input) throws IOException {

		if (input == null || input.size() == 0) {
			warn("Input tuple is null or empty", PigWarning.UDF_WARNING_1);
			return null;
		}

		String inString = (String) input.get(0);
		String query = (String) input.get(1);

		Object queryResult = null;

		try {
			queryResult = with(inString).get(query);
		} catch (IllegalArgumentException e) {
			warn("Failed to process input; error - " + e.getMessage(),
					PigWarning.UDF_WARNING_1);
			return null;
		}

		if (queryResult == null) {
			return new DataByteArray("");
		} else if (queryResult instanceof String) {
			String result = (String) queryResult;
			return new DataByteArray(result);
		} else if (queryResult instanceof Boolean) {
			Boolean result = (Boolean) queryResult;
			return new DataByteArray(String.valueOf(result));
		} else if (queryResult instanceof Double) {
			Double result = (Double) queryResult;
			return new DataByteArray(String.valueOf(result));
		} else if (queryResult instanceof Float) {
			Float result = (Float) queryResult;
			return new DataByteArray(String.valueOf(result));
		} else if (queryResult instanceof Integer) {
			Integer result = (Integer) queryResult;
			return new DataByteArray(String.valueOf(result));
		} else if (queryResult instanceof Long) {
			Long result = (Long) queryResult;
			return new DataByteArray(String.valueOf(result));
		} else if (queryResult instanceof BigInteger) {
			BigInteger result = (BigInteger) queryResult;
			return new DataByteArray(result.toString());
		} else if (queryResult instanceof BigDecimal) {
			BigDecimal result = (BigDecimal) queryResult;
			return new DataByteArray(result.toString());
		} else if (queryResult instanceof List) {
			List result = (List) queryResult;
			System.out.println("got a list result " + result.size());
			/*
			 * we only let the query expression to return a single primitive
			 * value
			 */
			if (result.size() == 1) {
				Object o = result.get(0);
				return new DataByteArray(o.toString());
			}
		}

		/*
		 * OK, we have gone through all the data types and none of them fits.
		 */

		String errorMessage = null;

		if (queryResult instanceof List) {
			errorMessage = "The query returned multiple results, it has to return a single value.";
		} else {
			errorMessage = "The query returned a type that is not supported: "
					+ queryResult.getClass();
		}
		throw new IllegalArgumentException(errorMessage);
	}
}
