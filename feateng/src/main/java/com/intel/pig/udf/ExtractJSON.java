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
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import static com.jayway.restassured.path.json.JsonPath.*;

/**
 * UDF for extracting fields from (potentially complex & nested) JSON documents
 * with JSONPath expressions See <a
 * href="http://code.google.com/p/json-path/">JSONPath</a>
 */
@MonitoredUDF(errorCallback = UDFExceptionHandler.class)
public class ExtractJSON extends EvalFunc<DataByteArray> {

	@Override
	public DataByteArray exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;

		/*
		 * our transform scripts return bytearrays so in chained transform calls
		 * we will receive byte arrays
		 */
		Object in = input.get(0);
		String inString = null;
		if (in instanceof DataByteArray)
			inString = ((DataByteArray) in).toString();
		else
			inString = (String) in;

		String query = (String) input.get(1);

		Object queryResult = null;
		
		try{
			queryResult= with(inString).get(query);
		}catch(IllegalArgumentException e){
            warn("Failed to process input; error - " + e.getMessage(), PigWarning.UDF_WARNING_1);
            return null;
		}
		
		/* null fields are supported in json */
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
		}
		else if (queryResult instanceof BigInteger) {
			BigInteger result = (BigInteger) queryResult;
			return new DataByteArray(result.toString());
		} else if (queryResult instanceof BigDecimal) {
			BigDecimal result = (BigDecimal) queryResult;
			return new DataByteArray(result.toString());
		} else if (queryResult instanceof List) {
			
			List result = (List) queryResult;
			System.out.println("got a list result "  + result.size());
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
		 * OK, we have gone through all the data types. If none of them fit,
		 * throw an exception.
		 */

		String errorMessage = null;

		if (queryResult instanceof List) {
			errorMessage = "The query returned multiple results, it has to return a single value.";
		} else {
			errorMessage = "The query returned a type that is not supported: "
					+ queryResult.getClass();
		}
		
		System.out.println("throwing illegalarg ex");

		throw new IllegalArgumentException(errorMessage);
	}
}
