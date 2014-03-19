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
import org.apache.pig.PigWarning;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.lang.StringBuilder;


import com.facebook.presto.operator.scalar.JsonExtract;
import io.airlift.slice.Slices;
import io.airlift.slice.Slice;
import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.BooleanUtils;


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
                String type  = "String"; /* default type */
                if (input.size() > 2)
                    type = (String) input.get(2);

		String queryResult = null;

                Slice squery = Slices.copiedBuffer(query, UTF_8);
                Slice sinString = Slices.copiedBuffer(inString, UTF_8);

		try {
			if (type.equals("List") || type.equals("JSON")) {
				queryResult = JsonExtract.extractJson(sinString, squery).toString(UTF_8);
				warn("JoyeshCalling List or JSON: Result " + queryResult,PigWarning.UDF_WARNING_1);
			} else {
				queryResult = JsonExtract.extractScalar(sinString, squery).toString(UTF_8);
				warn("JoyeshCalling remaining: Result " + queryResult,PigWarning.UDF_WARNING_1);
			}
		} catch (IllegalArgumentException e) {
			warn("JoyeshCalling IAE",PigWarning.UDF_WARNING_1);
			warn("Failed to process input; error - " + e.getMessage(),
					PigWarning.UDF_WARNING_1);
			return new DataByteArray("");
		} catch (IOException e) {
			warn("JoyeshCalling IOE",PigWarning.UDF_WARNING_1);
			warn("Failed to process input; error - " + e.getMessage(),
					PigWarning.UDF_WARNING_1);
			return new DataByteArray("");
		} catch (Exception e) {
			warn("JoyeshCalling E",PigWarning.UDF_WARNING_1);
			warn("Failed to process input; error - " + e.getMessage(),
					PigWarning.UDF_WARNING_1);
			return new DataByteArray("");
		}

		/* At this point, we have a queryResult */

		/* null fields are supported in json */	/* HOW WILL THIS WORK -- Will this ever be null */
		if (queryResult == null)
			return new DataByteArray("");
		System.out.println("Result is " + queryResult);

                try {

			if (type.equals("Integer")) {
				Integer result = NumberUtils.createInteger(queryResult);
				return new DataByteArray(String.valueOf(result));
			} else if (type.equals("Boolean")) {
				Boolean result = BooleanUtils.toBooleanObject(queryResult);
				return new DataByteArray(String.valueOf(result));
			} else if (type.equals("Double")) {
				Double result = NumberUtils.createDouble(queryResult);
				return new DataByteArray(String.valueOf(result));
			} else if (type.equals("Float")) {
				Float result = NumberUtils.createFloat(queryResult);
				return new DataByteArray(String.valueOf(result));
			} else if (type.equals("Long")) {
				Long result = NumberUtils.createLong(queryResult);
				return new DataByteArray(String.valueOf(result));
			} else if (type.equals("BigInteger")) {
				BigInteger result = NumberUtils.createBigInteger(queryResult);
				return new DataByteArray(result.toString());
			} else if (type.equals("BigDecimal")) {
				BigDecimal result = NumberUtils.createBigDecimal(queryResult);
				return new DataByteArray(result.toString());
			} else {
				String result = (String) queryResult;
				return new DataByteArray(result);
			}

		} catch (Exception e) {
			warn("JoyeshCalling Catching Final Exception",PigWarning.UDF_WARNING_1);
			warn("Failed to process input; error - " + e.getMessage(),
					PigWarning.UDF_WARNING_1);
			return new DataByteArray("");
		}
	}
}
