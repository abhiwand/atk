/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intel.pig.udf.eval;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.builtin.REGEX_EXTRACT_ALL;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.intel.pig.udf.GBUdfExceptionHandler;

/**
 * \brief RegexExtractAllMatches returns a bag of all matched strings given a
 * string and a regular expression. <br/>
 * This implementation is based on Pig's built in {@link REGEX_EXTRACT_ALL} UDF.
 */
@MonitoredUDF(errorCallback = GBUdfExceptionHandler.class)
public class RegexExtractAllMatches extends EvalFunc<DataBag> {
	private static TupleFactory tupleFactory = TupleFactory.getInstance();

	@Override
	public DataBag exec(Tuple input) throws IOException {
		if (input.size() != 2) {
			String msg = "RegexExtractAllMatches : Only 2 parameters are allowed.";
			throw new IOException(msg);
		}

		if (input.get(0) == null)
			return null;
		try {
			if (!input.get(1).equals(mExpression)) {
				try {
					mExpression = (String) input.get(1);
					mPattern = Pattern.compile(mExpression);
				} catch (Exception e) {
					String msg = "RegexExtractAllMatches : Malformed Regular expression : "
							+ input.get(1);
					throw new IOException(msg);
				}
			}
		} catch (NullPointerException e) {
			String msg = "RegexExtractAllMatches : Regular expression is null";
			throw new IOException(msg);
		}

		Matcher m = mPattern.matcher((String) input.get(0));

		DataBag result = DefaultBagFactory.getInstance().newDefaultBag();

		while (m.find()) {
			Tuple matchedString = tupleFactory.newTuple(1);
			matchedString.set(0, m.group(1));
			result.add(matchedString);
		}

		return result;
	}

	String mExpression = null;
	Pattern mPattern = null;

	@Override
	public Schema outputSchema(Schema input) {
		try {
			return new Schema(new Schema.FieldSchema(getSchemaName(this
					.getClass().getName().toLowerCase(), input), DataType.BAG));
		} catch (Exception e) {
			return null;
		}
	}
}
