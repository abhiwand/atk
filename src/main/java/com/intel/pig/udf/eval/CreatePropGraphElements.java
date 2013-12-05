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
 */package com.intel.pig.udf.eval;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.pig.EvalFunc;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement.GraphElementType;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.pig.data.GBTupleFactory;
import com.intel.pig.data.PropertyGraphElementTuple;
import com.intel.pig.udf.GBUdfExceptionHandler;

/**
 * \brief some documentation
 * 
 */
@MonitoredUDF(errorCallback = GBUdfExceptionHandler.class)
public class CreatePropGraphElements extends EvalFunc<Tuple> {
	private String tokenizationRule;

	public CreatePropGraphElements(String tokenizationRule) {
		this.tokenizationRule = tokenizationRule;
	}

	@Override
	public Tuple exec(Tuple input) throws IOException {
		Schema inputSchema = getInputSchema();
		
		/* assume that the user specified "f1" in the tokenizationRule, you can get the corresponding tuple with the following */
		int fieldPos = inputSchema.getPosition("f1");
		Object f1Tuple = input.get(fieldPos);

		PropertyGraphElementTuple t = (PropertyGraphElementTuple) new GBTupleFactory()
				.newTuple(1);
		PropertyGraphElementStringTypeVids p = new PropertyGraphElementStringTypeVids();
		Vertex<StringType> vertex = new Vertex<StringType>(new StringType(
				"test_vid"));
		vertex.setProperty("test_prop1", new StringType("test_val1"));
		vertex.setProperty("test_prop2", new StringType("test_val2"));
		p.init(GraphElementType.VERTEX, vertex);
		byte type = DataType.findType(t);
		System.out.println(">>>type " + type);
		System.out.println(p instanceof WritableComparable);
		System.out.println(p instanceof PropertyGraphElement);
		System.out.println(p instanceof Writable);
		t.set(0, p);
		// t.set(1, p);
		// t.set(2, p);
		// bag.add(t);
		return t;
	}

	// @Override
	// public Schema outputSchema(Schema input) {
	// Schema tuple = new Schema();
	// FieldSchema f1 = new FieldSchema("gb_tuple",
	// DataType.GENERIC_WRITABLECOMPARABLE);
	// tuple.add(f1);
	// return tuple;
	// // try {
	// // return new Schema(new Schema.FieldSchema(null, tuple, DataType.BAG));
	// // } catch (Exception e) {
	// // return null;
	// // }
	// }

}
