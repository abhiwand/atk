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
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.pig.data.PropertyGraphElementTuple;

/**
 * \brief some documentation
 * 
 */
public class TORDF extends EvalFunc<Tuple> {

	@Override
	public Tuple exec(Tuple input) throws IOException {
		PropertyGraphElementTuple t = (PropertyGraphElementTuple) input.get(0);
		List<Object> allGraphElements = t.getAll();
		for (Object o : allGraphElements) {
			PropertyGraphElement e = (PropertyGraphElement) o;
			System.out.println(e.graphElementType());
		}
		Tuple rdfTuple = TupleFactory.getInstance().newTuple(3);// an RDF tuple
																// is a triple
		rdfTuple.set(0, "subject");
		rdfTuple.set(1, "predicate");
		rdfTuple.set(2, "object");
		return rdfTuple;
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
