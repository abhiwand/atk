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
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.intel.hadoop.graphbuilder.graphelements.GraphElement;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import com.intel.pig.data.GBTupleFactory;
import com.intel.pig.data.PropertyGraphElementTuple;
import com.intel.pig.udf.GBUdfExceptionHandler;

/**
 * \brief MergeDuplicateGraphElements ... combine duplicate property graph
 * elements into a single property graph element.
 * 
 * The intended input for this function is a tuple from the output of GROUP
 * operation performed on pairs of the form (ID, property graph element) where
 * ID is the output of {@code GetPropGraphElementID} called on the property
 * graph element.
 * 
 * Example: DEFINE getPropGraphEltID
 * com.intel.pig.udf.eval.GetPropGraphElementID; pgeLabeled = FOREACH pge
 * GENERATE (getPropGraphEltID(*)), $0; grouped = GROUP pgeLabeled by $0; DEFINE
 * MergeDuplicateGraphElements
 * com.intel.pig.udf.eval.MergeDuplicateGraphElements; merged = FOREACH grouped
 * GENERATE(MergeDuplicateGraphElements(*));
 * 
 * This routine merges properties by simply merging the property lists by brute
 * force.
 * 
 * @see PropertyGraphElementTuple
 * @see com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement
 */
@MonitoredUDF(errorCallback = GBUdfExceptionHandler.class, duration = 30, timeUnit = TimeUnit.MINUTES)
public class AlgMergeDuplicateGraphElements extends EvalFunc<Tuple> implements
		Algebraic {

	private SerializedGraphElement graphElementFromGroupedBagEntry(Tuple tuple)
			throws ExecException {
		return (SerializedGraphElement) tuple.get(1);
	}

	/**
	 * Combine duplicate property graph elements into a single property graph
	 * element.
	 * 
	 * @param input
	 * @return Tuple containing the single property graph element that contains
	 *         the merged properties of all copies of this element.
	 * @throws IOException
	 */
	@Override
	public Tuple exec(Tuple input) throws IOException {
		return merge(input);
	}

	/**
	 * Provide return type information back to the Pig level.
	 * 
	 * @param input
	 *            ignored
	 * @return Schema for a property graph element packed into a unary tuple.
	 */
	@Override
	public Schema outputSchema(Schema input) {
		Schema pgeTuple = new Schema(new Schema.FieldSchema(
				"property graph element (unary tuple)", DataType.TUPLE));

		return pgeTuple;
	}

	public String getInitial() {
		return Initial.class.getName();
	}

	public String getIntermed() {
		return Intermed.class.getName();
	}

	public String getFinal() {
		return Final.class.getName();
	}

	static public class Initial extends EvalFunc<Tuple> {
		public Tuple exec(Tuple input) throws IOException {
			System.out.println("Initial ");
			return merge(input);
		}
	}

	static public class Intermed extends EvalFunc<Tuple> {
		public Tuple exec(Tuple input) throws IOException {
			System.out.println("Intermed ");
			return merge(input);
		}
	}

	static public class Final extends EvalFunc<Tuple> {
		public Tuple exec(Tuple input) throws IOException {
			System.out.println("Intermed ");
			return merge(input);
		}
	}

	static protected Tuple merge(Tuple input) throws ExecException {
		DataBag valueBag = (DataBag) input.get(1);
		PropertyGraphElementTuple outTuple = (PropertyGraphElementTuple) new GBTupleFactory()
				.newTuple(1);
		Iterator it = valueBag.iterator();

		Tuple firstTuple = (Tuple) it.next();
		// the bag contains at least one element
		SerializedGraphElement serializedGraphElement = (SerializedGraphElement) (firstTuple
				.get(1));

		GraphElement graphElement = serializedGraphElement.graphElement();

		if (graphElement == null) {
			// warn("Null property graph element", PigWarning.UDF_WARNING_1);
			return TupleFactory.getInstance().newTuple();
		}

		while (it.hasNext()) {
			Tuple t = (Tuple) it.next();
			GraphElement dupGraphElement = ((SerializedGraphElement) t.get(1))
					.graphElement();
			graphElement.getProperties().mergeProperties(
					dupGraphElement.getProperties());
		}

		outTuple.set(0, serializedGraphElement);
		System.out.println("returning " + serializedGraphElement);
		return outTuple;
	}
}
