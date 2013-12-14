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

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.pig.data.PropertyGraphElementTuple;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;

public class TestTORDF {
	EvalFunc<?> toRdfUdf;

	@Before
	public void setup() throws Exception {
		System.out.println("*** Starting TORDF tests. ***");
		toRdfUdf = (EvalFunc<?>) PigContext
				.instantiateFuncFromSpec("com.intel.pig.udf.eval.TORDF('OWL')");
	}

	@Test
	public void runTests() throws IOException {
		SerializedGraphElementStringTypeVids serializedGraphElement = new SerializedGraphElementStringTypeVids();
		Vertex<StringType> vertex = new Vertex<StringType>(new StringType(
				"test_vertex"));
		serializedGraphElement.init(vertex);
		vertex.setProperty("p-1", new StringType("v-1"));
		vertex.setLabel(new StringType("vertex_label"));

		PropertyGraphElementTuple t = new PropertyGraphElementTuple(1);
		t.set(0, serializedGraphElement);

		DataBag result = (DataBag) toRdfUdf.exec(t);
		assertEquals("Returned bag size should have been 1", result.size(), 1);
		Iterator<Tuple> iter = result.iterator();

		while (iter.hasNext()) {
			Tuple resultTuple = iter.next();
			String rdfStatement = (String) resultTuple.get(0);
			assertEquals(
					"RDF statement mismatch",
					rdfStatement,
					"http://www.w3.org/2002/07/owl#test_vertex http://www.w3.org/2002/07/owl#p-1 v-1 .");
		}

		serializedGraphElement = new SerializedGraphElementStringTypeVids();
		Edge<StringType> edge = new Edge<StringType>(new StringType("src"),
				new StringType("target"), new StringType("edge_label"));

		serializedGraphElement.init(edge);
		edge.setProperty("p-1", new StringType("v-1"));

		t = new PropertyGraphElementTuple(1);
		t.set(0, serializedGraphElement);

		result = (DataBag) toRdfUdf.exec(t);
		assertEquals("Returned bag size should have been 3", result.size(), 3);

		iter = result.iterator();
		while (iter.hasNext()) {
			Tuple resultTuple = iter.next();
			String rdfStatement = (String) resultTuple.get(0);
			if (rdfStatement.contains("#p-1")) {
				assertEquals(
						"RDF statement mismatch",
						rdfStatement,
						"http://www.w3.org/2002/07/owl#edge_label http://www.w3.org/2002/07/owl#p-1 v-1 .");
			} else if (rdfStatement.contains("#target")) {
				assertEquals(
						"RDF statement mismatch",
						rdfStatement,
						"http://www.w3.org/2002/07/owl#edge_label http://www.w3.org/2002/07/owl#target target .");
			} else if (rdfStatement.contains("#source")) {
				assertEquals(
						"RDF statement mismatch",
						rdfStatement,
						"http://www.w3.org/2002/07/owl#edge_label http://www.w3.org/2002/07/owl#source src .");
			}
		}

		/* test with a null graph element */
		serializedGraphElement = new SerializedGraphElementStringTypeVids();
		serializedGraphElement.init(null);
		t = new PropertyGraphElementTuple(1);
		t.set(0, serializedGraphElement);
		result = (DataBag) toRdfUdf.exec(t);
	}

	@After
	public void done() {
		System.out.println("*** Done with the TORDF tests ***");
	}

}