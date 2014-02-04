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

import static org.junit.Assert.*;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import org.apache.hadoop.io.Writable;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.junit.Before;
import org.junit.Test;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.DoubleType;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;

public class TestMergeDuplicateGraphElements {
	EvalFunc<?> flattenUdf;
	TupleFactory tupleFactory = TupleFactory.getInstance();

	@Before
	public void setup() throws Exception {
		flattenUdf = (EvalFunc<?>) PigContext
				.instantiateFuncFromSpec("com.intel.pig.udf.eval.MergeDuplicateGraphElements()");
	}

	@Test
	public void runTests() throws IOException {
		Tuple t = tupleFactory.newTuple(2);
		DataBag duplicateElementsBag = DefaultBagFactory.getInstance()
				.newDefaultBag();
		String elementKey = "element_id";
		t.set(0, elementKey);
		t.set(1, duplicateElementsBag);
		int nElems = 10;
		Hashtable<String, DoubleType> allProperties = new Hashtable<String, DoubleType>();
		for (int i = 0; i < nElems; i++) {
			String name = "v-" + i;
			StringType vid = new StringType(name);
			Vertex<StringType> vertex = new Vertex<StringType>(vid);
			vertex.setLabel(new StringType("label-" + i));
			String key = "p-" + i;
			DoubleType value = new DoubleType(i);
			vertex.setProperty(key, value);
			allProperties.put(key, value);
			SerializedGraphElementStringTypeVids vertexElement = new SerializedGraphElementStringTypeVids();
			vertexElement.init(vertex);
			Tuple newTuple = tupleFactory.newTuple(2);
			newTuple.set(0, elementKey);
			newTuple.set(1, vertexElement);
			duplicateElementsBag.add(newTuple);
		}
		Tuple result = (Tuple) flattenUdf.exec(t);
		assertEquals("Should contain a single element", 1, result.size());
		SerializedGraphElementStringTypeVids merged = (SerializedGraphElementStringTypeVids) result.get(0);
		Vertex<StringType> v = (Vertex<StringType>) merged.graphElement().get();
		assertEquals("Label mismatch", "label-0", v.getLabel().get());
		assertEquals("Name mismatch", "v-0", v.getId().getName().get());
		PropertyMap map = v.getProperties();
		Set<Writable> keys = map.getPropertyKeys();
		Iterator<Writable> iter = keys.iterator();
		while (iter.hasNext()) {
			String key = iter.next().toString();
			DoubleType value = (DoubleType) map.getProperty(key);
			assertTrue(allProperties.containsKey(key));
			DoubleType expected = allProperties.remove(key);
			assertEquals("Property value mismatch", expected, value);
		}
		assertEquals("Should be 0", 0, allProperties.size());
	}
}