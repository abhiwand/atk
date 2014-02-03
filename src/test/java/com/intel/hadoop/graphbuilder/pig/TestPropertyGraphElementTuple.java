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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.junit.Test;

import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.DoubleType;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.pig.data.PropertyGraphElementTuple;

public class TestPropertyGraphElementTuple {

	private static final double DELTA = 0.0001;

	@Test
	public void runTests() throws IOException {
		PropertyGraphElementTuple t = new PropertyGraphElementTuple();
		assertEquals("Should have been 0", 0, t.size());

		List<SerializedGraphElementStringTypeVids> elems = new ArrayList<SerializedGraphElementStringTypeVids>();
		int nElems = 10;
		for (int i = 0; i < nElems; i++) {
			String name = "v-" + i;
			StringType vid = new StringType(name);
			Vertex<StringType> vertex = new Vertex<StringType>(vid);
			vertex.setLabel(new StringType("label-" + i));
			vertex.setProperty("p-" + i, new DoubleType(i));
			SerializedGraphElementStringTypeVids vertexElement = new SerializedGraphElementStringTypeVids();
			vertexElement.init(vertex);
			elems.add(vertexElement);
		}

		t = new PropertyGraphElementTuple(elems);
		assertEquals("Should have been " + nElems, nElems, t.size());
		List<Object> allElements = t.getAll();
		int i = 0;
		for (Object o : allElements) {
			SerializedGraphElementStringTypeVids s = (SerializedGraphElementStringTypeVids) o;
			Vertex v = (Vertex) s.graphElement().get();
			assertEquals("Should have been v-" + i, "v-" + i, v.getId()
					.getName().toString());
			assertEquals("Should have been label-" + i, "label-" + i, v
					.getLabel().get());
			assertEquals("Should have been " + new DoubleType(i), i,
					((DoubleType) (v.getProperty("p-" + i))).get(), DELTA);
			i++;
		}

		// test append
		SerializedGraphElementStringTypeVids newElement = new SerializedGraphElementStringTypeVids();
		String name = "new_vertex";
		StringType vid = new StringType(name);
		Vertex<StringType> vertex = new Vertex<StringType>(vid);
		vertex.setLabel(new StringType("new_label"));
		vertex.setProperty("new_property", new StringType("new_value"));
		newElement.init(vertex);
		t.append(newElement);
		Vertex readVertex = (Vertex) ((SerializedGraphElementStringTypeVids) t
				.get(t.size() - 1)).graphElement().get();
		assertEquals("Should have been new_vertex", "new_vertex", vertex
				.getId().getName().toString());
		assertEquals("Should have been new_label", "new_label", vertex
				.getLabel().get());
		assertEquals("Should have been new_value", "new_value",
				((StringType) vertex.getProperty("new_property")).get());

		// test set
		newElement = new SerializedGraphElementStringTypeVids();
		name = "new_0th_vertex";
		vid = new StringType(name);
		vertex = new Vertex<StringType>(vid);
		vertex.setLabel(new StringType("new_0th_label"));
		vertex.setProperty("new_0th_property", new StringType("new_0th_value"));
		newElement.init(vertex);
		t.set(0, newElement);
		readVertex = (Vertex) ((SerializedGraphElementStringTypeVids) t.get(0))
				.graphElement().get();
		assertEquals("Should have been new_0th_vertex", "new_0th_vertex",
				vertex.getId().getName().toString());
		assertEquals("Should have been new_0th_label", "new_0th_label", vertex
				.getLabel().get());
		assertEquals("Should have been new_0th_value", "new_0th_value",
				((StringType) vertex.getProperty("new_0th_property")).get());

		assertEquals("Should be 1", 1,
				t.compareTo(new PropertyGraphElementTuple()));
		assertEquals("Should be -1", -1,
				t.compareTo(new PropertyGraphElementTuple(50)));
		assertEquals("Should be 0", 0, t.compareTo(t));
		assertNotEquals("Shouldn't be the same", t.hashCode(),
				new PropertyGraphElementTuple());

	}

	@Test(expected = ExecException.class)
	public void testGetFailureCases1() throws IOException {
		PropertyGraphElementTuple t = new PropertyGraphElementTuple(10);
		t.get(11);
	}

	@Test(expected = ExecException.class)
	public void testSetFailureCases1() throws IOException {
		PropertyGraphElementTuple t = new PropertyGraphElementTuple(10);
		t.set(11, null);
		;
	}

	@Test(expected = NullPointerException.class)
	public void testSetFailureCases2() throws IOException {
		PropertyGraphElementTuple t = new PropertyGraphElementTuple(10);
		t.set(5, null);
		;
	}

	@Test(expected = NullPointerException.class)
	public void testFailureCases2() throws IOException {
		PropertyGraphElementTuple t = new PropertyGraphElementTuple(10);
		t.append(null);
	}
}