/**
 * Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more about this software visit:
 *     http://www.01.org/GraphBuilder
 */
package com.intel.hadoop.graphbuilder.pipeline.output.titan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.intel.hadoop.graphbuilder.test.TestingGraphProvider;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class EdgesIntoTitanMapperTest {

	TestingGraphProvider provider = new TestingGraphProvider();
	TitanGraph graph;

	@Before
	public void setup() throws IOException {
		graph = provider.getTitanGraph();
	}

	@After
	public void tearDown() {
		provider.cleanUp();
	}

	@Test
	public void testFindOrCreateEdge_Found() throws Exception {
		// setup test data
		Vertex src = graph.addVertex(null);
		Vertex dst = graph.addVertex(null);
		String label = "label1243";
		Edge edge = graph.addEdge(null, src, dst, label);

		// initialize class under test
		EdgesIntoTitanMapper reducer = new EdgesIntoTitanMapper();
		reducer.setAppendToExistingGraph(true);

		// call method under test
		Edge actualEdge = reducer.findOrCreateEdge(src, dst, label);

		assertEquals(edge, actualEdge);
	}

	@Test
	public void testFindOrCreateEdge_Create() throws Exception {
		// setup test data
		Vertex src = graph.addVertex(null);
		Vertex dst = graph.addVertex(null);
		String label = "label1243";

		// initialize class under test
		EdgesIntoTitanMapper reducer = new EdgesIntoTitanMapper();
		reducer.setAppendToExistingGraph(true);
		reducer.graph = graph;

		// call method under test
		Edge actualEdge = reducer.findOrCreateEdge(src, dst, label);

		assertNotNull(actualEdge);
		assertEquals(actualEdge.getVertex(Direction.IN), dst);
		assertEquals(actualEdge.getVertex(Direction.OUT), src);
	}

	@Test
	public void testFindEdge_Found() throws Exception {
		// setup test data
		Vertex src = graph.addVertex(null);
		Vertex dst = graph.addVertex(null);
		String label = "label1243";
		Edge expectedEdge = graph.addEdge(null, src, dst, label);

		// initialize class under test
		EdgesIntoTitanMapper reducer = new EdgesIntoTitanMapper();

		// call method under test
		Edge actualEdge = reducer.findEdge(src, dst, label);

		assertEquals(expectedEdge, actualEdge);
	}

	@Test
	public void testFindEdge_NotFound() throws Exception {
		// setup test data
		Vertex src = graph.addVertex(null);
		Vertex dst = graph.addVertex(null);
		String label = "label1567";
		Edge edge = graph.addEdge(null, src, dst, label);

		// initialize class under test
		EdgesIntoTitanMapper reducer = new EdgesIntoTitanMapper();

		// call method under test
		Edge nullEdge = reducer.findEdge(src, dst, "non-matching-label");

		assertNull(nullEdge);
	}
}
