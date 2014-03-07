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

import com.intel.hadoop.graphbuilder.test.TestingGraphProvider;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class TitanGraphElementWriterTest {

	TestingGraphProvider provider = new TestingGraphProvider();
	TitanGraph graph;

	@Before
	public void setup() throws IOException {
		graph = provider.getTitanGraph();

		// initialize schema
		graph.makeKey(TitanConfig.GB_ID_FOR_TITAN).dataType(String.class)
				.indexed(Vertex.class).unique().make();

	}

	@After
	public void tearDown() {
		provider.cleanUp();
	}

	@Test
	public void testFindOrCreateVertex_Found() throws Exception {

		// setup test data
		String id = "12345";
		Vertex expectedVertex = addVertex(id);

		// initialize class under test
		TitanGraphElementWriter writer = new TitanGraphElementWriter();
		writer.graph = graph;
		writer.appendToExistingGraph = true;

		// call method under test
		Vertex actualVertex = writer.findOrCreateVertex(id);

		assertEquals(expectedVertex, actualVertex);
	}

	@Test
	public void testFindOrCreateVertex_Create() throws Exception {
		String id = "67899";

		// initialize class under test
		TitanGraphElementWriter writer = new TitanGraphElementWriter();
		writer.graph = graph;
		writer.appendToExistingGraph = true;

		// call method under test
		Vertex createdVertex = writer.findOrCreateVertex(id);

		assertNotNull(createdVertex);
		assertEquals(id, createdVertex.getProperty(TitanConfig.GB_ID_FOR_TITAN));
	}

	@Test
	public void testFindVertexById_Found() throws Exception {

		// setup test data
		String id = "12345";
		Vertex vertex = addVertex(id);

		// initialize class under test
		TitanGraphElementWriter writer = new TitanGraphElementWriter();
		writer.graph = graph;

		// call method under test
		Vertex actualVertex = writer.findVertexById(id);

		assertEquals(vertex, actualVertex);
	}

	@Test
	public void testFindVertexById_NotFound() throws Exception {

		// setup test data
		addVertex("any_id");

		// initialize class under test
		TitanGraphElementWriter writer = new TitanGraphElementWriter();
		writer.graph = graph;

		// call method under test
		Vertex nonExistingVertex = writer.findVertexById("other_id");

		assertNull(nonExistingVertex);
	}

	private Vertex addVertex(String graphBuilderId) {
		Vertex vertex = graph.addVertex(null);
		vertex.setProperty(TitanConfig.GB_ID_FOR_TITAN, graphBuilderId);
		return vertex;
	}

}
