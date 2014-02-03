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
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;

import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Vertex;

public class TitanGraphElementWriterMockTest {

	/**
	 * Append option turned off. This is the -a command line option.
	 */
	@Test
	public void testFindOrCreateVertex_NoAppend() throws Exception {

		// initialize class under test
		TitanGraphElementWriter writer = new TitanGraphElementWriter();
		writer.appendToExistingGraph = false;

		// setup mocks
		String id = "12340000";
		Vertex expectedVertex = mock(Vertex.class);
		writer.graph = mock(TitanGraph.class);
		when(writer.graph.addVertex(null)).thenReturn(expectedVertex);

		// call method under test
		Vertex actualVertex = writer.findOrCreateVertex(id);

		assertEquals(expectedVertex, actualVertex);

		verify(writer.graph).addVertex(null);
		verify(actualVertex).setProperty(TitanConfig.GB_ID_FOR_TITAN, id);
	}

	/**
	 * Append option turned on and the vertex was found
	 */
	@Test
	public void testFindOrCreateVertex_AppendFound() throws Exception {

		// initialize class under test
		TitanGraphElementWriter writer = new TitanGraphElementWriter();
		writer.appendToExistingGraph = true;

		// setup mocks
		String id = "12340002";
		Vertex expectedVertex = mock(Vertex.class);
		writer.graph = mockGraphForTestingFindVertexById(id, expectedVertex);

		// call method under test
		Vertex actualVertex = writer.findOrCreateVertex(id);

		assertEquals(expectedVertex, actualVertex);
		verify(writer.graph, never()).addVertex(null);
	}

	@Test
	public void testFindVertexById_Found() throws Exception {

		// initialize class under test
		TitanGraphElementWriter writer = new TitanGraphElementWriter();

		// setup mocks
		String id = "12341111";
		Vertex expectedVertex = mock(Vertex.class);
		writer.graph = mockGraphForTestingFindVertexById(id, expectedVertex);

		// call method under test
		Vertex actualVertex = writer.findVertexById(id);

		assertEquals(expectedVertex, actualVertex);
	}

	/**
	 * Expected to log an error
	 */
	@Test
	public void testFindVertexById_Duplicate() throws Exception {

		// initialize class under test
		TitanGraphElementWriter writer = new TitanGraphElementWriter();

		// setup mocks
		String id = "12342222";
		Vertex v1 = mock(Vertex.class);
		Vertex v2 = mock(Vertex.class);
		writer.graph = mockGraphForTestingFindVertexById(id, v1, v2);

		// call method under test
		Vertex actualVertex = writer.findVertexById(id);

		assertEquals(v1, actualVertex);
	}

	@Test
	public void testFindVertexById_NotFound() throws Exception {

		// initialize class under test
		TitanGraphElementWriter writer = new TitanGraphElementWriter();

		// setup mocks
		String id = "12343333";
		writer.graph = mockGraphForTestingFindVertexById(id);

		// call method under test
		Vertex nullVertex = writer.findVertexById(id);

		assertNull(nullVertex);
	}

	/**
	 * Mock graph that will return the supplied vertices when queried by the
	 * supplied id.
	 * 
	 * @param graphBuilderId
	 *            the id that will be used to query TitanGraph
	 * @param vertices
	 *            the vertices you want returned
	 * @return the mock graph
	 */
	private TitanGraph mockGraphForTestingFindVertexById(String graphBuilderId,
			Vertex... vertices) {

		Iterator<Vertex> iterator = Arrays.asList(vertices).iterator();

		Iterable<Vertex> iterable = mock(Iterable.class);
		when(iterable.iterator()).thenReturn(iterator);

		TitanGraph graph = mock(TitanGraph.class);
		when(graph.getVertices(TitanConfig.GB_ID_FOR_TITAN, graphBuilderId))
				.thenReturn(iterable);

		return graph;
	}

}
