package com.intel.graphbuilder.util

import com.tinkerpop.blueprints._
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import scala.collection.JavaConversions._


class GraphUtilsSpec extends Specification with Mockito {

  "GraphUtils" should {

    "give the correct output for 0 vertices and 0 edges" in {
      val graph = mock[Graph]
      graph.getVertices.returns(Nil)
      graph.getEdges.returns(Nil)

      GraphUtils.dumpGraph(graph) mustEqual "---- Graph Dump ----\n0 Vertices, 0 Edges"
    }

    "give the correct output for 1 vertex and 1 edge" in {

      val vertex = mock[Vertex]
      vertex.toString.returns("vertexMock")

      val edge = mock[Edge]
      edge.toString.returns("edgeMock")

      val graph = mock[Graph]
      graph.getVertices.returns(List(vertex))
      graph.getEdges.returns(List(edge))

      GraphUtils.dumpGraph(graph) mustEqual "---- Graph Dump ----\nvertexMock\nedgeMock\n1 Vertices, 1 Edges"
    }

    "give the correct output for 2 vertex and 2 edge" in {

      val vertex = mock[Vertex]
      vertex.toString.returns("vertexMock")

      val edge = mock[Edge]
      edge.toString.returns("edgeMock")

      val graph = mock[Graph]
      graph.getVertices.returns(List(vertex, vertex))
      graph.getEdges.returns(List(edge, edge))

      GraphUtils.dumpGraph(graph) mustEqual "---- Graph Dump ----\nvertexMock\nvertexMock\nedgeMock\nedgeMock\n2 Vertices, 2 Edges"
    }
  }
}
