//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

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
