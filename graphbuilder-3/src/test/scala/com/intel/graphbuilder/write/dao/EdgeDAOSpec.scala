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

package com.intel.graphbuilder.write.dao

import org.specs2.mutable.Specification
import com.intel.graphbuilder.elements.{ Edge, Property }
import com.tinkerpop.blueprints.Direction
import com.intel.graphbuilder.elements.Vertex
import com.intel.graphbuilder.write.titan.TitanIdUtils.titanId
import com.intel.graphbuilder.testutils.TestingTitan

class EdgeDAOSpec extends Specification {

  // before / after
  trait DAOSetup extends TestingTitan {
    lazy val vertexDAO = new VertexDAO(graph)
    lazy val edgeDAO = new EdgeDAO(graph, vertexDAO)
  }

  "EdgeDAO" should {
    "require a graph" in new DAOSetup {
      new EdgeDAO(null, vertexDAO) must throwA[IllegalArgumentException]
    }
  }

  "EdgeDAO create and update methods" should {

    "create a blueprints edge from a graphbuilder edge" in new DAOSetup {
      // setup data dependency - vertices are needed before edges
      vertexDAO.create(new Vertex(new Property("gbId", 10001), Nil))
      vertexDAO.create(new Vertex(new Property("gbId", 10002), Nil))

      // create input data
      val gbEdge = new Edge(new Property("gbId", 10001), new Property("gbId", 10002), "myLabel", Nil)

      // invoke method under test
      val bpEdge = edgeDAO.create(gbEdge)

      // validate
      bpEdge.getVertex(Direction.IN).getProperty("gbId").asInstanceOf[Int] mustEqual 10002
      bpEdge.getVertex(Direction.OUT).getProperty("gbId").asInstanceOf[Int] mustEqual 10001
      bpEdge.getLabel mustEqual "myLabel"
    }

    "updateOrCreate when create is needed" in new DAOSetup {
      // setup data dependency - vertices are needed before edges
      vertexDAO.create(new Vertex(new Property("gbId", 20001), Nil))
      vertexDAO.create(new Vertex(new Property("gbId", 20002), Nil))

      // create input data
      val gbEdge = new Edge(new Property("gbId", 20001), new Property("gbId", 20002), "myLabel", Nil)

      // invoke method under test
      val bpEdge = edgeDAO.updateOrCreate(gbEdge)

      // validate
      bpEdge.getVertex(Direction.IN).getProperty("gbId").asInstanceOf[Int] mustEqual 20002
      bpEdge.getVertex(Direction.OUT).getProperty("gbId").asInstanceOf[Int] mustEqual 20001
      bpEdge.getLabel mustEqual "myLabel"
    }

    "updateOrCreate when update is needed" in new DAOSetup {
      // setup data dependency - vertices are needed before edges
      val gbId1 = new Property("gbId", 30001)
      val gbId2 = new Property("gbId", 30002)
      vertexDAO.create(new Vertex(gbId1, Nil))
      vertexDAO.create(new Vertex(gbId2, Nil))

      // create an edge
      val gbEdge = new Edge(gbId1, gbId2, "myLabel", Nil)
      val bpEdgeOriginal = edgeDAO.create(gbEdge)
      graph.commit()

      // define an updated version of the same edge
      val updatedEdge = new Edge(gbId1, gbId2, "myLabel", List(new Property("newKey", "newValue")))

      // invoke method under test
      val bpEdgeUpdated = edgeDAO.updateOrCreate(updatedEdge)
      graph.commit()

      titanId(edgeDAO.find(gbEdge).get) mustEqual titanId(edgeDAO.find(updatedEdge).get)

      // validate
      graph.getEdge(bpEdgeOriginal.getId) must beNull // this is weird but when you update Titan assigns a new id
      bpEdgeUpdated.getProperty("newKey").asInstanceOf[String] mustEqual "newValue"
    }

    "create should fail when tail vertex does not exist" in new DAOSetup {
      // setup data dependency - vertices are needed before edges
      vertexDAO.create(new Vertex(new Property("gbId", 40002), Nil))

      // create input data
      val gbEdge = new Edge(new Property("gbId", 40001), new Property("gbId", 40002), "myLabel", Nil)

      // invoke method under test
      edgeDAO.create(gbEdge) must throwA[IllegalArgumentException]
    }

    "create should fail when head vertex does not exist" in new DAOSetup {
      // setup data dependency - vertices are needed before edges
      vertexDAO.create(new Vertex(new Property("gbId", 50001), Nil))

      // create input data
      val gbEdge = new Edge(new Property("gbId", 50001), new Property("gbId", 50002), "myLabel", Nil)

      // invoke method under test
      edgeDAO.create(gbEdge) must throwA[IllegalArgumentException]
    }

    "be able to update properties on existing edges" in new DAOSetup {
      // setup data
      val gbId1 = new Property("gbId", 60001)
      val gbId2 = new Property("gbId", 60002)

      vertexDAO.create(new Vertex(gbId1, Nil))
      vertexDAO.create(new Vertex(gbId2, Nil))

      val bpEdge1 = edgeDAO.create(new Edge(gbId1, gbId2, "myLabel", List(new Property("key1", "original"))))

      // invoke method under test
      edgeDAO.update(new Edge(gbId1, gbId2, "myLabel", List(new Property("key2", "added"))), bpEdge1)

      // validate
      bpEdge1.getProperty("key1").asInstanceOf[String] mustEqual "original"
      bpEdge1.getProperty("key2").asInstanceOf[String] mustEqual "added"
    }
  }

  // before / after
  trait FindSetup extends TestingTitan {

    lazy val vertexDAO = new VertexDAO(graph)
    lazy val edgeDAO = new EdgeDAO(graph, vertexDAO)

    val gbId1 = new Property("gbId", 10001)
    val gbId2 = new Property("gbId", 10002)
    val gbId3 = new Property("gbId", 10003)
    val gbId4 = new Property("gbId", 10004)
    val gbIdNotInGraph = new Property("gbId", 99999)

    val v1 = vertexDAO.create(new Vertex(gbId1, Nil))
    val v2 = vertexDAO.create(new Vertex(gbId2, Nil))
    val v3 = vertexDAO.create(new Vertex(gbId3, Nil))
    val v4 = vertexDAO.create(new Vertex(gbId4, Nil))

    val label = "myLabel"

    val bpEdge1 = edgeDAO.create(new Edge(gbId1, gbId2, label, Nil))
    val bpEdge2 = edgeDAO.create(new Edge(gbId2, gbId3, label, Nil))
    val bpEdge3 = edgeDAO.create(new Edge(gbId2, gbId4, label, Nil))

    graph.commit()
  }

  "EdgeDAO find methods" should {

    "find blueprints Edges using graphbuilder Edge definitions" in new FindSetup {
      val bpEdge = edgeDAO.find(new Edge(gbId1, gbId2, label, Nil))
      bpEdge.get mustEqual bpEdge1
    }

    "find blueprints Edges using graphbuilder GbId's and Label" in new FindSetup {
      val bpEdge = edgeDAO.find(gbId2, gbId3, label)
      bpEdge.get mustEqual bpEdge2
    }

    "find blueprints Edges using blueprints Vertices and Label" in new FindSetup {
      val bpEdge = edgeDAO.find(v1, v2, label)
      bpEdge.get mustEqual bpEdge1
    }

    "find blueprints Edges using blueprints Vertices and Label" in new FindSetup {
      val bpEdge = edgeDAO.find(v2, v4, label)
      bpEdge.get mustEqual bpEdge3
    }

    "not find blueprints Edge when the Edge does not exist" in new FindSetup {
      val bpEdge = edgeDAO.find(gbId2, gbId1, label)
      bpEdge.isEmpty mustEqual true
    }

    "not find blueprints Edge when the Edge label does not exist" in new FindSetup {
      val bpEdge = edgeDAO.find(gbId1, gbId2, "labelThatDoesNotExist")
      bpEdge.isEmpty mustEqual true
    }

    "not find blueprints Edge when the Edge does not exist" in new FindSetup {
      val bpEdge = edgeDAO.find(v3, v4, label)
      bpEdge.isEmpty mustEqual true
    }

    "not find blueprints Edge when the tail Vertex does not exist" in new FindSetup {
      val bpEdge = edgeDAO.find(new Edge(gbIdNotInGraph, gbId2, label, Nil))
      bpEdge.isEmpty mustEqual true
    }

    "not find blueprints Edge when the head Vertex does not exist" in new FindSetup {
      val bpEdge = edgeDAO.find(gbId1, gbIdNotInGraph, label)
      bpEdge.isEmpty mustEqual true
    }
  }

}