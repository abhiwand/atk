package com.intel.graphbuilder.write.dao

import com.intel.graphbuilder.write.titan.TitanIdUtils
import com.intel.graphbuilder.elements._
import com.intel.graphbuilder.testutils.TestingTitan
import org.specs2.mutable.Specification

class VertexDAOSpec extends Specification {

  // before / after
  trait DAOSetup extends TestingTitan {
    lazy val vertexDAO = new VertexDAO(graph)
  }

  "VertexDAO" should {

    "require a graph" in {
      new VertexDAO(null) must throwA[IllegalArgumentException]
    }

    "create a blueprints vertex from a graphbuilder vertex" in new DAOSetup {
      val gbVertex = new Vertex(new Property("gbId", 10001), Nil)
      val bpVertex = vertexDAO.create(gbVertex)
      bpVertex.getPropertyKeys.size() mustEqual 1
      bpVertex.getProperty("gbId").asInstanceOf[Int] mustEqual 10001
    }

    "set properties on a newly created blueprints vertex" in new DAOSetup {
      val gbVertex = new Vertex(new Property("gbId", 10002), List(new Property("name", "My Name")))
      val bpVertex = vertexDAO.create(gbVertex)
      bpVertex.getPropertyKeys.size() mustEqual 2
      bpVertex.getProperty("name").asInstanceOf[String] mustEqual "My Name"
    }

    "update properties on vertices" in new DAOSetup {
      // setup data
      val gbVertexOriginal = new Vertex(new Property("gbId", 10003), List(new Property("name", "Original Name")))
      val gbVertexUpdated = new Vertex(new Property("gbId", 10003), List(new Property("name", "Updated Name")))
      val bpVertexOriginal = vertexDAO.create(gbVertexOriginal)

      // invoke method under test
      val bpVertexUpdated = vertexDAO.update(gbVertexUpdated, bpVertexOriginal)

      // validate
      bpVertexUpdated.getPropertyKeys.size() mustEqual 2
      bpVertexUpdated.getProperty("name").asInstanceOf[String] mustEqual "Updated Name"
    }

    "update properties on vertices when no create is needed" in new DAOSetup {
      // setup data
      val gbVertexOriginal = new Vertex(new Property("gbId", 10004), List(new Property("name", "Original Name")))
      val gbVertexUpdated = new Vertex(new Property("gbId", 10004), List(new Property("name", "Updated Name")))
      vertexDAO.create(gbVertexOriginal)
      graph.commit()

      // invoke method under test
      val bpVertexUpdated = vertexDAO.updateOrCreate(gbVertexUpdated)

      // validate
      bpVertexUpdated.getPropertyKeys.size() mustEqual 2
      bpVertexUpdated.getProperty("name").asInstanceOf[String] mustEqual "Updated Name"
    }

    "create vertices when create is needed" in new DAOSetup {
      // setup data
      val gbVertex = new Vertex(new Property("gbId", 10005), List(new Property("name", "Original Name")))

      // invoke method under test
      val bpVertexUpdated = vertexDAO.updateOrCreate(gbVertex)

      // validate
      bpVertexUpdated.getPropertyKeys.size() mustEqual 2
      bpVertexUpdated.getProperty("name").asInstanceOf[String] mustEqual "Original Name"
    }

    "find a blueprints vertex using a physical titan id" in new DAOSetup {
      // setup data
      val gbVertex = new Vertex(new Property("gbId", 10006), Nil)
      val createdBpVertex = vertexDAO.create(gbVertex)
      val id = TitanIdUtils.titanId(createdBpVertex)
      graph.commit()

      // invoke method under test
      val foundBpVertex = vertexDAO.findByPhysicalId(id.asInstanceOf[AnyRef]).get

      // validate
      createdBpVertex mustEqual foundBpVertex
    }

    "handle null id's gracefully" in new DAOSetup {
      val v = vertexDAO.findByGbId(null)
      v.isEmpty mustEqual true
    }

  }
}
