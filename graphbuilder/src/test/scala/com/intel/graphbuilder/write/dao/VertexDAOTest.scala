//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

import com.intel.graphbuilder.elements._
import com.intel.graphbuilder.write.titan.TitanIdUtils
import com.intel.testutils.TestingTitan
import org.scalatest.{ BeforeAndAfter, Matchers, WordSpec }

class VertexDAOTest extends WordSpec with Matchers with TestingTitan with BeforeAndAfter {

  var vertexDAO: VertexDAO = null

  before {
    setupTitan()
    vertexDAO = new VertexDAO(titanGraph)
  }

  after {
    cleanupTitan()
    vertexDAO = null
  }

  "VertexDAO" should {

    "require a graph" in {
      an[IllegalArgumentException] should be thrownBy new VertexDAO(null)
    }

    "create a blueprints vertex from a graphbuilder vertex" in {
      val gbVertex = new GBVertex(new Property("gbId", 10001), Set.empty[Property])
      val bpVertex = vertexDAO.create(gbVertex)
      bpVertex.getPropertyKeys.size() shouldBe 1
      bpVertex.getProperty("gbId").asInstanceOf[Int] shouldBe 10001
    }

    "set properties on a newly created blueprints vertex" in {
      val gbVertex = new GBVertex(new Property("gbId", 10002), Set(new Property("name", "My Name")))
      val bpVertex = vertexDAO.create(gbVertex)
      bpVertex.getPropertyKeys.size() shouldBe 2
      bpVertex.getProperty("name").asInstanceOf[String] shouldBe "My Name"
    }

    "update properties on vertices" in {
      // setup data
      val gbVertexOriginal = new GBVertex(new Property("gbId", 10003), Set(new Property("name", "Original Name")))
      val gbVertexUpdated = new GBVertex(new Property("gbId", 10003), Set(new Property("name", "Updated Name")))
      val bpVertexOriginal = vertexDAO.create(gbVertexOriginal)

      // invoke method under test
      val bpVertexUpdated = vertexDAO.update(gbVertexUpdated, bpVertexOriginal)

      // validate
      bpVertexUpdated.getPropertyKeys.size() shouldBe 2
      bpVertexUpdated.getProperty("name").asInstanceOf[String] shouldBe "Updated Name"
    }

    "update properties on vertices when no create is needed" in {
      // setup data
      val gbVertexOriginal = new GBVertex(new Property("gbId", 10004), Set(new Property("name", "Original Name")))
      val gbVertexUpdated = new GBVertex(new Property("gbId", 10004), Set(new Property("name", "Updated Name")))
      vertexDAO.create(gbVertexOriginal)
      titanGraph.commit()

      // invoke method under test
      val bpVertexUpdated = vertexDAO.updateOrCreate(gbVertexUpdated)

      // validate
      bpVertexUpdated.getPropertyKeys.size() shouldBe 2
      bpVertexUpdated.getProperty("name").asInstanceOf[String] shouldBe "Updated Name"
    }

    "create vertices when create is needed" in {
      // setup data
      val gbVertex = new GBVertex(new Property("gbId", 10005), Set(new Property("name", "Original Name")))

      // invoke method under test
      val bpVertexUpdated = vertexDAO.updateOrCreate(gbVertex)

      // validate
      bpVertexUpdated.getPropertyKeys.size() shouldBe 2
      bpVertexUpdated.getProperty("name").asInstanceOf[String] shouldBe "Original Name"
    }

    "find a blueprints vertex using a physical titan id" in {
      // setup data
      val gbVertex = new GBVertex(new Property("gbId", 10006), Set.empty[Property])
      val createdBpVertex = vertexDAO.create(gbVertex)
      val id = TitanIdUtils.titanId(createdBpVertex)
      titanGraph.commit()

      // invoke method under test
      val foundBpVertex = vertexDAO.findByPhysicalId(id.asInstanceOf[AnyRef]).get

      // validate
      createdBpVertex shouldBe foundBpVertex
    }

    "handle null id's gracefully" in {
      val v = vertexDAO.findByGbId(null)
      v.isEmpty shouldBe true
    }

  }
}
