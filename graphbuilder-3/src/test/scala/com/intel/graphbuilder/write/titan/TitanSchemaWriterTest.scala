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

package com.intel.graphbuilder.write.titan

import com.intel.graphbuilder.driver.spark.TestingTitan
import com.intel.graphbuilder.schema.{ EdgeLabelDef, GraphSchema, PropertyDef, PropertyType }
import com.thinkaurelius.titan.core.TitanGraph
import com.tinkerpop.blueprints.{ Direction, Edge, Vertex }
import org.mockito.Mockito._
import org.scalatest.{ BeforeAndAfter, Matchers, WordSpec }
import org.scalatest.mock.MockitoSugar

class TitanSchemaWriterTest extends WordSpec with Matchers with MockitoSugar with TestingTitan with BeforeAndAfter {

  var titanSchemaWriter: TitanSchemaWriter = null

  before {
    setupTitan()
    titanSchemaWriter = new TitanSchemaWriter(graph)
  }

  after {
    cleanupTitan()
    titanSchemaWriter = null
  }

  "TitanSchemaWriter" should {

    "write an edge label definition" in {
      // setup
      val edgeLabel = new EdgeLabelDef("myLabel")
      val schema = new GraphSchema(List(edgeLabel), Nil)

      // invoke method under test
      titanSchemaWriter.write(schema)

      // validate
      graph.getType("myLabel").isEdgeLabel shouldBe true
    }

    "ignore duplicate edge label definitions" in {
      // setup
      val edgeLabel = new EdgeLabelDef("myLabel")
      val edgeLabelDup = new EdgeLabelDef("myLabel")
      val schema = new GraphSchema(List(edgeLabel, edgeLabelDup), Nil)

      // invoke method under test
      titanSchemaWriter.write(schema)

      // validate
      graph.getType("myLabel").isEdgeLabel shouldBe true
    }

    "write a property definition" in {
      val propertyDef = new PropertyDef(PropertyType.Vertex, "propName", classOf[String], unique = false, indexed = false)
      val schema = new GraphSchema(Nil, List(propertyDef))

      // invoke method under test
      titanSchemaWriter.write(schema)

      // validate
      graph.getType("propName").isPropertyKey shouldBe true
      graph.getType("propName").isUnique(Direction.IN) shouldBe false
    }

    "write a property definition that is unique and indexed" in {
      val propertyDef = new PropertyDef(PropertyType.Vertex, "propName", classOf[String], unique = true, indexed = true)
      val schema = new GraphSchema(Nil, List(propertyDef))

      // invoke method under test
      titanSchemaWriter.write(schema)

      // validate
      graph.getType("propName").isPropertyKey shouldBe true
      graph.getType("propName").isUnique(Direction.IN) shouldBe true
    }

    "ignore duplicate property definitions" in {
      val propertyDef = new PropertyDef(PropertyType.Vertex, "propName", classOf[String], unique = false, indexed = false)
      val propertyDup = new PropertyDef(PropertyType.Vertex, "propName", classOf[String], unique = false, indexed = false)
      val schema = new GraphSchema(Nil, List(propertyDef, propertyDup))

      // invoke method under test
      titanSchemaWriter.write(schema)

      // validate
      graph.getType("propName").isPropertyKey shouldBe true
      graph.getType("propName").isUnique(Direction.IN) shouldBe false
    }

    "handle empty lists" in {
      val schema = new GraphSchema(Nil, Nil)

      // invoke method under test
      titanSchemaWriter.write(schema)

      // validate
      graph.getType("propName") should be(null)
    }

    "require a graph" in {
      an[IllegalArgumentException] should be thrownBy new TitanSchemaWriter(null)
    }

    "require an open graph" in {
      // setup mocks
      val graph = mock[TitanGraph]
      when(graph.isOpen).thenReturn(false)

      // invoke method under test
      an[IllegalArgumentException] should be thrownBy new TitanSchemaWriter(graph)
    }

    "determine indexType for Edges " in {
      val graph = mock[TitanGraph]
      when(graph.isOpen).thenReturn(true)
      new TitanSchemaWriter(graph).indexType(PropertyType.Edge) shouldBe classOf[Edge]
    }

    "determine indexType for Vertices " in {
      val graph = mock[TitanGraph]
      when(graph.isOpen).thenReturn(true)
      new TitanSchemaWriter(graph).indexType(PropertyType.Vertex) shouldBe classOf[Vertex]
    }

    "fail for unexpected types" in {
      val graph = mock[TitanGraph]
      when(graph.isOpen).thenReturn(true)
      an[RuntimeException] should be thrownBy new TitanSchemaWriter(graph).indexType(new PropertyType.Value {
        override def id: Int = -99999999
      })
    }
  }
}