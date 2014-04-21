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

import org.specs2.mutable.Specification
import com.intel.graphbuilder.schema.{PropertyType, PropertyDef, EdgeLabelDef, GraphSchema}
import com.tinkerpop.blueprints.{Vertex, Direction, Edge}
import org.specs2.mock.Mockito
import com.thinkaurelius.titan.core.TitanGraph
import com.intel.graphbuilder.testutils.TestingTitan

class TitanSchemaWriterSpec extends Specification with Mockito {

  // before / after
  trait SchemaWriterSetup extends TestingTitan {
    lazy val titanSchemaWriter = new TitanSchemaWriter(graph)
  }

  "TitanSchemaWriter" should {

    "write an edge label definition" in new SchemaWriterSetup {
      // setup
      val edgeLabel = new EdgeLabelDef("myLabel")
      val schema = new GraphSchema(List(edgeLabel), Nil)

      // invoke method under test
      titanSchemaWriter.write(schema)

      // validate
      graph.getType("myLabel").isEdgeLabel mustEqual true
    }

    "ignore duplicate edge label definitions" in new SchemaWriterSetup {
      // setup
      val edgeLabel = new EdgeLabelDef("myLabel")
      val edgeLabelDup = new EdgeLabelDef("myLabel")
      val schema = new GraphSchema(List(edgeLabel, edgeLabelDup), Nil)

      // invoke method under test
      titanSchemaWriter.write(schema)

      // validate
      graph.getType("myLabel").isEdgeLabel mustEqual true
    }

    "write a property definition" in new SchemaWriterSetup {
      val propertyDef = new PropertyDef(PropertyType.Vertex, "propName", classOf[String], unique = false, indexed = false)
      val schema = new GraphSchema(Nil, List(propertyDef))

      // invoke method under test
      titanSchemaWriter.write(schema)

      // validate
      graph.getType("propName").isPropertyKey mustEqual true
      graph.getType("propName").isUnique(Direction.IN) mustEqual false
    }

    "write a property definition that is unique and indexed" in new SchemaWriterSetup {
      val propertyDef = new PropertyDef(PropertyType.Vertex, "propName", classOf[String], unique = true, indexed = true)
      val schema = new GraphSchema(Nil, List(propertyDef))

      // invoke method under test
      titanSchemaWriter.write(schema)

      // validate
      graph.getType("propName").isPropertyKey mustEqual true
      graph.getType("propName").isUnique(Direction.IN) mustEqual true
    }

    "ignore duplicate property definitions" in new SchemaWriterSetup {
      val propertyDef = new PropertyDef(PropertyType.Vertex, "propName", classOf[String], unique = false, indexed = false)
      val propertyDup = new PropertyDef(PropertyType.Vertex, "propName", classOf[String], unique = false, indexed = false)
      val schema = new GraphSchema(Nil, List(propertyDef, propertyDup))

      // invoke method under test
      titanSchemaWriter.write(schema)

      // validate
      graph.getType("propName").isPropertyKey mustEqual true
      graph.getType("propName").isUnique(Direction.IN) mustEqual false
    }

    "handle empty lists" in new SchemaWriterSetup {
      val schema = new GraphSchema(Nil, Nil)

      // invoke method under test
      titanSchemaWriter.write(schema)

      // validate
      graph.getType("propName") must beNull
    }

    "require a graph" in {
      new TitanSchemaWriter(null) must throwA[IllegalArgumentException]
    }

    "require an open graph" in {
      // setup mocks
      val graph = mock[TitanGraph]
      graph.isOpen returns false

      // invoke method under test
      new TitanSchemaWriter(graph) must throwA[IllegalArgumentException]
    }

    "determine indexType for Edges " in {
      val graph = mock[TitanGraph]
      graph.isOpen returns true
      new TitanSchemaWriter(graph).indexType(PropertyType.Edge) mustEqual classOf[Edge]
    }

    "determine indexType for Vertices " in {
      val graph = mock[TitanGraph]
      graph.isOpen returns true
      new TitanSchemaWriter(graph).indexType(PropertyType.Vertex) mustEqual classOf[Vertex]
    }

    "fail for unexpected types" in {
      val graph = mock[TitanGraph]
      graph.isOpen returns true
      new TitanSchemaWriter(graph).indexType(new PropertyType.Value {
        override def id: Int = -99999999
      }) must throwA[RuntimeException]
    }
  }
}
