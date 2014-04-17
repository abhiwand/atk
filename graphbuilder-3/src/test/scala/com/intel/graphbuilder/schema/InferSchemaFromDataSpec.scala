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

package com.intel.graphbuilder.schema

import org.specs2.mutable.Specification
import com.intel.graphbuilder.elements.{Vertex, Property, Edge}
import java.util.Date

class InferSchemaFromDataSpec extends Specification {

  "InferSchemaFromData" should {

    "infer one Edge label from one Edges" in {
      val edge = new Edge(null, null, "myLabel", Nil)
      val inferSchemaFromData = new InferSchemaFromData()

      inferSchemaFromData.add(edge)

      val edgeLabelDefs = inferSchemaFromData.graphSchema.edgeLabelDefs
      edgeLabelDefs.size mustEqual 1
      edgeLabelDefs.head.label mustEqual "myLabel"
    }

    "infer one Edge label from two Edges with the same label" in {
      val edge1 = new Edge(null, null, "myLabel", Nil)
      val edge2 = new Edge(null, null, "myLabel", Nil)
      val inferSchemaFromData = new InferSchemaFromData()

      inferSchemaFromData.add(edge1)
      inferSchemaFromData.add(edge2)

      val edgeLabelDefs = inferSchemaFromData.graphSchema.edgeLabelDefs
      edgeLabelDefs.size mustEqual 1
      edgeLabelDefs.head.label mustEqual "myLabel"
    }

    "infer two Edge labels from two Edges with different labels" in {
      val edge1 = new Edge(null, null, "myLabel", Nil)
      val edge2 = new Edge(null, null, "secondLabel", Nil)
      val inferSchemaFromData = new InferSchemaFromData()

      inferSchemaFromData.add(edge1)
      inferSchemaFromData.add(edge2)

      val edgeLabelDefs = inferSchemaFromData.graphSchema.edgeLabelDefs
      edgeLabelDefs.size mustEqual 2
      edgeLabelDefs.count(_.label == "myLabel") mustEqual 1
      edgeLabelDefs.count(_.label == "secondLabel") mustEqual 1
    }

    "infer no properties from an Edge when none are present" in {
      val edge = new Edge(null, null, null, Nil)
      val inferSchemaFromData = new InferSchemaFromData()

      inferSchemaFromData.add(edge)

      inferSchemaFromData.graphSchema.propertyDefs.size mustEqual 0
    }

    "infer an Edge property from an Edge" in {
      val edge = new Edge(null, null, null, List(new Property("key", "value")))
      val inferSchemaFromData = new InferSchemaFromData()

      inferSchemaFromData.add(edge)

      val propertyDefs = inferSchemaFromData.graphSchema.propertyDefs
      propertyDefs.size mustEqual 1

      propertyDefs.head mustEqual new PropertyDef(PropertyType.Edge, "key", classOf[String], false, false)
    }

    "infer one Edge property from two Edges with the same property" in {
      val edge1 = new Edge(null, null, null, List(new Property("key", "value1")))
      val edge2 = new Edge(null, null, null, List(new Property("key", "value2")))
      val inferSchemaFromData = new InferSchemaFromData()

      inferSchemaFromData.add(edge1)
      inferSchemaFromData.add(edge2)

      val propertyDefs = inferSchemaFromData.graphSchema.propertyDefs
      propertyDefs.size mustEqual 1

      propertyDefs.head mustEqual new PropertyDef(PropertyType.Edge, "key", classOf[String], false, false)
    }

    "infer two Edge properties from an Edge" in {
      val edge = new Edge(null, null, null, List(new Property("key1", "value1"), new Property("key2", new Date())))
      val inferSchemaFromData = new InferSchemaFromData()

      inferSchemaFromData.add(edge)

      val schema = inferSchemaFromData.graphSchema
      schema.propertyDefs.size mustEqual 2

      val propDef1 = schema.propertiesWithName("key1").head
      propDef1 mustEqual new PropertyDef(PropertyType.Edge, "key1", classOf[String], false, false)

      val propDef2 = schema.propertiesWithName("key2").head
      propDef2 mustEqual new PropertyDef(PropertyType.Edge, "key2", classOf[Date], false, false)
    }

    "infer the gbId property from a Vertex" in {
      val vertex = new Vertex(new Property("gbId", 10001L), Nil)
      val inferSchemaFromData = new InferSchemaFromData()

      inferSchemaFromData.add(vertex)

      val schema = inferSchemaFromData.graphSchema
      schema.propertyDefs.size mustEqual 1

      val propDef = schema.propertiesWithName("gbId").head
      propDef mustEqual new PropertyDef(PropertyType.Vertex, "gbId", classOf[java.lang.Long], true, true)
    }

    "infer a Vertex property from a Vertex" in {
      val vertex = new Vertex(new Property("gbId", 10001L), List(new Property("key", "value")))
      val inferSchemaFromData = new InferSchemaFromData()

      inferSchemaFromData.add(vertex)

      val schema = inferSchemaFromData.graphSchema
      schema.propertyDefs.size mustEqual 2

      val propDef1 = schema.propertiesWithName("gbId").head
      propDef1 mustEqual new PropertyDef(PropertyType.Vertex, "gbId", classOf[java.lang.Long], true, true)

      val propDef2 = schema.propertiesWithName("key").head
      propDef2 mustEqual new PropertyDef(PropertyType.Vertex, "key", classOf[String], false, false)
    }

    "infer two Vertex properties from a Vertex" in {
      val vertex = new Vertex(new Property("gbId", 10001L), List(new Property("key1", "value1"), new Property("key2", new Date())))
      val inferSchemaFromData = new InferSchemaFromData()

      inferSchemaFromData.add(vertex)

      val schema = inferSchemaFromData.graphSchema
      schema.propertyDefs.size mustEqual 3

      val propDef1 = schema.propertiesWithName("key1").head
      propDef1 mustEqual new PropertyDef(PropertyType.Vertex, "key1", classOf[String], false, false)

      val propDef2 = schema.propertiesWithName("key2").head
      propDef2 mustEqual new PropertyDef(PropertyType.Vertex, "key2", classOf[Date], false, false)
    }

    "combine results from Edges and Vertices" in {
      val edge = new Edge(null, null, "myLabel", List(new Property("key3", "value3")))
      val vertex = new Vertex(new Property("gbId", 10001L), List(new Property("key1", "value1"), new Property("key2", new Date())))
      val inferSchemaFromData = new InferSchemaFromData()

      inferSchemaFromData.add(edge)
      inferSchemaFromData.add(vertex)

      val schema = inferSchemaFromData.graphSchema

      schema.edgeLabelDefs.size mustEqual 1
      schema.edgeLabelDefs.head.label mustEqual "myLabel"

      schema.propertyDefs.size mustEqual 4

      val propDef1 = schema.propertiesWithName("key1").head
      propDef1 mustEqual new PropertyDef(PropertyType.Vertex, "key1", classOf[String], false, false)

      val propDef2 = schema.propertiesWithName("key2").head
      propDef2 mustEqual new PropertyDef(PropertyType.Vertex, "key2", classOf[Date], false, false)

      val propDef3 = schema.propertiesWithName("key3").head
      propDef3 mustEqual new PropertyDef(PropertyType.Edge, "key3", classOf[String], false, false)
    }
  }

}
