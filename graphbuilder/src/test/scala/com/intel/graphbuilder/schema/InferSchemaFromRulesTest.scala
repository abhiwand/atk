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

package com.intel.graphbuilder.schema

import com.intel.graphbuilder.parser._
import com.intel.graphbuilder.parser.rule.RuleParserDSL._
import com.intel.graphbuilder.parser.rule.{ DataTypeResolver, EdgeRule, ParsedValue, VertexRule }
import org.scalatest.{ Matchers, WordSpec }

class InferSchemaFromRulesTest extends WordSpec with Matchers {

  "InferSchemaFromRules when given enough rules to be able to infer" should {

    val columnDefs = List(
      new ColumnDef("userId", classOf[Long]),
      new ColumnDef("userName", classOf[String]),
      new ColumnDef("age", classOf[Int]),
      new ColumnDef("movieId", classOf[Long]),
      new ColumnDef("movieTitle", classOf[String]),
      new ColumnDef("rating", classOf[Int]),
      new ColumnDef("date", classOf[String]),
      new ColumnDef("emptyColumn", classOf[String]),
      new ColumnDef("noneColumn", null))
    val inputSchema = new InputSchema(columnDefs)
    val dataTypeParser = new DataTypeResolver(inputSchema)
    val vertexRules = List(
      VertexRule(property("userId"), List(constant("name") -> column("userName"))),
      VertexRule(property("movieId"), List(constant("title") -> column("movieTitle"))))
    val edgeRules = List(new EdgeRule(property("userId"), property("movieId"), constant("watched"), property("date")))

    val inferSchemaFromRules = new InferSchemaFromRules(dataTypeParser, vertexRules, edgeRules)
    val schema = inferSchemaFromRules.inferGraphSchema()

    "infer the watched edge label" in {
      schema.edgeLabelDefs.size shouldBe 1
      schema.edgeLabelDefs.head.label shouldBe "watched"
    }

    "infer the correct number of properties" in {
      schema.propertyDefs.size shouldBe 5
    }

    "infer the date edge property" in {
      schema.propertiesWithName("date").size shouldBe 1
      val actualPropertyDef = schema.propertiesWithName("date").head
      val expectedPropertyDef = new PropertyDef(PropertyType.Edge, "date", classOf[String], false, true)
      actualPropertyDef shouldBe expectedPropertyDef
    }

    "infer the userName vertex property" in {
      schema.propertiesWithName("name").size shouldBe 1
      val actualPropertyDef = schema.propertiesWithName("name").head
      val expectedPropertyDef = new PropertyDef(PropertyType.Vertex, "name", classOf[String], false, true)
      actualPropertyDef shouldBe expectedPropertyDef
    }

    "infer the movieTitle vertex property" in {
      schema.propertiesWithName("title").size shouldBe 1
      val actualPropertyDef = schema.propertiesWithName("title").head
      val expectedPropertyDef = new PropertyDef(PropertyType.Vertex, "title", classOf[String], false, true)
      actualPropertyDef shouldBe expectedPropertyDef
    }

    "infer the user vertex gbId property" in {
      schema.propertiesWithName("userId").size shouldBe 1
      val actualPropertyDef = schema.propertiesWithName("userId").head
      val expectedPropertyDef = new PropertyDef(PropertyType.Vertex, "userId", classOf[Long], true, true)
      actualPropertyDef shouldBe expectedPropertyDef
    }

    "infer the movie vertex gbId property" in {
      schema.propertiesWithName("movieId").size shouldBe 1
      val actualPropertyDef = schema.propertiesWithName("movieId").head
      val expectedPropertyDef = new PropertyDef(PropertyType.Vertex, "movieId", classOf[Long], true, true)
      actualPropertyDef shouldBe expectedPropertyDef
    }

    "report it can infer edge labels" in {
      inferSchemaFromRules.canInferEdgeLabels shouldBe true
    }

    "report it can infer all properties" in {
      inferSchemaFromRules.canInferAllPropertyKeyNames shouldBe true
    }

    "report it can infer the schema" in {
      inferSchemaFromRules.canInferAll shouldBe true
    }

  }

  "InferSchemaFromRules when given incomplete information in the vertex and edge rules" should {

    val columnDefs = List(
      new ColumnDef("id", classOf[Long]),
      new ColumnDef("dynamicPropertyName", classOf[String]), // can't be inferred
      new ColumnDef("dynamicPropertyValue", classOf[Int]),
      new ColumnDef("dynamicLabel", classOf[String]), // can't be inferred
      new ColumnDef("date", classOf[java.util.Date]))
    val inputSchema = new InputSchema(columnDefs)
    val dataTypeParser = new DataTypeResolver(inputSchema)
    val vertexRules = List(
      VertexRule(property("id"), List(column("dynamicPropertyName") -> column("dynamicPropertyValue"))))
    val edgeRules = List(new EdgeRule(property("id"), property("id"), column("dynamicLabel"), property("date")))
    val inferSchemaFromRules = new InferSchemaFromRules(dataTypeParser, vertexRules, edgeRules)
    val graphSchema = inferSchemaFromRules.inferGraphSchema()

    "infer as many properties as possible" in {
      graphSchema.propertyDefs.size shouldBe 2
    }

    "NOT infer edge labels if they aren't available" in {
      graphSchema.edgeLabelDefs.size shouldBe 0
    }

    "report it can't infer edge labels" in {
      inferSchemaFromRules.canInferEdgeLabels shouldBe false
    }

    "report it can't infer all properties" in {
      inferSchemaFromRules.canInferAllPropertyKeyNames shouldBe false
    }

    "report it can't infer the schema" in {
      inferSchemaFromRules.canInferAll shouldBe false
    }
  }

  "InferSchemaFromRules when given incomplete information in the edge rules" should {

    val columnDefs = List(
      new ColumnDef("id", classOf[Long]),
      new ColumnDef("dynamicPropertyName", classOf[String]), // can't be inferred
      new ColumnDef("dynamicPropertyValue", classOf[Int]),
      new ColumnDef("dynamicLabel", classOf[String]), // can't be inferred
      new ColumnDef("date", classOf[java.util.Date]))
    val inputSchema = new InputSchema(columnDefs)
    val dataTypeParser = new DataTypeResolver(inputSchema)
    val vertexRules = List(VertexRule(property("id"), Nil))
    val edgeRules = List(new EdgeRule(property("id"), property("id"), constant("myLabel"), column("dynamicPropertyName") -> column("date")))

    val inferSchemaFromRules = new InferSchemaFromRules(dataTypeParser, vertexRules, edgeRules)
    val graphSchema = inferSchemaFromRules.inferGraphSchema()

    "infer as many properties as possible" in {
      graphSchema.propertyDefs.size shouldBe 1
    }

    "infer edge labels if they are available" in {
      graphSchema.edgeLabelDefs.size shouldBe 1
    }

    "report it can infer edge labels" in {
      inferSchemaFromRules.canInferEdgeLabels shouldBe true
    }

    "report it can't infer all properties" in {
      inferSchemaFromRules.canInferAllPropertyKeyNames shouldBe false
    }

    "report it can't infer the schema" in {
      inferSchemaFromRules.canInferAll shouldBe false
    }
  }

  "InferScheamFromRules" should {

    "throw exception if safeValue() method gets wrong input type" in {
      val inferSchemaFromRules = new InferSchemaFromRules(null, null, null)
      an[RuntimeException] should be thrownBy inferSchemaFromRules.safeValue(new ParsedValue("parsed"))
    }
  }

}
