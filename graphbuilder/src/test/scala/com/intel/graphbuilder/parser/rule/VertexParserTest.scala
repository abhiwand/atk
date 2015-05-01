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

package com.intel.graphbuilder.parser.rule

import RuleParserDSL._
import com.intel.graphbuilder.parser.{ InputRow, InputSchema }
import org.scalatest.{ Matchers, WordSpec }

class VertexParserTest extends WordSpec with Matchers {

  "VertexParser" should {

    // setup data
    val columnNames = List("id", "name", "age", "managerId", "emptyColumn", "noneColumn")
    val rowValues = List("0001", "Joe", 30, "0004", "", None)

    // setup parser dependencies
    val inputSchema = new InputSchema(columnNames, null)
    val inputRow = new InputRow(inputSchema, rowValues)

    "copy the parsed gbId into created vertex" in {
      val parser = new VertexRuleParser(inputSchema, VertexRule(property("id")))
      val vertexList = parser.parse(inputRow)
      vertexList.head.gbId.value shouldBe "0001"
    }

    "copy the parsed gbId into created vertex from different sources" in {
      val parser = new VertexRuleParser(inputSchema, VertexRule(property("managerId")))
      val vertexList = parser.parse(inputRow)
      vertexList.head.gbId.value shouldBe "0004"
    }

    "parse 1 vertex when 1 of 1 rules match" in {
      val parser = new VertexRuleParser(inputSchema, VertexRule(property("id")))
      parser.parse(inputRow).size shouldBe 1
    }

    "parse 2 vertices when 2 of 2 rules match" in {
      val parser = new VertexRuleParser(inputSchema, VertexRule(property("id")) :: VertexRule(property("managerId")))
      parser.parse(inputRow).size shouldBe 2
    }

    "parse 1 vertex when 1 of 2 rules match" in {
      val parser = new VertexRuleParser(inputSchema, new VertexRule(property("id")) :: new VertexRule(property("noneColumn")))
      parser.parse(inputRow).size shouldBe 1
    }

    "parse 0 vertices when 0 of 2 rules match" in {
      val parser = new VertexRuleParser(inputSchema, new VertexRule(property("emptyColumn")) :: new VertexRule(property("noneColumn")))
      parser.parse(inputRow).size shouldBe 0
    }

    "parse 0 vertices when there are no rules" in {
      val parser = new VertexRuleParser(inputSchema, Nil)
      parser.parse(inputRow).size shouldBe 0
    }

    "parse 0 properties when a matching vertex rule does NOT have a matching property rule" in {
      val propertyRules = new PropertyRule("employeeName", column("emptyColumn"))
      val parser = new VertexRuleParser(inputSchema, new VertexRule(property("id"), propertyRules))
      val vertexList = parser.parse(inputRow)
      vertexList.size shouldBe 1
      vertexList.head.properties.size shouldBe 0
    }

    "parse 1 property when a matching vertex rule also has a matching property rule" in {
      val propertyRules = new PropertyRule("employeeName", column("name"))
      val parser = new VertexRuleParser(inputSchema, new VertexRule(property("id"), propertyRules))
      val vertexList = parser.parse(inputRow)
      vertexList.size shouldBe 1
      vertexList.head.properties.size shouldBe 1
      vertexList.head.properties.head.value shouldBe "Joe"
    }

    "parse 2 properties when a matching vertex rule also has 2 matching property rules" in {
      val propertyRules = new PropertyRule("employeeName", column("name")) :: new PropertyRule("age", column("age"))
      val parser = new VertexRuleParser(inputSchema, new VertexRule(property("id"), propertyRules))
      val vertexList = parser.parse(inputRow)
      vertexList.size shouldBe 1
      vertexList.head.properties.size shouldBe 2
    }
  }
}
