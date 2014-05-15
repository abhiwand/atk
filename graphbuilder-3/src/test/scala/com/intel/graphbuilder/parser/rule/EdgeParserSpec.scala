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

package com.intel.graphbuilder.parser.rule

import RuleParserDSL._
import com.intel.graphbuilder.parser.{ InputSchema, InputRow }
import org.specs2.mutable.Specification

class EdgeParserSpec extends Specification {

  // setup data
  val columnNames = List("userId", "userName", "age", "movieId", "movieTitle", "rating", "date", "emptyColumn", "noneColumn")
  val rowValues = List("0001", "Joe", 30, "0004", "The Titanic", 4, "Jan 2014", "", None)

  // setup parser dependencies
  val inputSchema = new InputSchema(columnNames, null)
  val inputRow = new InputRow(inputSchema, rowValues)

  "EdgeParser" should {

    "copy vertex gbId values into edge when EdgeRule matches" in {
      val parser = new SingleEdgeRuleParser(new EdgeRule(property("userId"), property("movieId"), constant("watched")))
      val edge = parser.parse(inputRow)

      edge.tailVertexGbId.key mustEqual "userId"
      edge.tailVertexGbId.value mustEqual "0001"

      edge.headVertexGbId.key mustEqual "movieId"
      edge.headVertexGbId.value mustEqual "0004"
    }

    "copy label value into edge when EdgeRule matches" in {
      val parser = new SingleEdgeRuleParser(new EdgeRule(property("userId"), property("movieId"), constant("watched")))
      val edge = parser.parse(inputRow)

      edge.label mustEqual "watched"
    }

    "handle dynamic labels parsed from input into the edge when EdgeRule matches" in {
      val parser = new SingleEdgeRuleParser(new EdgeRule(property("userId"), property("movieId"), column("rating")))
      val edge = parser.parse(inputRow)

      edge.label mustEqual "4" // Even though input was an Int, labels must always get converted to Strings
    }

    "parse properties into edge when EdgeRule matches" in {
      val propertyRules = List(constant("when") -> column("date"), property("rating"))
      val parser = new SingleEdgeRuleParser(new EdgeRule(property("userId"), property("movieId"), constant("watched"), propertyRules))
      val edge = parser.parse(inputRow)

      edge.properties must have size 2
      edge.properties.head.key mustEqual "when"
      edge.properties.head.value mustEqual "Jan 2014"

      edge.properties.last.key mustEqual "rating"
      edge.properties.last.value mustEqual 4
    }
  }

  "EdgeListParser" should {

    "parse 0 edges when 0 of 1 EdgeRules match" in {
      val parser = new EdgeRuleParser(inputSchema, EdgeRule(property("userId"), property("emptyColumn"), constant("watched")))
      parser.parse(inputRow) must have size 0
    }

    "parse 1 edge when 1 of 1 EdgeRules match" in {
      val propertyRules = List(constant("when") -> column("date"), property("rating"))
      val parser = new EdgeRuleParser(inputSchema, EdgeRule(property("userId"), property("movieId"), constant("watched")))
      parser.parse(inputRow) must have size 1
    }

    "parse 1 edge when 1 of 2 EdgeRules match" in {
      val edgeRules = List(EdgeRule(property("userId"), property("movieId"), constant("watched")),
        EdgeRule(property("userId"), property("emptyColumn"), constant("watched")))
      val parser = new EdgeRuleParser(inputSchema, edgeRules)
      parser.parse(inputRow) must have size 1
    }

    "parse 2 edges when 2 of 2 EdgeRules match" in {
      val edgeRules = List(new EdgeRule(property("userId"), property("movieId"), constant("watched")),
        new EdgeRule(property("movieId"), property("userId"), constant("watchedBy")))
      val parser = new EdgeRuleParser(inputSchema, edgeRules)
      parser.parse(inputRow) must have size 2
    }
  }
}
