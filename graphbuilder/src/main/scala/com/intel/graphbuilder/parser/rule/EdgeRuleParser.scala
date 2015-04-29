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

import com.intel.graphbuilder.parser._
import com.intel.graphbuilder.parser.InputSchema
import com.intel.graphbuilder.elements.GBEdge
import scala.collection.mutable.ListBuffer

/**
 * Parse InputRow's into Edges using EdgeRules
 */
case class EdgeRuleParser(inputSchema: InputSchema, edgeRules: List[EdgeRule]) extends Parser[GBEdge](inputSchema) with Serializable {

  // each rule gets its own parser
  private val edgeParsers = edgeRules.map(rule => rule -> new SingleEdgeRuleParser(rule)).toMap

  /**
   * Parse the supplied InputRow into zero or more Edges using all applicable rules
   */
  def parse(row: InputRow): Seq[GBEdge] = {
    val edges = ListBuffer[GBEdge]()
    for {
      rule <- edgeRules
      if rule appliesTo row
    } {
      val edge = edgeParsers(rule).parse(row)
      edges += edge
      if (rule.biDirectional) {
        edges += edge.reverse()
      }
    }
    edges
  }
}

/**
 * Parse a single InputRow into an Edge
 */
private[graphbuilder] case class SingleEdgeRuleParser(rule: EdgeRule) extends Serializable {

  // each rule can have different rules for parsing tailVertexGbId's, headVertexGbId's, and properties
  private val tailGbIdParser = new SinglePropertyRuleParser(rule.tailVertexGbId)
  private val headGbIdParser = new SinglePropertyRuleParser(rule.headVertexGbId)
  private val propertyParser = new PropertyRuleParser(rule.propertyRules)

  def parse(row: InputRow): GBEdge = {
    new GBEdge(None, tailGbIdParser.parse(row), headGbIdParser.parse(row), rule.label.value(row), propertyParser.parse(row))
  }
}
