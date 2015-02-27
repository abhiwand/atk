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

import com.intel.graphbuilder.elements.GBVertex
import com.intel.graphbuilder.parser.{ InputRow, InputSchema, Parser }

/**
 * Parse an InputRow into Vertices using VertexRules
 */
case class VertexRuleParser(inputSchema: InputSchema, vertexRules: List[VertexRule]) extends Parser[GBVertex](inputSchema) with Serializable {

  // each rule gets its own parser
  private val vertexParsers = vertexRules.map(rule => rule -> new SingleVertexRuleParser(rule)).toMap

  /**
   * Parse the supplied InputRow into zero or more Vertices using all applicable rules
   */
  def parse(row: InputRow): Seq[GBVertex] = {
    for {
      rule <- vertexRules
      if rule appliesTo row
    } yield vertexParsers(rule).parse(row)
  }

}

/**
 * Parse an InputRow into a Vertex using a VertexRule
 */
private[graphbuilder] case class SingleVertexRuleParser(rule: VertexRule) extends Serializable {

  private val gbIdParser = new SinglePropertyRuleParser(rule.gbId)
  private val propertyParser = new PropertyRuleParser(rule.propertyRules)

  def parse(row: InputRow): GBVertex = {
    new GBVertex(gbIdParser.parse(row), propertyParser.parse(row))
  }
}
