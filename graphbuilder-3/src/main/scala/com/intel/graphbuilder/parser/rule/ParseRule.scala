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

import com.intel.graphbuilder.parser._

/**
 * Marker interface
 */
trait ParseRule {

}

/**
 * A VertexRule describes how to parse Vertices from InputRows
 * @param gbId a PropertyRule that defines a property will be used as the unique property for parsed vertices
 * @param propertyRules rules describing how to parse properties
 */
case class VertexRule(gbId: PropertyRule, propertyRules: List[PropertyRule] = Nil) extends ParseRule {

  /**
   * Does this rule apply to the supplied row
   */
  def appliesTo(row: InputRow): Boolean = {
    gbId appliesTo row
  }

  /**
   * The complete list of property rules including the special gbId rule
   */
  def fullPropertyRules: List[PropertyRule] = {
    gbId :: propertyRules
  }
}

/**
 * An EdgeRule describes how to parse Edges from InputRows
 * @param tailVertexGbId the rule describing how to parse the source Vertex unique Id
 * @param headVertexGbId the rule describing how to parse the destination Vertex unique Id
 * @param label the value of the Edge label
 * @param propertyRules rules describing how to parse properties
 */
case class EdgeRule(tailVertexGbId: PropertyRule, headVertexGbId: PropertyRule, label: Value, propertyRules: List[PropertyRule] = Nil) extends ParseRule {

  /**
   * Does this rule apply to the supplied row
   */
  def appliesTo(row: InputRow): Boolean = {
    headVertexGbId.appliesTo(row) && tailVertexGbId.appliesTo(row) && label.in(row)
  }

}