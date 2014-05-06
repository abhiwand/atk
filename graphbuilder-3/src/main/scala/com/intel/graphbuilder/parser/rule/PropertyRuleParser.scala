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

import com.intel.graphbuilder.elements.Property
import com.intel.graphbuilder.parser._

/**
 * Parse zero or more properties using a list of rules
 */
case class PropertyRuleParser(propertyRules: Seq[PropertyRule]) extends Serializable {

  // create a parser for each rule
  val propertyParsers = propertyRules.map(rule => rule -> new SinglePropertyRuleParser(rule)).toMap

  /**
   * Parser zero or more properties from the supplied input using the configured rules.
   */
  def parse(row: InputRow): Seq[Property] = {
    for {
      rule <- propertyRules
      if rule appliesTo row
    } yield propertyParsers(rule).parse(row)
  }

}

/**
 * Always parse a single property using a single rule.
 *
 * This parser should be used for GbId's.
 */
private[graphbuilder] case class SinglePropertyRuleParser(rule: PropertyRule) extends Serializable {

  /**
   * Always parse a singe property from the supplied input using the configured rule.
   */
  def parse(row: InputRow): Property = {
    new Property(rule.key.value(row), rule.value.value(row))
  }
}
