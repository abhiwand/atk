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

import com.intel.graphbuilder.parser.InputRow

/**
 * A rule definition for how to Parse properties from columns
 *
 * It is helpful to import ValueDSL._ when creating rules
 *
 * @param key the name for the property
 * @param value the value for the property
 */
class PropertyRule(val key: Value, val value: Value) extends Serializable {

  /**
   * Create a simple property where the source columnName is also the destination property name
   * @param columnName from input row
   */
  def this(columnName: String) {
    this(new ConstantValue(columnName), new ParsedValue(columnName))
  }

  /**
   * Does this rule apply to the supplied row
   */
  def appliesTo(row: InputRow): Boolean = {
    value.in(row)
  }
}
