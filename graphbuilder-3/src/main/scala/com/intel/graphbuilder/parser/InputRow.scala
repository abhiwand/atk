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

package com.intel.graphbuilder.parser

import org.apache.commons.lang3.StringUtils

/**
 * Wrapper for rows to simplify parsing.
 * <p>
 * An input row is row of raw data plus a schema
 * </p>
 */
class InputRow(inputSchema: InputSchema, row: Seq[Any]) {

  if (row.size != inputSchema.size) {
    throw new IllegalArgumentException("Input row should have the same number of columns as the inputSchema")
  }

  /**
   * The data or value from a column.
   */
  def value(columnName: String): Any = {
    row(inputSchema.columnIndex(columnName))
  }

  /**
   * Column is considered empty if it is null, None, Nil, or the empty String
   */
  def columnIsNotEmpty(columnName: String): Boolean = {
    val any = row(inputSchema.columnIndex(columnName))
    any match {
      case null => false
      case None => false
      case Nil => false
      case s: String => StringUtils.isNotEmpty(s)
      case _ => true
    }
  }

}