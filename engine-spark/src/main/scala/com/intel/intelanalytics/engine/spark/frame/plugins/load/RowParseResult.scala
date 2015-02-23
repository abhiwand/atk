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

package com.intel.intelanalytics.engine.spark.frame.plugins.load

/**
 * Parsing results into two types of rows: successes and failures.
 *
 * Successes go into one data frame and errors go into another.
 *
 * @param parseSuccess true if this row was a success, false otherwise
 * @param row either the successfully parsed row OR a row of two columns (original line and error message)
 */
case class RowParseResult(parseSuccess: Boolean, row: Array[Any]) {
  if (!parseSuccess) {
    require(row.length == 2, "error rows have two columns: the original un-parsed line and the error message")
  }
}
