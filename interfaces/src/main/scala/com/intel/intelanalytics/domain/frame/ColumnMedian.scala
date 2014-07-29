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

package com.intel.intelanalytics.domain.frame

import spray.json.JsValue

/**
 * Command for calculating the median of a (possibly weighted) dataframe column.
 * @param frame identifier for the input dataframe
 */
case class ColumnMedian(frame: FrameReference, dataColumn: String) {

  // TODO TRIB-2245
  // weightsColumn: Option[String]) {

  require(frame != null, "frame is required")
  require(dataColumn != null, "data column is required")
}

/**
 * The median value of the (possibly weighted) column. None when the sum of the weights is 0.
 *
 * If no weights are provided, all elements receive a uniform weight of 1.
 *
 * If any element receives a weight that is NaN, infinite or <= 0, that element is thrown
 * out of the calculation.
 * @param value The median. None if the net weight of the column is 0.
 */
case class ColumnMedianReturn(value: JsValue)