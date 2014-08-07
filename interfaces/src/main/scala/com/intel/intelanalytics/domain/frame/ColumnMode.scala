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
 * Command for calculating the mode of a (possibly weighted) column.
 * @param frame Identifier for the input dataframe.
 */
case class ColumnMode(frame: FrameReference, dataColumn: String, weightsColumn: Option[String]) {

  require(frame != null, "frame is required")
  require(dataColumn != null, "data column is required")
}

/**
 * Mode data for a dataframe column.
 *
 * If no weights are provided, all elements receive a uniform weight of 1.
 * If any element receives a weight that is NaN, infinite or <= 0, that element is thrown
 * out of the calculation.
 *
 * @param mode A data value of maximum weight. Ties are resolved arbitrarily.
 *             None is returned when the sum of the weights is 0.
 * @param weightOfMode Weight of the mode. (If no weight column is specified,
 *                     this is the number of appearances of the mode.)
 * @param totalWeight Total weight in the column. (If no weight column is specified, this is the number of entries
 *                    with finite, non-zero weight.)
 * @param modeCount The number of modes in the data.
 */
case class ColumnModeReturn(mode: JsValue, weightOfMode: Double, totalWeight: Double, modeCount: Long) {
}
