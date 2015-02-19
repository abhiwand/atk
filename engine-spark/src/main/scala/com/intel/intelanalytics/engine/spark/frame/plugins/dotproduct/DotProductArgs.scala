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

package com.intel.intelanalytics.engine.spark.frame.plugins.dotproduct

import com.intel.intelanalytics.domain.frame.FrameReference

/**
 * Arguments for dot-product plugin
 *
 * The dot-product plugin uses the left column values, and the right column values to compute the dot product for each row.
 *
 * @param frame Data frame
 * @param leftColumnNames Left column names
 * @param rightColumnNames Right column names
 * @param dotProductColumnName Name of column for storing results of dot-product
 * @param defaultLeftValues Optional default values used to substitute null values in the left columns
 * @param defaultRightValues Optional default values used to substitute null values in the right columns
 */
case class DotProductArgs(frame: FrameReference,
                          leftColumnNames: List[String],
                          rightColumnNames: List[String],
                          dotProductColumnName: String,
                          defaultLeftValues: Option[List[Double]] = None,
                          defaultRightValues: Option[List[Double]] = None) {
  require(frame != null, "frame is required")
  require(leftColumnNames.size > 0, "number of left columns cannot be zero")
  require(leftColumnNames.size == rightColumnNames.size, "number of left columns should equal number of right columns")
  require(dotProductColumnName != null, "dot product column name cannot be null")
  require(defaultLeftValues.isEmpty || defaultLeftValues.get.size == leftColumnNames.size,
    "size of default left values should equal number of left columns")
  require(defaultRightValues.isEmpty || defaultRightValues.get.size == rightColumnNames.size,
    "size of default right values should equal number of right columns")
}
