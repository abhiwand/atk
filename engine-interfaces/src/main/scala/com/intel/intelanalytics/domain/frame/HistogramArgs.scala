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

package com.intel.intelanalytics.domain.frame

/**
 * Arguments needed to compute a histogram from a column
 *
 * @param frame frame containing the column to be computed
 * @param columnName name of column to be evalueated
 * @param numBins number of bins in histogram [Optional] if None Square-root choice will be used (ie math.floor(math.sqrt(frame.row_count))
 * @param weightColumnName of column containing weights [Optional] if None all observations are weighted equally
 * @param binType the type of binning algorithm to use: "equalwidth", "equaldepth" defaults to "equalwidth"
 */
case class HistogramArgs(frame: FrameReference, columnName: String, numBins: Option[Int], weightColumnName: Option[String], binType: Option[String] = Some("equalwidth")) {
  require(binType.isEmpty || binType == Some("equalwidth") || binType == Some("equaldepth"), "bin type must be 'equalwidth' or 'equaldepth', not " + binType)
  if (numBins.isDefined)
    require(numBins.get > 0, "the number of bins must be greater than 0")
}
