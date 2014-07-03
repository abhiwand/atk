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

/**
 * Represents a MeanColumn object
 * @param frame identifier for the input dataframe
 * @param columnName name of the column to bin
 * @tparam FrameRef
 */
case class ColumnStatistic[FrameRef](frame: FrameRef, operation: String, columnName: String, multiplierColumnName: Option[String]) {

  require(frame != null, "frame is required")
  require(columnName != null, "column name is required")
  require(operation != null, "operation is required")
  require(ColumnStatisticConstants.legalColumnStatistics.contains(operation), "illegal operation specified")

}

/**
 * Represents a MeanColumn return object
 */
case class ColumnStatisticReturn(value: Double) {
}

/**
 * Holds constants related to calculation of per-column statistics.
 */
object ColumnStatisticConstants {
  val legalColumnStatistics = Set("MEAN", "SUM", "MIN", "MAX", "STDEV", "VARIANCE", "GEOMEAN", "MEDIAN", "MODE")
}