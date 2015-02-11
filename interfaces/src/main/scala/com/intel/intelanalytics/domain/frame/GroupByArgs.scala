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

/** Arguments to GroupByPlugin (see Spark API) */
case class GroupByArgs(frame: FrameReference, groupByColumns: List[String], aggregations: List[GroupByAggregationArgs]) {
  require(frame != null, "frame is required")
  require(groupByColumns != null, "group_by columns is required")
  require(aggregations != null, "aggregation list is required")
}

/**
 * Arguments for GroupBy aggregation
 *
 * @param function Name of aggregation function (e.g., count, sum, variance)
 * @param columnName Name of column to aggregate
 * @param newColumnName Name of new column that stores the aggregated results
 */
case class GroupByAggregationArgs(function: String, columnName: String, newColumnName: String)
