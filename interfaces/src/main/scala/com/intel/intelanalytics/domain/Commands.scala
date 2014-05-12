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

package com.intel.intelanalytics.domain

//TODO: Many of these classes will go away in the future, replaced with something more generic.

//TODO: Add more parameters as appropriate
case class Als[GraphRef](graph: GraphRef, lambda: Double, max_supersteps: Option[Int],
                         converge_threshold: Option[Int], feature_dimension: Option[Int])

case class LoadLines[+Arguments, FrameRef](source: String, destination: FrameRef, skipRows: Option[Int], lineParser: Partial[Arguments]) {
  require(source != null, "source is required")
  require(destination != null, "destination is required")
  require(skipRows.isEmpty || skipRows.get >= 0, "cannot skip negative number of rows")
  require(lineParser != null, "lineParser is required")
}

case class FilterPredicate[+Arguments, FrameRef](frame: FrameRef, predicate: String) {
  require(frame != null, "frame is required")
  require(predicate != null, "predicate is required")
}
case class FrameRemoveColumn[+Arguments, FrameRef](frame: FrameRef, column: String) {
  require(frame != null, "frame is required")
  require(column != null, "column is required")
}

case class SeparatorArgs(separator: Char)
