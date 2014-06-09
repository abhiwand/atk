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

case class FrameProject[+Arguments, FrameRef](frame: FrameRef, projected_frame: FrameRef, columns: List[String], new_column_names: List[String]) {
  require(frame != null, "frame is required")
  require(projected_frame != null, "projected frame is required")
  require(columns != null && columns.size > 0, "column is required")
  if (new_column_names != null && new_column_names.size > 0) {
    // TODO - accept a null Json deserialization... for now Python is passing an empty list rather than null
    require(columns.size == new_column_names.size, "number of renamed columns must equal number of columns")
    // TODO - ensure no duplicate names in columns
    // TODO - ensure no duplicate names in renamed_columns
  }
}
