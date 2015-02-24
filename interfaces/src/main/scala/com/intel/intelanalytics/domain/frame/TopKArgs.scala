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
 * Command for retrieving the top (or bottom) K distinct values by count for a specified column.
 *
 * @param frame Reference to the input data frame
 * @param columnName Column name
 * @param k Number of entries to return (if negative, return the bottom K values, else return top K)
 * @param weightsColumn Optional. Name of the column that provides weights (frequencies).
 */
case class TopKArgs(frame: FrameReference, columnName: String, k: Int,
                    weightsColumn: Option[String] = None) {
  require(frame != null, "frame is required")
  require(columnName != null, "column name is required")
  require(k != 0, "k should not be equal to zero")
}
