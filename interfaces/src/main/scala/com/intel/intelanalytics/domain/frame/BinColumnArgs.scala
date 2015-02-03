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
 * Represents a BinColumn object
 *
 * @param frame identifier for the input dataframe
 * @param columnName name of the column to bin
 * @param cutoffs cutoff points of the bins
 * @param includeLowest if true the lowerbound of the bin will be inclusive while the upperbound is exclusive if false it is the opposite
 * @param strictBinning if true values smaller than the first bin or larger than the last bin will not be given a bin.
 *                      if false smaller vales will be in the first bin and larger values will be in the last
 * @param binColumnName name for the created binned column
 */
case class BinColumnArgs(frame: FrameReference, columnName: String, cutoffs: List[Double],
                         includeLowest: Option[Boolean], strictBinning: Option[Boolean], binColumnName: Option[String]) {
  require(frame != null, "frame is required")
  require(columnName != null, "column name is required")
  require(cutoffs.size >= 2, "at least one bin is required")
  require(cutoffs == cutoffs.sorted, "the cutoff points of the bins must be monotonically increasing")
}
