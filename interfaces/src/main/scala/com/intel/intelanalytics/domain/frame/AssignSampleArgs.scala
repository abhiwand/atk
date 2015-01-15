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

case class AssignSampleArgs(frame: FrameReference,
                            sample_percentages: List[Double],
                            sample_labels: Option[List[String]] = None,
                            output_column: Option[String] = None,
                            random_seed: Option[Int] = None) {
  require(frame != null, "AssignSample requires a non-null dataframe.")

  require(sample_percentages != null, "AssignSample requires that the percentages vector be non-null.")

  require(sample_percentages.forall(_ >= 0), "AssignSample requires that all percentages be non-negative")
  require(sample_percentages.reduce(_ + _) <= 1, "AssignSample requires that percentages sum to no more than 1")
}
