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

package com.intel.intelanalytics.engine.spark.frame.plugins.groupby.aggregators

import org.scalatest.Matchers
import org.scalatest.matchers.{ MatchResult, Matcher }

object MatcherUtils extends Matchers {

  /**
   * Tests if two mean counters are equal +- tolerance.
   */
  def equalWithTolerance(right: MeanCounter, tolerance: Double) = Matcher { (left: MeanCounter) =>
    MatchResult(
      left.count == right.count && left.sum === (right.sum +- tolerance),
      left + " did not equal " + right + " with tolerance " + tolerance,
      left + " equaled " + right + " with tolerance " + tolerance
    )
  }

  /**
   * Tests if two mean counters are equal +- tolerance.
   */
  def equalWithTolerance(right: VarianceCounter, tolerance: Double) = Matcher { (left: VarianceCounter) =>
    MatchResult(
      left.count == right.count &&
        left.mean.value === (right.mean.value +- tolerance) &&
        left.mean.delta === (right.mean.delta +- tolerance) &&
        left.m2.value === (right.m2.value +- tolerance) &&
        left.m2.delta === (right.m2.delta +- tolerance),
      left + " did not equal " + right + " with tolerance " + tolerance,
      left + " equaled " + right + " with tolerance " + tolerance
    )
  }
}
