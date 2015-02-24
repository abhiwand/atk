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

package com.intel.intelanalytics.engine.spark.graph.query.roc

import com.intel.testutils.TestingSparkContextFlatSpec
import org.scalatest.matchers.{ MatchResult, Matcher }
import org.scalatest.{ FlatSpec, Matchers }
import com.intel.testutils.MatcherUtils._

class HistogramTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {
  val tolerance = 0.001

  "Histogram" should "create evenly-spaced buckets" in {
    val buckets = Histogram.makeBuckets(5, 0, 1)
    buckets should equalWithTolerance(Array(0, 0.2, 0.4, 0.6, 0.8, 1), tolerance)
  }
  it should "Throw an IllegalArgumentException when creating buckets where number of buckets is less than or equal to zero" in {
    intercept[IllegalArgumentException] {
      Histogram.makeBuckets(-1, 0, 1)
    }
  }
  it should "Throw an IllegalArgumentException when creating buckets where maximum value is less than minimum value" in {
    intercept[IllegalArgumentException] {
      Histogram.makeBuckets(5, 1, 0.5)
    }
  }

  it should "Compute the minimum and maximum values for an array of doubles" in {
    val rdd = sparkContext.parallelize(Seq(-1.0, 2.0, 3.0, 4.0))
    val (min, max) = Histogram.getMinMax(rdd)
    min should equal(-1.0 +- tolerance)
    max should equal(4.0 +- tolerance)
  }
  it should "Return (NaN, NaN) when computing minimum and maximum values with NaN values" in {
    val rdd = sparkContext.parallelize(Seq(0.0, Double.NaN))
    val (min, max) = Histogram.getMinMax(rdd)
    min.isNaN should equal(true)
    max.isNaN should equal(true)
  }
  it should "Return (-Infinity, +Infinity) when computing minimum and maximum values for an empty array" in {
    val rdd = sparkContext.parallelize(Seq.empty[Double])
    val (min, max) = Histogram.getMinMax(rdd)
    min.isNegInfinity should equal(true)
    max.isPosInfinity should equal(true)
  }

  it should "Compute a histogram for an array of doubles" in {
    val rdd = sparkContext.parallelize(Seq(0.0, 1.0, 2.0, 3.0, 4.0))
    val histogram = Histogram.getHistogram(rdd, 2)
    histogram.buckets should equalWithTolerance(Array(0.0, 2.0, 4.0), tolerance)
    histogram.counts should equal(Array(2, 3))
  }
  it should "Throw an IllegalArgumentException when computing a histogram for an array with infinite values" in {
    intercept[IllegalArgumentException] {
      val rdd = sparkContext.parallelize(Seq(0.0, 1.0 / 0.0))
      Histogram.getHistogram(rdd, 2)
    }
  }
  it should "Throw an IllegalArgumentException when computing a histogram for an array that contains NaNs" in {
    intercept[IllegalArgumentException] {
      val rdd = sparkContext.parallelize(Seq(0.0, Double.NaN))
      Histogram.getHistogram(rdd, 2)
    }
  }
}
