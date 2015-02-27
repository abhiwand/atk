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

package com.intel.intelanalytics.engine.spark.frame.plugins.bincolumn

import com.intel.testutils.TestingSparkContextWordSpec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers

class HistogramTests extends TestingSparkContextWordSpec with Matchers {
  "Histogram" should {
    val plugin = new HistogramPlugin
    "compute data properly for weighted values" in {
      val data = List(
        Array[Any]("A", 1, 3),
        Array[Any]("B", 2, 2),
        Array[Any]("C", 3, .5),
        Array[Any]("D", 4, 1),
        Array[Any]("E", 5, .5))
      val rdd: RDD[Row] = sparkContext.parallelize(data).map(row => new GenericRow(row))
      val numBins = 2
      val hist = plugin.computeHistogram(rdd, 1, Some(2), numBins)
      hist.cutoffs should be(Array(1, 3, 5))
      hist.hist should be(Array(5.0, 2.0))
      hist.density should be(Array(5 / 7.0, 2 / 7.0))
    }

    "compute data properly for unweighted values" in {
      val data = List(
        Array[Any]("A", 1, 3),
        Array[Any]("B", 2, 2),
        Array[Any]("C", 3, .5),
        Array[Any]("D", 4, 1),
        Array[Any]("E", 5, .5))
      val rdd: RDD[Row] = sparkContext.parallelize(data).map(row => new GenericRow(row))
      val numBins = 2
      val hist = plugin.computeHistogram(rdd, 1, None, numBins)
      hist.cutoffs should be(Array(1, 3, 5))
      hist.hist should be(Array(2.0, 3.0))
      hist.density should be(Array(2 / 5.0, 3 / 5.0))
    }

    "observations with negative weights are ignored" in {
      val data = List(
        Array[Any]("A", 1, 3),
        Array[Any]("B", 2, 2),
        Array[Any]("C", 3, .5),
        Array[Any]("D", 4, -1),
        Array[Any]("E", 5, .5))
      val rdd: RDD[Row] = sparkContext.parallelize(data).map(row => new GenericRow(row))
      val numBins = 2
      val hist = plugin.computeHistogram(rdd, 1, Some(2), numBins)
      hist.cutoffs should be(Array(1, 3, 5))
      hist.hist should be(Array(5.0, 1.0))
      hist.density should be(Array(5 / 6.0, 1 / 6.0))
    }

    "can be computed for equal depth" in {
      // Input data
      val inputList = List(
        Array[Any]("A", 1),
        Array[Any]("B", 2),
        Array[Any]("C", 5),
        Array[Any]("D", 7),
        Array[Any]("E", 9))
      val rdd: RDD[Row] = sparkContext.parallelize(inputList, 2).map(row => new GenericRow(row))
      val hist = plugin.computeHistogram(rdd, 1, None, 2, false)
      hist.cutoffs should be(Array(1, 5, 9))
      hist.hist should be(Array(2, 3))
      hist.density should be(Array(2 / 5.0, 3 / 5.0))
    }

    "equal depth supports duplicates" in {
      // Input data
      val inputList = List(
        Array[Any]("A", 1, 3),
        Array[Any]("A", 1, 3),
        Array[Any]("A", 1, 3),
        Array[Any]("B", 2, 2),
        Array[Any]("B", 2, 2),
        Array[Any]("C", 5, 1),
        Array[Any]("D", 7, 3),
        Array[Any]("D", 7, 3),
        Array[Any]("D", 7, 3),
        Array[Any]("E", 9, 5),
        Array[Any]("E", 9, 5),
        Array[Any]("E", 9, 5),
        Array[Any]("E", 9, 5),
        Array[Any]("E", 9, 5))
      val rdd: RDD[Row] = sparkContext.parallelize(inputList, 2).map(row => new GenericRow(row))
      val hist = plugin.computeHistogram(rdd, 1, None, 2, false)
      hist.cutoffs should be(Array(1.0, 7.0, 9.0))
      hist.hist should be(Array(6, 8))
      hist.density should be(Array(6 / 14.0, 8 / 14.0))
    }

    "equal depth supports weights" in {
      // Input data
      val inputList = List(
        Array[Any]("A", 1, 3),
        Array[Any]("B", 2, 2),
        Array[Any]("C", 5, 1),
        Array[Any]("D", 7, 3),
        Array[Any]("E", 9, 5))
      val rdd: RDD[Row] = sparkContext.parallelize(inputList, 2).map(row => new GenericRow(row))
      val hist = plugin.computeHistogram(rdd, 1, Some(2), 2, false)
      hist.cutoffs should be(Array(1.0, 7.0, 9.0))
      hist.hist should be(Array(6, 8))
      hist.density should be(Array(6 / 14.0, 8 / 14.0))
    }

    "zero weights do not throw exceptions" in {
      // Input data
      val inputList = List(
        Array[Any]("A", 1, 3),
        Array[Any]("B", 2, 0),
        Array[Any]("C", 5, -1),
        Array[Any]("D", 7, 0),
        Array[Any]("E", 9, 5))
      val rdd: RDD[Row] = sparkContext.parallelize(inputList, 2).map(row => new GenericRow(row))
      val hist = plugin.computeHistogram(rdd, 1, Some(2), 2, false)
      hist.cutoffs should be(Array(1.0, 7.0, 9.0))
      hist.hist should be(Array(3, 5))
      hist.density should be(Array(3 / 8.0, 5 / 8.0))
    }

    "bins with 0 records are included" in {
      val data = List(
        Array[Any]("A", 1),
        Array[Any]("B", 2),
        Array[Any]("C", 3),
        Array[Any]("D", 9),
        Array[Any]("E", 10))
      val rdd: RDD[Row] = sparkContext.parallelize(data).map(row => new GenericRow(row))
      val numBins = 3
      val hist = plugin.computeHistogram(rdd, 1, None, numBins)
      hist.cutoffs should be(Array(1, 4, 7, 10))
      hist.hist should be(Array(3.0, 0.0, 2.0))
      hist.density should be(Array(3 / 5.0, 0, 2 / 5.0))
    }

  }
}
