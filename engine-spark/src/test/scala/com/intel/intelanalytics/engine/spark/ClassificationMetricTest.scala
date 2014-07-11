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

package com.intel.intelanalytics.engine.spark

import com.intel.intelanalytics.engine.TestingSparkContext
import org.scalatest.Matchers

class ClassificationMetricTest extends TestingSparkContext with Matchers {

  // posLabel = 1
  // tp = 1
  // tn = 2
  // fp = 0
  // fn = 1
  val inputListBinary = List(
    Array[Any](0, 0),
    Array[Any](1, 1),
    Array[Any](0, 0),
    Array[Any](1, 0))

  val inputListBinaryChar = List(
    Array[Any]("no", "no"),
    Array[Any]("yes", "yes"),
    Array[Any]("no", "no"),
    Array[Any]("yes", "no"))

  val inputListBinary2 = List(
    Array[Any](0, 0),
    Array[Any](1, 1),
    Array[Any](0, 1),
    Array[Any](1, 1),
    Array[Any](0, 0),
    Array[Any](0, 1),
    Array[Any](0, 0),
    Array[Any](1, 1),
    Array[Any](1, 1),
    Array[Any](0, 0),
    Array[Any](1, 0),
    Array[Any](0, 1),
    Array[Any](1, 1),
    Array[Any](0, 1))

  // tp + tn = 2
  val inputListMulti = List(
    Array[Any](0, 0),
    Array[Any](1, 2),
    Array[Any](2, 1),
    Array[Any](0, 0),
    Array[Any](1, 0),
    Array[Any](2, 1))

  val inputListMultiChar = List(
    Array[Any]("red", "red"),
    Array[Any]("green", "blue"),
    Array[Any]("blue", "green"),
    Array[Any]("red", "red"),
    Array[Any]("green", "red"),
    Array[Any]("blue", "green"))

  "accuracy measure" should "compute correct value for binary classifier" in {
    val rdd = sc.parallelize(inputListBinary)

    val metricValue = SparkOps.modelAccuracy(rdd, 0, 1)
    metricValue shouldEqual 0.75
  }

  "accuracy measure" should "compute correct value for binary classifier with string labels" in {
    val rdd = sc.parallelize(inputListBinaryChar)

    val metricValue = SparkOps.modelAccuracy(rdd, 0, 1)
    metricValue shouldEqual 0.75
  }

  "accuracy measure" should "compute correct value for binary classifier 2" in {
    val rdd = sc.parallelize(inputListBinary2)

    val metricValue = SparkOps.modelAccuracy(rdd, 0, 1)
    val diff = (metricValue - 0.6428571).abs
    diff should be <= 0.0000001
  }

  "accuracy measure" should "compute correct value for multi-class classifier" in {
    val rdd = sc.parallelize(inputListMulti)

    val metricValue = SparkOps.modelAccuracy(rdd, 0, 1)
    val diff = (metricValue - 0.3333333).abs
    diff should be <= 0.0000001
  }

  "accuracy measure" should "compute correct value for multi-class classifier with string labels" in {
    val rdd = sc.parallelize(inputListMultiChar)

    val metricValue = SparkOps.modelAccuracy(rdd, 0, 1)
    val diff = (metricValue - 0.3333333).abs
    diff should be <= 0.0000001
  }

  "precision measure" should "compute correct value for binary classifier" in {
    val rdd = sc.parallelize(inputListBinary)

    val metricValue = SparkOps.modelPrecision(rdd, 0, 1, "1")
    metricValue shouldEqual 1.0
  }

  "precision measure" should "compute correct value for binary classifier with string labels" in {
    val rdd = sc.parallelize(inputListBinaryChar)

    val metricValue = SparkOps.modelPrecision(rdd, 0, 1, "yes")
    metricValue shouldEqual 1.0
  }

  "precision measure" should "compute correct value for binary classifier 2" in {
    val rdd = sc.parallelize(inputListBinary2)

    val metricValue = SparkOps.modelPrecision(rdd, 0, 1, "1")
    val diff = (metricValue - 0.5555555).abs
    diff should be <= 0.0000001
  }

  "precision measure" should "return 0 for binary classifier if posLabel does not exist in label column" in {
    val rdd = sc.parallelize(inputListBinary)

    val metricValue = SparkOps.modelPrecision(rdd, 0, 1, "yoyoyo")
    metricValue shouldEqual 0.0
  }

  "precision measure" should "compute correct value for multi-class classifier" in {
    val rdd = sc.parallelize(inputListMulti)

    val metricValue = SparkOps.modelPrecision(rdd, 0, 1, "1")
    val diff = (metricValue - 0.2222222).abs
    diff should be <= 0.0000001
  }

  "precision measure" should "compute correct value for multi-class classifier with string labels" in {
    val rdd = sc.parallelize(inputListMultiChar)

    val metricValue = SparkOps.modelPrecision(rdd, 0, 1, "red") // posLabel is ignored for multi-class
    val diff = (metricValue - 0.2222222).abs
    diff should be <= 0.0000001
  }

  "recall measure" should "compute correct value for binary classifier" in {
    val rdd = sc.parallelize(inputListBinary)

    val metricValue = SparkOps.modelRecall(rdd, 0, 1, "1")
    metricValue shouldEqual 0.5
  }

  "recall measure" should "compute correct value for binary classifier with string labels" in {
    val rdd = sc.parallelize(inputListBinaryChar)

    val metricValue = SparkOps.modelRecall(rdd, 0, 1, "yes")
    metricValue shouldEqual 0.5
  }

  "recall measure" should "compute correct value for binary classifier 2" in {
    val rdd = sc.parallelize(inputListBinary2)

    val metricValue = SparkOps.modelRecall(rdd, 0, 1, "1")
    val diff = (metricValue - 0.8333333).abs
    diff should be <= 0.0000001
  }

  "recall measure" should "return 0 for binary classifier if posLabel does not exist in label column" in {
    val rdd = sc.parallelize(inputListBinary)

    val metricValue = SparkOps.modelRecall(rdd, 0, 1, "yoyoyo")
    metricValue shouldEqual 0.0
  }

  "recall measure" should "compute correct value for multi-class classifier" in {
    val rdd = sc.parallelize(inputListMulti)

    val metricValue = SparkOps.modelRecall(rdd, 0, 1, "1")
    val diff = (metricValue - 0.3333333).abs
    diff should be <= 0.0000001
  }

  "recall measure" should "compute correct value for multi-class classifier with string labels" in {
    val rdd = sc.parallelize(inputListMultiChar)

    val metricValue = SparkOps.modelRecall(rdd, 0, 1, "red") // posLabel is ignored for multi-class
    val diff = (metricValue - 0.3333333).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for binary classifier for beta = 0.5" in {
    val rdd = sc.parallelize(inputListBinary)

    val metricValue = SparkOps.modelFMeasure(rdd, 0, 1, "1", 0.5)
    val diff = (metricValue - 0.8333333).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for binary classifier 2 for beta = 0.5" in {
    val rdd = sc.parallelize(inputListBinary2)

    val metricValue = SparkOps.modelFMeasure(rdd, 0, 1, "1", 0.5)
    val diff = (metricValue - 0.5952380).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for binary classifier for beta = 1" in {
    val rdd = sc.parallelize(inputListBinary)

    val metricValue = SparkOps.modelFMeasure(rdd, 0, 1, "1", 1)
    val diff = (metricValue - 0.6666666).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for binary classifier for beta = 1 with string labels" in {
    val rdd = sc.parallelize(inputListBinaryChar)

    val metricValue = SparkOps.modelFMeasure(rdd, 0, 1, "yes", 1)
    val diff = (metricValue - 0.6666666).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for binary classifier 2 for beta = 1" in {
    val rdd = sc.parallelize(inputListBinary2)

    val metricValue = SparkOps.modelFMeasure(rdd, 0, 1, "1", 1)
    val diff = (metricValue - 0.6666666).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for binary classifier for beta = 2" in {
    val rdd = sc.parallelize(inputListBinary)

    val metricValue = SparkOps.modelFMeasure(rdd, 0, 1, "1", 2)
    val diff = (metricValue - 0.5555555).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for binary classifier 2 for beta = 2" in {
    val rdd = sc.parallelize(inputListBinary2)

    val metricValue = SparkOps.modelFMeasure(rdd, 0, 1, "1", 2)
    val diff = (metricValue - 0.7575757).abs
    diff should be <= 0.0000001
  }

  "f measure" should "return 0 for binary classifier if posLabel does not exist in label column" in {
    val rdd = sc.parallelize(inputListBinary)

    val metricValue = SparkOps.modelFMeasure(rdd, 0, 1, "yoyoyo", 1)
    metricValue shouldEqual 0.0
  }

  "f measure" should "compute correct value for multi-class classifier for beta = 0.5" in {
    val rdd = sc.parallelize(inputListMulti)

    val metricValue = SparkOps.modelFMeasure(rdd, 0, 1, "1", 0.5)
    val diff = (metricValue - 0.2380952).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for multi-class classifier for beta = 1" in {
    val rdd = sc.parallelize(inputListMulti)

    val metricValue = SparkOps.modelFMeasure(rdd, 0, 1, "1", 1)
    val diff = (metricValue - 0.2666666).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for multi-class classifier for beta = 1 with string labels" in {
    val rdd = sc.parallelize(inputListMultiChar)

    val metricValue = SparkOps.modelFMeasure(rdd, 0, 1, "red", 1)
    val diff = (metricValue - 0.2666666).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for multi-class classifier for beta = 2" in {
    val rdd = sc.parallelize(inputListMulti)

    val metricValue = SparkOps.modelFMeasure(rdd, 0, 1, "1", 2)
    val diff = (metricValue - 0.3030303).abs
    diff should be <= 0.0000001
  }

}