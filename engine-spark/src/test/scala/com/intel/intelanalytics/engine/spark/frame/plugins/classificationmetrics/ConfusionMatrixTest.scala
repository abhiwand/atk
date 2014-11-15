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

package com.intel.intelanalytics.engine.spark.frame.plugins.classificationmetrics

import com.intel.testutils.TestingSparkContextFlatSpec
import org.scalatest.Matchers

class ConfusionMatrixTest extends TestingSparkContextFlatSpec with Matchers {

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

  val inputListMulti = List(
    Array[Any](0, 0),
    Array[Any](1, 2),
    Array[Any](2, 1),
    Array[Any](0, 0),
    Array[Any](1, 0),
    Array[Any](2, 1))

  "confusion matrix" should "compute correct TP, TN, FP, FN values" in {
    val rdd = sparkContext.parallelize(inputListBinary)

    val metricValue = ClassificationMetrics.binaryClassificationMetrics(rdd, 0, 1, "1", 1)
    metricValue.confusionMatrix get "tp" shouldEqual Some(1)
    metricValue.confusionMatrix get "tn" shouldEqual Some(2)
    metricValue.confusionMatrix get "fp" shouldEqual Some(0)
    metricValue.confusionMatrix get "fn" shouldEqual Some(1)
  }

  "confusion matrix" should "compute correct TP, TN, FP, FN values for string labels" in {
    val rdd = sparkContext.parallelize(inputListBinaryChar)

    val metricValue = ClassificationMetrics.binaryClassificationMetrics(rdd, 0, 1, "yes", 1)
    metricValue.confusionMatrix get "tp" shouldEqual Some(1)
    metricValue.confusionMatrix get "tn" shouldEqual Some(2)
    metricValue.confusionMatrix get "fp" shouldEqual Some(0)
    metricValue.confusionMatrix get "fn" shouldEqual Some(1)
  }

  "confusion matrix" should "return an empty map if user gives multi-class data as input" in {
    val rdd = sparkContext.parallelize(inputListMulti)

    val metricValue = ClassificationMetrics.multiclassClassificationMetrics(rdd, 0, 1, 1)
    metricValue.confusionMatrix.isEmpty
  }

}
