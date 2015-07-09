/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.taproot.analytics.engine.spark.frame.plugins.classificationmetrics

import com.intel.taproot.testutils.TestingSparkContextFlatSpec
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
