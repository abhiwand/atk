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

package org.apache.spark.mllib.classification.mllib.plugins

import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.spark.frame.FrameRDD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object MLLibMethods {

  /* This method creates a labeled RDD as required by MLLib */
  def createLabeledRDD(inputRDD: FrameRDD, labelColumnName: String, featureColumnNames: List[String]): RDD[LabeledPoint] = {
    inputRDD.mapRows { row =>
      val features = row.values(featureColumnNames).map(value => DataTypes.toDouble(value))
      new LabeledPoint(DataTypes.toDouble(row.value(labelColumnName)), new DenseVector(features.toArray))
    }
//    val inputSplitRDD: RDD[DenseVector] =
//      inputRDD.map { row =>
//        new DenseVector(row.map(item => item.asInstanceOf[Double]).toArray)
//      }
//    inputSplitRDD.map { row =>
//      val features = new DenseVector(row.toArray.slice(1, row.size))
//      val label = row(0)
//      new LabeledPoint(label, features)
//    }
  }

  /* This method invokes MLLib's classification metrics. May be replaced with calls to IAT's metrics */
  def outputClassificationMetrics(scoreAndLabelRDD: RDD[(Double, Double)]) = {
    val bcm = new BinaryClassificationMetrics(scoreAndLabelRDD)
    bcm.areaUnderPR()
    bcm.areaUnderROC()
    val fMeasure = bcm.fMeasureByThreshold()
    fMeasure.collect()
    val precision = bcm.precisionByThreshold()
    precision.collect()
    val recall = bcm.recallByThreshold()
    recall.collect()
    val roc = bcm.roc()
    roc.collect()
    val threshold = bcm.thresholds()
    threshold.collect()
  }
}
