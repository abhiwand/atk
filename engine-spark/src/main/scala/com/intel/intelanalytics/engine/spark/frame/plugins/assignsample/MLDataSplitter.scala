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

package com.intel.intelanalytics.engine.spark.frame.plugins.assignsample

import org.apache.spark.SparkException
import org.apache.spark.rdd._

import scala.reflect.ClassTag

/**
 * Class that represents the entry content and label of a data point.
 *
 * @param label for this data point.
 * @param entry content for this data point.
 */
case class LabeledLine[L: ClassTag, T: ClassTag](label: L, entry: T)

/**
 * Data Splitter for ML algorithms. It randomly labels an input RDD with user
 * specified percentage for each category.
 *
 * @param percentages A double array stores percentages.
 * @param seed Random seed for random number generator.
 */
class MLDataSplitter(percentages: Array[Double], labels: Array[String], seed: Int) extends Serializable {

  require(percentages.forall(p => p > 0d), "MLDataSplitter: Some percentage numbers are negative or zero.")
  require((Math.abs(percentages.sum - 1.0d) < 0.000000001d), "MLDataSplitter: Sum of percentages does not equal  1.")
  require(labels.length == percentages.length, "Number of class labels differs from number of percentages given.")

  var cdf: Array[Double] = percentages.scanLeft(0.0d)(_ + _)
  cdf = cdf.drop(1)

  // clamp the final value to 1.0d so that we cannot get rare (but in big data, still possible!)
  // occurrences where the sample value falls between the gap of the summed input probabilities and 1.0d
  cdf(cdf.length - 1) = 1.0d

  /**
   * Randomly label each entry of an input RDD according to user specified percentage
   * for each category.
   *
   * @param inputRDD RDD of type T.
   */
  def randomlyLabelRDD[T: ClassTag](inputRDD: RDD[T]): RDD[LabeledLine[String, T]] = {
    // generate auxiliary (sample) RDD
    val auxiliaryRDD = new AuxiliaryRDD(inputRDD, seed)
    val labeledRDD = inputRDD.zip(auxiliaryRDD).map { p =>
      val (line, sampleValue) = p
      val label = labels.apply(cdf.indexWhere(_ >= sampleValue))
      LabeledLine(label, line)
    }
    labeledRDD
  }
}