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

import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.Rows.Row
import scala.math.pow

//implicit conversion for PairRDD
import org.apache.spark.SparkContext._

/**
 * Model Accuracy, Precision, Recall, FMeasure, ConfusionMatrix
 *
 * This is a wrapper to encapsulate methods that may need to be serialized to executed on Spark worker nodes.
 * If you don't know what this means please read about Closure Mishap
 * [[http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf]]
 * and Task Serialization
 * [[http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when]]
 */
object ClassificationMetrics extends Serializable {

  /**
   * Compute accuracy of a classification model
   *
   * @param frameRdd the dataframe RDD containing the labeled and predicted columns
   * @param labelColumnIndex column index for the correctly labeled data
   * @param predColumnIndex column index for the model prediction
   * @return a Double of the model accuracy measure
   */
  def modelAccuracy(frameRdd: RDD[Row], labelColumnIndex: Int, predColumnIndex: Int): Double = {
    require(labelColumnIndex >= 0, "label column index must be greater than or equal to zero")
    require(predColumnIndex >= 0, "prediction column index must be greater than or equal to zero")

    val k = frameRdd.count()
    val t = frameRdd.sparkContext.accumulator[Long](0)

    frameRdd.foreach(row =>
      if (row(labelColumnIndex).toString.equals(row(predColumnIndex).toString)) {
        t.add(1)
      }
    )

    k match {
      case 0 => 0
      case _ => t.value / k.toDouble
    }
  }

  /**
   * Compute precision of a classification model
   *
   * @param frameRdd the dataframe RDD containing the labeled and predicted columns
   * @param labelColumnIndex column index for the correctly labeled data
   * @param predColumnIndex column index for the model prediction
   * @param posLabel the label for a positive instance
   * @return a Double of the model precision measure
   */
  def modelPrecision(frameRdd: RDD[Row], labelColumnIndex: Int, predColumnIndex: Int, posLabel: String): Double = {
    require(labelColumnIndex >= 0, "label column index must be greater than or equal to zero")
    require(predColumnIndex >= 0, "prediction column index must be greater than or equal to zero")

    /**
     * compute precision for binary classifier: TP / (TP + FP)
     */
    def binaryPrecision = {
      val tp = frameRdd.sparkContext.accumulator[Long](0)
      val fp = frameRdd.sparkContext.accumulator[Long](0)

      frameRdd.foreach { row =>
        if (row(labelColumnIndex).toString.equals(posLabel) && row(predColumnIndex).toString.equals(posLabel)) {
          tp.add(1)
        }
        else if (!row(labelColumnIndex).toString.equals(posLabel) && row(predColumnIndex).toString.equals(posLabel)) {
          fp.add(1)
        }
      }
      // if, for example, user specifies pos_label that does not exist, then return 0 for precision
      (tp.value + fp.value) match {
        case 0 => 0.toDouble
        case _ => tp.value / (tp.value + fp.value).toDouble
      }
    }

    /**
     * compute precision for multi-class classifier using weighted averaging
     */
    def multiclassPrecision = {
      val pairedRdd = frameRdd.map(row => (row(labelColumnIndex).toString, row(predColumnIndex).toString)).cache()

      val labelGroupedRdd = pairedRdd.groupBy(pair => pair._1)
      val predictGroupedRdd = pairedRdd.groupBy(pair => pair._2)

      val joinedRdd = labelGroupedRdd.join(predictGroupedRdd)

      val weightedPrecisionRdd: RDD[Double] = joinedRdd.map { label =>
        // label is tuple of (labelValue, (SeqOfInstancesWithThisActualLabel, SeqOfInstancesWithThisPredictedLabel))
        val labelCount = label._2._1.size // get the number of instances with this label as the actual label
        var correctPredict: Long = 0
        val totalPredict = label._2._2.size

        label._2._1.map { prediction =>
          if (prediction._1.equals(prediction._2)) {
            correctPredict += 1
          }
        }
        totalPredict match {
          case 0 => 0
          case _ => labelCount * (correctPredict / totalPredict.toDouble)
        }
      }
      weightedPrecisionRdd.sum() / pairedRdd.count().toDouble
    }

    // determine if this is binary or multi-class classifier
    frameRdd.map(row => row(labelColumnIndex)).distinct().count() match {
      case x if x == 1 || x == 2 => binaryPrecision
      case y if y > 2 => multiclassPrecision
      case _ => throw new IllegalArgumentException()
    }
  }

  /**
   * Compute recall of a classification model
   *
   * @param frameRdd the dataframe RDD containing the labeled and predicted columns
   * @param labelColumnIndex column index for the correctly labeled data
   * @param predColumnIndex column index for the model prediction
   * @param posLabel the label for a positive instance
   * @return a Double of the model recall measure
   */
  def modelRecall(frameRdd: RDD[Row], labelColumnIndex: Int, predColumnIndex: Int, posLabel: String): Double = {
    require(labelColumnIndex >= 0, "label column index must be greater than or equal to zero")
    require(predColumnIndex >= 0, "prediction column index must be greater than or equal to zero")

    /**
     * compute recall for binary classifier: TP / (TP + FN)
     */
    def binaryRecall = {
      val tp = frameRdd.sparkContext.accumulator[Long](0)
      val fn = frameRdd.sparkContext.accumulator[Long](0)

      frameRdd.foreach { row =>
        if (row(labelColumnIndex).toString.equals(posLabel) && row(predColumnIndex).toString.equals(posLabel)) {
          tp.add(1)
        }
        else if (row(labelColumnIndex).toString.equals(posLabel) && !row(predColumnIndex).toString.equals(posLabel)) {
          fn.add(1)
        }
      }

      (tp.value + fn.value) match {
        case 0 => 0.toDouble
        case _ => tp.value / (tp.value + fn.value).toDouble
      }
    }

    /**
     * compute recall for multi-class classifier using weighted averaging
     */
    def multiclassRecall = {
      val pairedRdd = frameRdd.map(row => (row(labelColumnIndex).toString, row(predColumnIndex).toString)).cache()

      val labelGroupedRdd = pairedRdd.groupBy(pair => pair._1)

      val weightedRecallRdd: RDD[Double] = labelGroupedRdd.map { label =>
        // label is tuple of (labelValue, SeqOfInstancesWithThisActualLabel)
        var correctPredict: Long = 0

        label._2.map { prediction =>
          if (prediction._1.equals(prediction._2)) {
            correctPredict += 1
          }
        }
        correctPredict
      }
      weightedRecallRdd.sum() / pairedRdd.count().toDouble
    }

    // determine if this is binary or multi-class classifier
    frameRdd.map(row => row(labelColumnIndex)).distinct().count() match {
      case x if x == 1 || x == 2 => binaryRecall
      case y if y > 2 => multiclassRecall
      case _ => throw new IllegalArgumentException()
    }
  }

  /**
   * Compute f measure of a classification model
   *
   * @param frameRdd the dataframe RDD containing the labeled and predicted columns
   * @param labelColumnIndex column index for the correctly labeled data
   * @param predColumnIndex column index for the model prediction
   * @param posLabel the label for a positive instance
   * @param beta the beta value to use to compute the f measure
   * @return a Double of the model f measure
   */
  def modelFMeasure(frameRdd: RDD[Row], labelColumnIndex: Int, predColumnIndex: Int, posLabel: String, beta: Double): Double = {
    require(labelColumnIndex >= 0, "label column index must be greater than or equal to zero")
    require(predColumnIndex >= 0, "prediction column index must be greater than or equal to zero")

    /**
     * compute recall for binary classifier
     */
    def binaryFMeasure = {
      val tp = frameRdd.sparkContext.accumulator[Long](0)
      val fp = frameRdd.sparkContext.accumulator[Long](0)
      val fn = frameRdd.sparkContext.accumulator[Long](0)

      frameRdd.foreach { row =>
        if (row(labelColumnIndex).toString.equals(posLabel) && row(predColumnIndex).toString.equals(posLabel)) {
          tp.add(1)
        }
        else if (!row(labelColumnIndex).toString.equals(posLabel) && row(predColumnIndex).toString.equals(posLabel)) {
          fp.add(1)
        }
        else if (row(labelColumnIndex).toString.equals(posLabel) && !row(predColumnIndex).toString.equals(posLabel)) {
          fn.add(1)
        }
      }

      val precision = (tp.value + fp.value) match {
        case 0 => 0
        case _ => tp.value / (tp.value + fp.value).toDouble
      }

      val recall = (tp.value + fn.value) match {
        case 0 => 0
        case _ => tp.value / (tp.value + fn.value).toDouble
      }

      ((pow(beta, 2) * precision + recall)) match {
        case 0 => 0.toDouble
        case _ => (1 + pow(beta, 2)) * ((precision * recall) / ((pow(beta, 2) * precision) + recall))
      }
    }

    /**
     * compute f measure for multi-class classifier using weighted averaging
     */
    def multiclassFMeasure = {
      val pairedRdd = frameRdd.map(row => (row(labelColumnIndex).toString, row(predColumnIndex).toString)).cache()

      val labelGroupedRdd = pairedRdd.groupBy(pair => pair._1)
      val predictGroupedRdd = pairedRdd.groupBy(pair => pair._2)

      val joinedRdd = labelGroupedRdd.join(predictGroupedRdd)

      val weightedFMeasureRdd: RDD[Double] = joinedRdd.map { label =>
        // label is tuple of (labelValue, (SeqOfInstancesWithThisActualLabel, SeqOfInstancesWithThisPredictedLabel))
        val labelCount = label._2._1.size // get the number of instances with this label as the actual label
        var correctPredict: Long = 0
        val totalPredict = label._2._2.size

        label._2._1.map { prediction =>
          if (prediction._1.equals(prediction._2)) {
            correctPredict += 1
          }
        }

        val precision = totalPredict match {
          case 0 => 0
          case _ => labelCount * (correctPredict / totalPredict.toDouble)
        }

        val recall = labelCount match {
          case 0 => 0
          case _ => labelCount * (correctPredict / labelCount.toDouble)
        }

        ((pow(beta, 2) * precision) + recall) match {
          case 0 => 0
          case _ => (1 + pow(beta, 2)) * ((precision * recall) / ((pow(beta, 2) * precision) + recall))
        }
      }
      weightedFMeasureRdd.sum() / pairedRdd.count().toDouble
    }

    // determine if this is binary or multi-class classifier
    frameRdd.map(row => row(labelColumnIndex)).distinct().count() match {
      case x if x == 1 || x == 2 => binaryFMeasure
      case y if y > 2 => multiclassFMeasure
      case _ => throw new IllegalArgumentException()
    }
  }

  def confusionMatrix(frameRdd: RDD[Row], labelColumnIndex: Int, predColumnIndex: Int, posLabel: String): Seq[Long] = {
    require(labelColumnIndex >= 0, "label column index must be greater than or equal to zero")
    require(predColumnIndex >= 0, "prediction column index must be greater than or equal to zero")

    val tp = frameRdd.sparkContext.accumulator[Long](0)
    val tn = frameRdd.sparkContext.accumulator[Long](0)
    val fp = frameRdd.sparkContext.accumulator[Long](0)
    val fn = frameRdd.sparkContext.accumulator[Long](0)

    frameRdd.foreach { row =>
      if (row(labelColumnIndex).toString.equals(posLabel) && row(predColumnIndex).toString.equals(posLabel)) {
        tp.add(1)
      }
      else if (!row(labelColumnIndex).toString.equals(posLabel) && !row(predColumnIndex).toString.equals(posLabel)) {
        tn.add(1)
      }
      else if (!row(labelColumnIndex).toString.equals(posLabel) && row(predColumnIndex).toString.equals(posLabel)) {
        fp.add(1)
      }
      else if (row(labelColumnIndex).toString.equals(posLabel) && !row(predColumnIndex).toString.equals(posLabel)) {
        fn.add(1)
      }
    }

    val labels = frameRdd.map(row => row(labelColumnIndex)).distinct().collect()
    labels.size match {
      case x if x == 1 || x == 2 => Seq(tp.value, tn.value, fp.value, fn.value)
      case _ => throw new IllegalArgumentException("Confusion matrix only supports binary classifiers")
    }
  }
}
