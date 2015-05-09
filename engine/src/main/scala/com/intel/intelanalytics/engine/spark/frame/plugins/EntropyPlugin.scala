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

package com.intel.intelanalytics.engine.spark.frame.plugins

import com.intel.intelanalytics.domain.DoubleValue
import com.intel.intelanalytics.domain.frame.EntropyArgs
import com.intel.intelanalytics.domain.schema.Column
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.plugins.statistics.descriptives.ColumnStatistics
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.engine.spark.frame.plugins.statistics.NumericValidationUtils
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.rdd.RDD

import org.apache.spark.SparkContext._

import scala.concurrent.ExecutionContext
import scala.util.Try

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Calculate Shannon entropy of a column.
 *
 * Entropy is a measure of the uncertainty in a random variable.
 */
class EntropyPlugin extends SparkCommandPlugin[EntropyArgs, DoubleValue] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/entropy"

  /**
   * Calculate Shannon entropy of a column.
   *
   * Entropy is a measure of the uncertainty in a random variable.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: EntropyArgs)(implicit invocation: Invocation): DoubleValue = {
    // dependencies (later to be replaced with dependency injection)
    val frames = engine.frames

    // validate arguments
    val frameRef = arguments.frame
    val frame = frames.expectFrame(frameRef)
    val columnIndex = frame.schema.columnIndex(arguments.dataColumn)

    // run the operation and return results
    val frameRdd = frames.loadLegacyFrameRdd(sc, frameRef)
    val weightsColumnOption = frame.schema.column(arguments.weightsColumn)
    val entropy = EntropyRddFunctions.shannonEntropy(frameRdd, columnIndex, weightsColumnOption)
    DoubleValue(entropy)
  }
}

/**
 * Functions for computing entropy.
 *
 * Entropy is a measure of the uncertainty in a random variable.
 *
 * This is a wrapper to encapsulate methods that may need to be serialized to executed on Spark worker nodes.
 * If you don't know what this means please read about Closure Mishap
 * [[http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf]]
 * and Task Serialization
 * [[http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when]]
 */
private[spark] object EntropyRddFunctions extends Serializable {

  /**
   * Calculate the Shannon entropy for specified column in data frame.
   *
   * @param frameRdd RDD for data frame
   * @param dataColumnIndex Index of data column
   * @param weightsColumnOption Option for column providing the weights. Must be numerical data.
   * @return Weighted shannon entropy (using natural log)
   */
  def shannonEntropy(frameRdd: RDD[Row],
                     dataColumnIndex: Int,
                     weightsColumnOption: Option[Column] = None): Double = {
    require(dataColumnIndex >= 0, "column index must be greater than or equal to zero")

    val dataWeightPairs =
      ColumnStatistics.getDataWeightPairs(dataColumnIndex, weightsColumnOption, frameRdd)
        .filter({ case (data, weight) => NumericValidationUtils.isFinitePositive(weight) })

    val distinctCountRDD = dataWeightPairs.reduceByKey(_ + _).map({ case (value, count) => count })

    // sum() throws an exception if RDD is empty so catching it and returning zero
    val totalCount = Try(distinctCountRDD.sum()).getOrElse(0d)

    val entropy = if (totalCount > 0) {
      val distinctProbabilities = distinctCountRDD.map(count => count / totalCount)
      -distinctProbabilities.map(probability => if (probability > 0) probability * math.log(probability) else 0).sum()
    }
    else 0d

    entropy
  }
}