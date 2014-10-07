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

package com.intel.intelanalytics.engine.spark.frame.plugins

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ EntropyReturn, Entropy, DataFrame }
import com.intel.intelanalytics.domain.schema.ColumnInfo
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.engine.spark.frame.plugins.statistics.descriptives.ColumnStatistics
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.engine.spark.frame.plugins.statistics.NumericValidationUtils
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.rdd.RDD

import org.apache.spark.SparkContext._

import scala.concurrent.ExecutionContext
import scala.math
import scala.util.Try

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Calculate Shannon entropy of a column.
 *
 * Entropy is a measure of the uncertainty in a random variable.
 */
class ShannonEntropyPlugin extends SparkCommandPlugin[Entropy, EntropyReturn] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:/entropy"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = Some(CommandDoc(oneLineSummary = "Calculate Shannon entropy of a column.",
    extendedSummary = Some("""
    Calculate the Shannon entropy of a column. The column can be weighted. All data elements of weight <= 0
    are excluded from the calculation, as are all data elements whose weight is NaN or infinite.
    If there are no data elements of finite weight > 0, the entropy is zero.

    Parameters
    ----------
    data_column : str
        The column whose entropy is to be calculated

    weights_column : str (Optional)
        The column that provides weights (frequencies) for the entropy calculation.
        Must contain numerical data. Uniform weights of 1 for all items will be used for the calculation if this
        parameter is not provided.

    Returns
    -------
    entropy : float64

    Example
    -------
    >>> entropy = frame.entropy('data column')
    >>> weighted_entropy = frame.entropy('data column', 'weight column')

    ..versionadded :: 0.8 """)))

  /**
   * Calculate Shannon entropy of a column.
   *
   * Entropy is a measure of the uncertainty in a random variable.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @param user current user
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: SparkInvocation, arguments: Entropy)(implicit user: UserPrincipal, executionContext: ExecutionContext): EntropyReturn = {
    // dependencies (later to be replaced with dependency injection)
    val frames = invocation.engine.frames
    val ctx = invocation.sparkContext

    // validate arguments
    val frameRef = arguments.frame
    val frame = frames.expectFrame(frameRef)
    val columnIndex = frame.schema.columnIndex(arguments.dataColumn)

    // run the operation and return results
    val frameRdd = frames.loadFrameRdd(ctx, frameRef.id)
    val weightsColumnOption = frame.schema.column(arguments.weightsColumn)
    val entropy = EntropyRDDFunctions.shannonEntropy(frameRdd, columnIndex, weightsColumnOption)
    EntropyReturn(entropy)
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
private[spark] object EntropyRDDFunctions extends Serializable {

  /**
   * Calculate the Shannon entropy for specified column in data frame.
   *
   * @param frameRDD RDD for data frame
   * @param dataColumnIndex Index of data column
   * @param weightsColumnOption Option for column providing the weights. Must be numerical data.
   * @return Weighted shannon entropy (using natural log)
   */
  def shannonEntropy(frameRDD: RDD[Row],
                     dataColumnIndex: Int,
                     weightsColumnOption: Option[ColumnInfo] = None): Double = {
    require(dataColumnIndex >= 0, "column index must be greater than or equal to zero")

    val dataWeightPairs =
      ColumnStatistics.getDataWeightPairs(dataColumnIndex, weightsColumnOption, frameRDD)
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
