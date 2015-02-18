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

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema.{ Column, DataTypes, FrameSchema, Schema }
import com.intel.intelanalytics.engine.Rows
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.plugins.groupby.GroupByAggregationFunctions
import com.intel.intelanalytics.engine.spark.frame.{ FrameRDD, LegacyFrameRDD, SparkFrameData }
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkContext, sql }
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import org.apache.spark.SparkContext._

class HistogramPlugin extends SparkCommandPlugin[HistogramArgs, Histogram] {

  override def name: String = "frame/histogram"

  /**
   * Compute histogram for a column in a frame.
   * @param arguments histogram arguments, frame, column, column of weights, and number of bins
   * @param invocation current invocation
   * @return Histogram object containing three Seqs one each for, cutoff points of the bins, size of bins, and percentages of total results per bin
   */
  override def execute(arguments: HistogramArgs)(implicit invocation: Invocation): Histogram = {
    val frame: SparkFrameData = resolve(arguments.frame)
    val schema: Schema = frame.meta.schema

    val columnIndex: Int = schema.columnIndex(arguments.columnName)
    val columnType = schema.columnDataType(arguments.columnName)
    require(columnType.isNumerical, s"Invalid column ${arguments.columnName} for histogram.  Expected a numerical data type, but got $columnType.")

    val weightColumnIndex: Option[Int] = arguments.weightColumnName match {
      case Some(n) => {
        val columnType = schema.columnDataType(n)
        require(columnType.isNumerical, s"Invalid column ${n} for bin column.  Expected a numerical data type, but got ${columnType}.")
        Some(schema.columnIndex(n))
      }
      case None => None
    }

    val numBins: Int = HistogramPlugin.getNumBins(arguments.numBins, frame)

    computeHistogram(frame.data, columnIndex, weightColumnIndex, numBins, arguments.binType.getOrElse("equalwidth") == "equalwidth")
  }

  /**
   *
   * @param arguments command arguments: used if a command can produce variable number of jobs
   * @param invocation
   * @return number of jobs in this command
   */
  override def numberOfJobs(arguments: HistogramArgs)(implicit invocation: Invocation): Int = arguments.binType match {
    case Some("equaldepth") => 8
    case _ => 7
  }

  /**
   * compute histogram information from column in a dataFrame
   * @param dataFrame rdd containing the required information
   * @param columnIndex index of column to compute information against
   * @param weightColumnIndex optional index of a column containing the weighted value of each record. Must be numeric, will assume to equal 1 if not included
   * @param numBins number of bins to compute
   * @param equalWidth true if we are using equalwidth binning false if not
   * @return a new RDD containing the inclusive start, exclusive end, size and density of each bin.
   */
  private[bincolumn] def computeHistogram(dataFrame: RDD[Row], columnIndex: Int, weightColumnIndex: Option[Int], numBins: Int, equalWidth: Boolean = true): Histogram = {
    val binnedResults = if (equalWidth)
      DiscretizationFunctions.binEqualWidth(columnIndex, numBins, dataFrame)
    else
      DiscretizationFunctions.binEqualDepth(columnIndex, numBins, weightColumnIndex, dataFrame)

    //get the size of each observation in the rdd. if it is negative do not count the observation
    //todo: warn user if a negative weight appears
    val pairedRDD: RDD[(Int, Double)] = binnedResults.rdd.map(row => (DataTypes.toInt(row.last),
      weightColumnIndex match {
        case Some(i) => math.max(DataTypes.toDouble(row(i)), 0.0)
        case None => HistogramPlugin.UNWEIGHTED_OBSERVATION_SIZE
      })).reduceByKey(_ + _)

    val filledBins = pairedRDD.collect()
    val emptyBins = (0 to binnedResults.cutoffs.length - 2).map(i => (i, 0.0))
    //reduce by key and return either 0 or the value from filledBins
    val bins = (filledBins ++ emptyBins).groupBy(_._1).map {
      case (key, values) => (key, values.map(_._2).max)
    }.toList

    //sort by key return values
    val histSizes: Seq[Double] = bins.sortBy(_._1).map(_._2)

    val totalSize: Double = histSizes.reduce(_ + _)
    val frequencies: Seq[Double] = histSizes.map(size => size / totalSize)

    new Histogram(binnedResults.cutoffs, histSizes, frequencies)
  }

}

object HistogramPlugin {
  val MAX_COMPUTED_NUMBER_OF_BINS: Int = 1000
  val UNWEIGHTED_OBSERVATION_SIZE: Double = 1.0

  def getNumBins(numBins: Option[Int], frame: SparkFrameData): Int = {
    numBins match {
      case Some(n) => n
      case None => {
        math.min(math.floor(math.sqrt(frame.meta.rowCount match {
          case Some(r) => r
          case None => frame.data.count()
        })), HistogramPlugin.MAX_COMPUTED_NUMBER_OF_BINS).toInt
      }
    }
  }
}
