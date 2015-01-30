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

package com.intel.intelanalytics.engine.spark.frame.plugins.bincolumn

import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.{ FrameRDD, SparkFrameData }
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import org.apache.spark.SparkContext._

/**
 * Column values into bins.
 *
 * Two types of binning are provided: equalwidth and equaldepth.
 *
 * Equal width binning places column values into bins such that the values in each bin fall within the same
 * interval and the interval width for each bin is equal.
 *
 * Equal depth binning attempts to place column values into bins such that each bin contains the same number
 * of elements
 */
abstract class ComputedBinColumnPlugin extends SparkCommandPlugin[ComputedBinColumnArgs, BinColumnResults] {

  /**
   * Column values into bins.
   *
   * Two types of binning are provided: equalwidth and equaldepth.
   *
   * Equal width binning places column values into bins such that the values in each bin fall within the same
   * interval and the interval width for each bin is equal.
   *
   * Equal depth binning attempts to place column values into bins such that each bin contains the same number
   * of elements
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ComputedBinColumnArgs)(implicit invocation: Invocation): BinColumnResults = {
    val frame: SparkFrameData = resolve(arguments.frame)
    val columnIndex = frame.meta.schema.columnIndex(arguments.columnName)
    val columnType = frame.meta.schema.columnDataType(arguments.columnName)
    require(columnType.isNumerical, s"Invalid column ${arguments.columnName} for bin column.  Expected a numerical data type, but got $columnType.")
    val binColumnName = arguments.binColumnName.getOrElse(frame.meta.schema.getNewColumnName(arguments.columnName + "_binned"))
    if (frame.meta.schema.hasColumn(binColumnName))
      throw new IllegalArgumentException(s"Duplicate column name: ${arguments.binColumnName}")
    val numBins = HistogramPlugin.getNumBins(arguments.numBins, frame)

    // run the operation and save results
    val updatedSchema = frame.meta.schema.addColumn(binColumnName, DataTypes.int32)
    val rdd = frame.data

    val binnedResults = executeBinColumn(columnIndex, numBins, rdd)

    val frameMeta = save(new SparkFrameData(frame.meta.withSchema(updatedSchema), new FrameRDD(updatedSchema, binnedResults.rdd))).meta

    new BinColumnResults(frameMeta, binnedResults.cutoffs)

  }

  /**
   * Discretize a variable into a finite number of bins
   * @param columnIndex index of column to bin
   * @param numBins number of bins to use
   * @param rdd rdd to bin against
   * @return a result object containing the binned rdd and the list of computed cutoffs
   */
  def executeBinColumn(columnIndex: Int, numBins: Int, rdd: FrameRDD): RddWithCutoffs = ???

}

