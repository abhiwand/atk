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

package com.intel.intelanalytics.engine.spark.frame.plugins.cumulativedist

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ FrameName, DataFrame, DataFrameTemplate, ECDF }
import com.intel.intelanalytics.domain.schema.{ DataTypes, Schema, Column }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameStorage, LegacyFrameRDD }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.ExecutionContext

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Empirical Cumulative Distribution for a column
 */
class EcdfPlugin extends SparkCommandPlugin[ECDF, DataFrame] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/ecdf"

  override def numberOfJobs(arguments: ECDF) = 6

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  /**
   * Replaced by Mark Aldrich to match other functions.
   *
   * override def doc: Option[CommandDoc] = Some(CommandDoc("Empirical Cumulative Distribution.", Some("""
   *   Empirical Cumulative Distribution.
   */
  override def doc = Some(CommandDoc(oneLineSummary = "Empirical Cumulative Distribution.",
    extendedSummary = Some("""
                           |    Extended Summary
                           |    ----------------
                           |    Generates the :term:`empirical cumulative distribution` for the input column.
                           |
                           |    Parameters
                           |    ----------
                           |    sample_col : str
                           |        The name of the column containing sample.
                           |
                           |    Returns
                           |    -------
                           |    frame : Frame
                           |        a Frame object containing each distinct value in the sample and its
                           |        corresponding ecdf value.
                           |
                           |    Examples
                           |    --------
                           |    Consider the following sample data set in *frame* with actual data labels
                           |    specified in the *labels* column and the predicted labels in the
                           |    *predictions* column::
                           |
                           |        frame.inspect()
                           |
                           |          a:unicode   b:int32
                           |        /---------------------/
                           |           red         1
                           |           blue        3
                           |           blue        1
                           |           green       0
                           |
                           |        result = frame.ecdf('b')
                           |        result.inspect()
                           |
                           |          b:int32   b_ECDF:float64
                           |        /--------------------------/
                           |           1             0.2
                           |           2             0.5
                           |           3             0.8
                           |           4             1.0
                           |
                           |    .. versionadded:: 0.8
                           |
                            """.stripMargin)))

  /**
   * Empirical Cumulative Distribution for a column
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ECDF)(implicit invocation: Invocation): DataFrame = {
    // dependencies (later to be replaced with dependency injection)
    val frames = invocation.engine.frames.asInstanceOf[SparkFrameStorage]

    // validate arguments
    val frame = frames.expectFrame(arguments.frame.id)
    val sampleColumn = frame.schema.column(arguments.column)
    require(sampleColumn.dataType.isNumerical, s"Invalid column ${sampleColumn.name} for ECDF.  Expected a numeric data type, but got ${sampleColumn.dataType}.")
    val ecdfSchema = Schema(List(sampleColumn.copy(), Column(sampleColumn.name + "_ECDF", DataTypes.float64)))
    val ecdfFrameName = arguments.resultFrameName.getOrElse(FrameName.generate(Some("ECDF")).name)

    // run the operation
    val template = DataFrameTemplate(ecdfFrameName, Some("Generated by ECDF"))
    frames.tryNewFrame(template) { ecdfFrame =>
      val rdd = frames.loadLegacyFrameRdd(sc, frame)
      val ecdfRdd = CumulativeDistFunctions.ecdf(rdd, sampleColumn)
      val rowCount = ecdfRdd.count()
      frames.saveLegacyFrame(ecdfFrame, new LegacyFrameRDD(ecdfSchema, ecdfRdd), Some(rowCount))
    }
  }
}
