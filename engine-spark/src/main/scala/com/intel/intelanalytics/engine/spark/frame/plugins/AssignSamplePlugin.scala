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
import com.intel.intelanalytics.domain.frame.{ AssignSample, DataFrame }
import com.intel.intelanalytics.domain.schema.{ Schema, DataTypes }
import com.intel.intelanalytics.engine.spark.frame.FrameRDD
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.spark.mllib.util.MLDataSplitter

import scala.concurrent.ExecutionContext

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Assign classes to rows.
 */
class AssignSamplePlugin extends SparkCommandPlugin[AssignSample, DataFrame] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:/assign_sample"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = Some(CommandDoc(oneLineSummary = "Assign classes to rows.",
    extendedSummary = Some("""
    Randomly assign classes to rows given a vector of percentages.
    The table receives an additional column that contains a random label generated by the probability distribution
    function specified by a list of floating point values.
    The labels are non-negative integers drawn from the range [ 0,  len(split_percentages) - 1].
    Optionally, the user can specify a list of strings to be used as the labels. If the number of labels is 3,
    the labels will default to "TR", "TE" and "VA".

    Parameters
    ----------
    sample_percentages : list of floating point values
        Entries are non-negative and sum to 1.
        If the *i*'th entry of the  list is *p*,
        then then each row receives label *i* with independent probability *p*.
    sample_labels : str (optional)
        Names to be used for the split classes.
        Defaults "TR", "TE", "VA" when there are three numbers given in split_percentages,
        defaults to Sample#0, Sample#1, ... otherwise.
    output_column : str (optional)
        Name of the new column which holds the labels generated by the function
    random_seed : int (optional)
        Random seed used to generate the labels. Defaults to 0.

    Examples
    --------
    For this example, my_frame is a BigFrame object accessing a frame with data.
    Append a new column *sample_bin* to the frame;
    Assign the value in the new column to "train", "test", or "validate"::

        my_frame.assign_sample([0.3, 0.3, 0.4], ["train", "test", "validate"])

    Now the frame accessed by BigFrame *my_frame* has a new column named "sample_bin" and each row contains one of the values "train",
    "test", or "validate".  Values in the other columns are unaffected.
    """.toString)))

  /**
   * Assign classes to rows.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @param user current user
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: SparkInvocation, arguments: AssignSample)(implicit user: UserPrincipal, executionContext: ExecutionContext): DataFrame = {
    // dependencies (later to be replaced with dependency injection)
    val frames = invocation.engine.frames
    val ctx = invocation.sparkContext

    // validate arguments
    val frameID = arguments.frame.id
    val frame = frames.expectFrame(frameID)
    val splitPercentages = arguments.sample_percentages.toArray
    val outputColumn = arguments.output_column.getOrElse("sample_bin")
    if (frame.schema.columns.indexWhere(columnTuple => columnTuple._1 == outputColumn) >= 0)
      throw new IllegalArgumentException(s"Duplicate column name: $outputColumn")
    val seed = arguments.random_seed.getOrElse(0)

    val splitLabels: Array[String] = if (arguments.sample_labels.isEmpty) {
      if (splitPercentages.length == 3) {
        Array("TR", "TE", "VA")
      }
      else {
        (0 to splitPercentages.length - 1).map(i => "Sample#" + i).toArray
      }
    }
    else {
      arguments.sample_labels.get.toArray
    }

    // run the operation
    val splitter = new MLDataSplitter(splitPercentages, splitLabels, seed)
    val labeledRDD = splitter.randomlyLabelRDD(frames.loadFrameRdd(ctx, frameID))
    val splitRDD = labeledRDD.map(labeledRow => labeledRow.entry :+ labeledRow.label.asInstanceOf[Any])
    val allColumns = frame.schema.columns :+ (outputColumn, DataTypes.string)

    // save results
    frames.saveFrame(frame, new FrameRDD(new Schema(allColumns), splitRDD))
  }
}
