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
import com.intel.intelanalytics.domain.frame.{ FrameRenameColumns, DataFrame, FlattenColumn }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.ExecutionContext

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

// TODO: shouldn't be a Spark Plugin, doesn't need Spark

/**
 * Rename columns of a frame
 */
class RenameColumnsPlugin extends SparkCommandPlugin[FrameRenameColumns, DataFrame] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:/rename_columns"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = Some(CommandDoc("Rename one or more columns", Some("""
    Renames columns in a frame.

    Parameters
    ----------
    column_names : dictionary of str pairs
        The name pair (existing name, new name).

    Notes
    -----
    Unicode in column names is not supported and will likely cause the drop_frames() function
    (and others) to fail!

    Examples
    --------
    Start with a frame with columns *Wrong* and *Wong*.
    Rename the columns to *Right* and *Wite*::

        my_frame.rename_columns({"Wrong": "Right, "Wong": "Wite"})

    Now, what was *Wrong* is now *Right* and what was *Wong* is now *Wite*.

    .. versionchanged:: 0.8.5""")))

  /**
   * Rename columns of a frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: FrameRenameColumns)(implicit invocation: Invocation): DataFrame = {
    // dependencies (later to be replaced with dependency injection)
    val frames = engine.frames

    val frame = frames.expectFrame(arguments.frame)
    frames.renameColumns(frame, arguments.names.toSeq)
  }
}
