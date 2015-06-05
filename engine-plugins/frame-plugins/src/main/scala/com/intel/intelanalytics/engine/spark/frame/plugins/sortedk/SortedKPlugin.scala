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

package com.intel.intelanalytics.engine.spark.frame.plugins.sortedk

import com.intel.intelanalytics.domain.DomainJsonProtocol._
import com.intel.intelanalytics.domain.CreateEntityArgs
import com.intel.intelanalytics.domain.frame.FrameEntity
import com.intel.intelanalytics.engine.plugin.{ ApiMaturityTag, Invocation }
import com.intel.intelanalytics.engine.spark.frame.SparkFrameData
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }

// Implicits needed for JSON conversion
object SortedKJsonFormat {
  implicit val sortedKFormat = jsonFormat4(SortedKArgs)
}

import SortedKJsonFormat._

/**
 * Plugin that returns the top-K rows in a frame ordered by column(s).
 *
 * This plugin is more efficient than sorting the entire frame when
 * K is much smaller than the total number of rows in a frame.
 *
 * The results of this plugin are stored in a new frame.
 * Parameters
 * ----------
 * k : int
 *   The number of sorted rows to copy from the currently active Frame.
 * columns : list of tuples
 *   Each tuple is a column name, and a boolean value that indicates
 *   whether to sort the column in ascending or descending order.
 * reduce_tree_depth : int (optional)
 *   Advanced tuning parameter which determines the depth of the
 *   reduce-tree for the sorted_k plugin. This plugin uses Spark's treeReduce()
 *   for scalability. The default depth is 2.
 */

@PluginDoc(oneLine = "Get a sorted subset of the data.",
  extended = """Take the first k (sorted) rows for the currently active Frame.
Rows are sorted by column values in either ascending or descending order.

Returning the first k (sorted) rows is more efficient than sorting the
entire frame when k is much smaller than the number of rows in the frame.

Notes
-----
The number of sorted rows (k) should be much smaller than the number of rows
in the original frame.

In particular:

1) The number of sorted rows (k) returned should fit in Spark driver memory.
  The maximum size of serialized results that can fit in the Spark driver is
  set by the Spark configuration parameter *spark.driver.maxResultSize*.

2) If you encounter a Kryo buffer overflow exception, increase the Spark
  configuration parameter *spark.kryoserializer.buffer.max.mb*.

3) Use Frame.sort() instead if the number of sorted rows (k) is
  very large (i.e., cannot fit in Spark driver memory).""",
  returns = "A new frame with the first k sorted rows from the original frame.")
class SortedKPlugin extends SparkCommandPlugin[SortedKArgs, FrameEntity] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/sorted_k"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: SortedKArgs)(implicit invocation: Invocation) = 2

  /**
   * Plugin that returns the top-K rows in a frame ordered by column(s).
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return New frame with top-K sorted rows.
   */
  override def execute(arguments: SortedKArgs)(implicit invocation: Invocation): FrameEntity = {
    // load frame
    val frame: SparkFrameData = resolve(arguments.frame)
    val frameRdd = frame.data

    // return new frame with top-k sorted records
    val sortedKFrame = SortedKFunctions.takeOrdered(
      frameRdd,
      arguments.k,
      arguments.columnNamesAndAscending,
      arguments.reduceTreeDepth
    )

    // save the new frame
    val frames = engine.frames
    frames.tryNewFrame(CreateEntityArgs(description = Some("created by sorted_k command"))) {
      newFrame => frames.saveFrameData(newFrame.toReference, sortedKFrame)
    }
  }
}
