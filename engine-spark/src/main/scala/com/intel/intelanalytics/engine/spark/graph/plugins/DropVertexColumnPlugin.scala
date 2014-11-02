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

package com.intel.intelanalytics.engine.spark.graph.plugins

import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.plugins.DropColumnsPlugin
import com.intel.intelanalytics.engine.spark.plugin.SparkInvocation
import com.intel.intelanalytics.domain.frame.{ FrameReference, DataFrame, FrameDropColumns }
import com.intel.intelanalytics.security.UserPrincipal
import scala.concurrent.ExecutionContext

/**
 * Drop columns from vertex frame.
 */
class DropVertexColumnPlugin extends DropColumnsPlugin {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:vertex/drop_columns"

  val systemFields = Set("_vid", "_label")

  override def execute(arguments: FrameDropColumns)(implicit invocation: Invocation): FrameReference = {
    DropVertexColumnPlugin.rejectInvalidColumns(arguments.columns.toList, systemFields)
    super.execute(arguments)
  }

}

object DropVertexColumnPlugin {

  def rejectInvalidColumns(columns: List[String], invalidColumns: Set[String]) {
    val invalid = columns.filter(s => invalidColumns.contains(s))

    if (!invalid.isEmpty) {
      val canNotDrop = invalid.mkString(",")
      throw new IllegalArgumentException(s"The following columns are not allowed to be dropped: $canNotDrop")
    }
  }

}
