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

package com.intel.intelanalytics.engine.spark.frame

import com.intel.intelanalytics.domain.frame.{DataFrame, DataFrameTemplate}
import com.intel.intelanalytics.engine.spark.plugin.{SparkCommandPlugin, SparkInvocation}
import com.intel.intelanalytics.security.UserPrincipal
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

import scala.concurrent.ExecutionContext

class CreateFrame extends SparkCommandPlugin[DataFrameTemplate, DataFrame] {
  /**
   * Plugins must implement this method to do the work requested by the user.
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments the arguments supplied by the caller
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: SparkInvocation, arguments: Argument)
                      (implicit user: UserPrincipal, executionContext: ExecutionContext): Return = {
    invocation.frames.create(arguments)
  }

  //TODO: Replace with generic code that works on any case class
  override def serializeReturn(returnValue: Any): JsObject = returnValue.asInstanceOf[DataFrame].toJson.asJsObject

  //TODO: Replace with generic code that works on any case class
  override def parseArguments(arguments: JsObject) = arguments.convertTo[DataFrameTemplate]

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   */
  override def name: String = "dataframes/create"
}
