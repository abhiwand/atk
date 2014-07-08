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

package com.intel.intelanalytics.engine.spark.plugin

import com.intel.intelanalytics.engine.plugin.{ CommandPlugin, Invocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.ExecutionContext

/**
 * Base trait for command plugins that need direct access to a SparkContext
 *
 * @tparam Argument the argument type for the command
 * @tparam Return the return type for the command
 */
trait SparkCommandPlugin[Argument <: Product, Return <: Product]
    extends CommandPlugin[Argument, Return] {

  /**
   * Operation plugins must implement this method to do the work requested by the user.
   * @param invocation information about the user and the circumstances at the time of the call
   * @param arguments the arguments supplied by the caller
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: Invocation, arguments: Argument)(implicit user: UserPrincipal, executionContext: ExecutionContext): Return = {
    execute(invocation.asInstanceOf[SparkInvocation], arguments)(user, executionContext)
  }

  /**
   * Plugins must implement this method to do the work requested by the user.
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments the arguments supplied by the caller
   * @return a value of type declared as the Return type.
   */
  def execute(invocation: SparkInvocation, arguments: Argument)(implicit user: UserPrincipal, executionContext: ExecutionContext): Return
}
