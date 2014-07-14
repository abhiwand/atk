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

package com.intel.intelanalytics.engine.plugin

import com.intel.intelanalytics.component.{ClassLoaderAware, Plugin, Component}
import com.intel.intelanalytics.security.UserPrincipal
import com.typesafe.config.Config
import spray.json.JsObject

import scala.concurrent.ExecutionContext

/**
 * Base trait for all operation-based plugins (query and command, for example).
 *
 * Plugin authors should implement the execute() method
 *
 * @tparam Argument the type of the arguments that the plugin expects to receive from
 *           the user
 * @tparam Return the type of the data that this plugin will return when invoked.
 */
sealed abstract class OperationPlugin[Argument <: Product: ClassManifest, Return <: Product: ClassManifest] extends ((Invocation, Any) => Return)
    with Plugin
    with ClassLoaderAware {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   */
  def name: String

  /**
   * Operation plugins must implement this method to do the work requested by the user.
   * @param invocation information about the user and the circumstances at the time of the call
   * @param arguments the arguments supplied by the caller
   * @return a value of type declared as the Return type.
   */
  def execute(invocation: Invocation, arguments: Argument)(implicit user: UserPrincipal, executionContext: ExecutionContext): Return

  /**
   * Invokes the operation, which calls the execute method that each plugin implements.
   * @return the results of calling the execute method
   */
  final override def apply(invocation: Invocation, arguments: Any): Return = {
    require(invocation != null, "Invocation required")
    require(arguments != null, "Arguments required")

    //We call execute rather than letting plugin authors directly implement
    //apply so that if we ever need to put additional actions before or
    //after the plugin code, we can.
    withMyClassLoader {
      val result = execute(invocation, arguments.asInstanceOf[Argument])(invocation.user, invocation.executionContext)
      if (result == null) { throw new Exception(s"Plugin ${this.getClass.getName} returned null") }
      result
    }
  }
}

/**
 * Base trait for command plugins
 */
abstract class CommandPlugin[Argument <: Product: ClassManifest, Return <: Product: ClassManifest]
    extends OperationPlugin[Argument, Return] {

  val argumentManifest = implicitly[ClassManifest[Argument]]
  val returnManifest = implicitly[ClassManifest[Return]]

  /**
   * Convert the given JsObject to an instance of the Argument type
   */
  def parseArguments(arguments: JsObject): Argument

  /**
   * Convert the given argument to a JsObject
   */
  def serializeArguments(arguments: Argument): JsObject

  /**
   * Convert the given object to a JsObject
   */
  def serializeReturn(returnValue: Return): JsObject
}

/**
 * Base trait for query plugins
 */
//trait QueryPlugin[Argument, Return] extends OperationPlugin[Argument, Return] {

//}