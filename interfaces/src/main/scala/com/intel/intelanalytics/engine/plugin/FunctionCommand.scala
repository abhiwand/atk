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

import com.intel.intelanalytics.security.UserPrincipal
import spray.json._

import scala.concurrent.ExecutionContext

/**
 * Encapsulates a normal Scala function as a CommandPlugin.
 *
 * @param name the name to assign to the command
 * @param function the function to call when the command executes
 * @tparam Arguments the argument type of the command
 * @tparam Return the return type of the command
 */
case class FunctionCommand[Arguments <: Product: JsonFormat: ClassManifest, Return <: Product: JsonFormat: ClassManifest](name: String,
                                                                                                                          function: (Arguments, UserPrincipal) => Return)
    extends CommandPlugin[Arguments, Return] {

  override def parseArguments(arguments: JsObject) = arguments.convertTo[Arguments]

  override def serializeReturn(returnValue: Return): JsObject = returnValue.toJson.asJsObject

  override def serializeArguments(arguments: Arguments): JsObject = arguments.toJson.asJsObject()

  /**
   * Operation plugins must implement this method to do the work requested by the user.
   * @param invocation information about the user and the circumstances at the time of the call
   * @param arguments the arguments supplied by the caller
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: Invocation, arguments: Arguments)(implicit user: UserPrincipal, executionContext: ExecutionContext): Return = {
    //Since the function may come from any class loader, we use the function's
    //class loader, not our own
    withLoader(function.getClass.getClassLoader) {
      function(arguments, user)
    }
  }
}
