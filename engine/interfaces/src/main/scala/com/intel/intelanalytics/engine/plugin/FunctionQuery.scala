/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.intelanalytics.engine.plugin

import com.intel.intelanalytics.security.UserPrincipal
import spray.json._

import scala.concurrent.ExecutionContext

/**
 * Encapsulates a normal Scala function as a QueryPlugin.
 *
 * @param name the name to assign to the query
 * @param function the function to call when the query executes
 * @tparam Arguments the argument type of the query
 */
case class FunctionQuery[Arguments <: Product: JsonFormat: ClassManifest](name: String,
                                                                          function: (Arguments, UserPrincipal, Invocation) => Any)
    extends QueryPlugin[Arguments] {

  /**
   * Operation plugins must implement this method to do the work requested by the user.
   * @param context information about the user and the circumstances at the time of the call
   * @param arguments the arguments supplied by the caller
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: Arguments)(implicit context: Invocation): Any = {
    //Since the function may come from any class loader, we use the function's
    //class loader, not our own
    withLoader(function.getClass.getClassLoader) {
      function(arguments, context.user, context)
    }
  }
}
