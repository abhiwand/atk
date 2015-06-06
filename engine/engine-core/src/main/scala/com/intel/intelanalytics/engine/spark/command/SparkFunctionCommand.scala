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

package com.intel.intelanalytics.engine.spark.command

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.engine.plugin.{ Invocation, FunctionCommand }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.security.UserPrincipal
import spray.json.JsonFormat

import scala.concurrent.ExecutionContext
import scala.reflect.runtime.{ universe => ru }
import ru._
/**
 * Function commands that work with Spark
 */
class SparkFunctionCommand[Arguments <: Product: JsonFormat: ClassManifest: TypeTag, Return <: Product: JsonFormat: ClassManifest: TypeTag](
  name: String,
  function: (Arguments, UserPrincipal, SparkInvocation) => Return,
  numberOfJobsFunc: Arguments => Int,
  doc: Option[CommandDoc])
    extends FunctionCommand[Arguments, Return](
      name, function.asInstanceOf[(Arguments, UserPrincipal, Invocation) => Return], numberOfJobsFunc, doc)
    with SparkCommandPlugin[Arguments, Return] {
  /**
   * Plugins must implement this method to do the work requested by the user.
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments the arguments supplied by the caller
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: Arguments)(implicit invocation: Invocation): Return = {
    //Since the function may come from any class loader, we use the function's
    //class loader, not our own
    withLoader(function.getClass.getClassLoader) {
      function(arguments, invocation.user, invocation.asInstanceOf[SparkInvocation])
    }
  }

}
