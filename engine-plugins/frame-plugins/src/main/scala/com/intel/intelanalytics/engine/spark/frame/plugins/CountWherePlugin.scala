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

package com.intel.intelanalytics.engine.spark.frame.plugins

import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.PythonRddStorage
import com.intel.intelanalytics.domain.frame.CountWhereArgs
import org.bson.BSON
import org.bson.types.BasicBSONList
import scala.concurrent.ExecutionContext
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.LongValue
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Counts rows which meet criteria specified by a UDF predicate
 */
@PluginDoc(oneLine = "",
  extended = "",
  returns = "")
class CountWherePlugin extends SparkCommandPlugin[CountWhereArgs, LongValue] {

  override def name: String = "frame/count_where"

  /**
   * Return count of rows which meet criteria specified by a UDF predicate
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: CountWhereArgs)(implicit invocation: Invocation): LongValue = {
    val sourceFrame = engine.frames.expectFrame(arguments.frame)
    val pythonRDDStorage = new PythonRddStorage(engine.frames)
    val pyRdd = pythonRDDStorage.createPythonRDD(sourceFrame.toReference, arguments.udf.function, sc)
    LongValue(pyRdd.map(s => BSON.decode(s).get("array").asInstanceOf[BasicBSONList].size()).fold(0)(_ + _))
  }
}
