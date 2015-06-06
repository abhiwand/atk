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

//
//package com.intel.intelanalytics.engine.spark.frame.plugins
//
//import com.intel.intelanalytics.domain.command.CommandDoc
//import com.intel.intelanalytics.domain.frame.DataFrame
//import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
//import com.intel.intelanalytics.security.UserPrincipal
//
//import scala.concurrent.ExecutionContext
//
//// Implicits needed for JSON conversion
//import spray.json._
//import com.intel.intelanalytics.domain.DomainJsonProtocol._
//
///**
//* Template to follow when writing plugins
//*/
//class TemplatePlugin extends SparkCommandPlugin[Args, DataFrame] {
//
//  /**
//   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
//   *
//   * The format of the name determines how the plugin gets "installed" in the client layer
//   * e.g Python client via code generation.
//   */
//  override def name: String = ???
//
//  /**
//   * User documentation exposed in Python.
//   *
//   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
//   */
//  override def doc: Option[CommandDoc] = None
//
//  /**
//   * Number of Spark jobs that get created by running this command
//   * (this configuration is used to prevent multiple progress bars in Python client)
//   */
//  override def numberOfJobs(arguments: Args) = ???
//
//  /**
//   *
//   *
//   * @param invocation information about the user and the circumstances at the time of the call,
//   *                   as well as a function that can be called to produce a SparkContext that
//   *                   can be used during this invocation.
//   * @param arguments user supplied arguments to running this plugin
//   * @param user current user
//   * @return a value of type declared as the Return type.
//   */
//  override def execute(arguments: Args)(implicit invocation: Invocation): DataFrame = {
//    // dependencies (later to be replaced with dependency injection)
//    val frames = engine.frames
//    val ctx = sc
//
//    // validate arguments
//
//    // run the operation
//
//    // save results
//
//    null
//  }
//}
