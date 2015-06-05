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

//
//package com.intel.intelanalytics.engine.spark.frame.plugins
//
//import com.intel.intelanalytics.domain.command.CommandDoc
//import com.intel.intelanalytics.domain.frame.DataFrame
//import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
//import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }
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
//@PluginDoc(oneLine = "",
//  extended = "",
//  returns = "")
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
