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

import com.intel.intelanalytics.component.{ ClassLoaderAware, Plugin }
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.security.UserPrincipal
import spray.json.JsObject
import spray.json._

import scala.reflect.runtime.{ universe => ru }
import ru._

/**
 * Base trait for all operation-based plugins (query and command, for example).
 *
 * Plugin authors should implement the execute() method
 *
 * @tparam Arguments the type of the arguments that the plugin expects to receive from
 *           the user
 * @tparam Return the type of the data that this plugin will return when invoked.
 */
abstract class OperationPlugin[Arguments <: Product: JsonFormat: ClassManifest, Return]
    extends ((Invocation, Arguments) => Return)
    with Plugin
    with ClassLoaderAware {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   *
   * The colon ":" is used to to indicate command destination base classes, default classes or classes of a
   * specific storage type:
   *
   * - graph:titan means command is loaded into class TitanGraph
   * - graph: means command is loaded into class Graph, our default type which will be the Parquet-backed graph
   * - graph would mean command is loaded into class BaseGraph, which applies to all graph classes
   * - frame: and means command is loaded in class Frame.  Example: "frame:/assign_sample"
   * - model:logistic_regression  means command is loaded into class LogisticRegressionModel
   */
  def name: String

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  def doc: Option[CommandDoc] = None

  /**
   * Convert the given JsObject to an instance of the Argument type
   */
  def parseArguments(arguments: JsObject): Arguments = arguments.convertTo[Arguments]

  /**
   * Convert the given argument to a JsObject
   */
  def serializeArguments(arguments: Arguments): JsObject = arguments.toJson.asJsObject()

  /**
   * Operation plugins must implement this method to do the work requested by the user.
   * @param context information about the user and the circumstances at the time of the call
   * @param arguments the arguments supplied by the caller
   * @return a value of type declared as the Return type.
   */
  def execute(arguments: Arguments)(implicit context: Invocation): Return
  /**
   * Invokes the operation, which calls the execute method that each plugin implements.
   * @return the results of calling the execute method
   */
  final override def apply(invocation: Invocation, arguments: Arguments): Return = {
    require(invocation != null, "Invocation required")
    require(arguments != null, "Arguments required")

    //We call execute rather than letting plugin authors directly implement
    //apply so that if we ever need to put additional actions before or
    //after the plugin code, we can.
    withMyClassLoader {
      val result = execute(arguments)(invocation)
      if (result == null) { throw new Exception(s"Plugin ${this.getClass.getName} returned null") }
      result
    }
  }

  implicit def user(implicit invocation: Invocation): UserPrincipal = invocation.user

}

/**
 * Base trait for command plugins
 */
abstract class CommandPlugin[Arguments <: Product: JsonFormat: ClassManifest: TypeTag,
                              Return <: Product: JsonFormat: ClassManifest: TypeTag]
    extends OperationPlugin[Arguments, Return] {

  def engine(implicit invocation: Invocation) = invocation.engine

  val argumentManifest = implicitly[ClassManifest[Arguments]]
  val returnManifest = implicitly[ClassManifest[Return]]
  val argumentTag = implicitly[TypeTag[Arguments]]
  val returnTag = implicitly[TypeTag[Return]]
  /**
   * Convert the given object to a JsObject
   */
  def serializeReturn(returnValue: Return): JsObject = returnValue.toJson.asJsObject

  /**
   * Convert the given JsObject to an instance of the Return type
   */
  def parseReturn(js: JsObject) = js.convertTo[Return]

  /**
   * Number of jobs needs to be known to give a single progress bar
   * @param arguments command arguments: used if a command can produce variable number of jobs
   * @return number of jobs in this command
   */
  def numberOfJobs(arguments: Arguments): Int = 1

  /**
   * Transforms must implement this method to do the work requested by the user.
   *
   * This is different from other plugins' execute methods in that an instance of the
   * return type that was created by the lazy execution engine will be passed in. This
   * pre-created return instance will contain, for example, the UriReferences that
   * were pre-created, and so the Transform instance should re-use those UriReferences
   * in the return object that it returns to avoid creating duplicate frames and other
   * entities.
   *
   * @param context information about the user and the circumstances at the time of the call
   * @param arguments the arguments supplied by the caller
   * @return a value of type declared as the Return type.
   */
  def execute(arguments: Arguments)(implicit context: Invocation): Return = ???
}

/**
 * Base trait for query plugins
 */
abstract class QueryPlugin[Arguments <: Product: JsonFormat: ClassManifest]
  extends OperationPlugin[Arguments, Any] {}

/**
 * Transforms are command plugins that work lazily - they are only executed when
 * the things they produce are needed. For example, a command that takes a frame
 * reference and returns a frame reference. The lazy execution system will create
 * a new (empty) frame and assign an ID to it when this command is called, but
 * the command's execute method will not be called until the frame that was created
 * is actually inspected, exported to a file, or materialized. At that time, the
 * engine will invoke the Transform.
 */
trait Transformation[Arguments <: Product, Return <: Product] extends CommandPlugin[Arguments, Return] {

  final override def execute(arguments: Arguments)(implicit context: Invocation): Return =
    throw new IllegalArgumentException("Transformation called as an action")
}
