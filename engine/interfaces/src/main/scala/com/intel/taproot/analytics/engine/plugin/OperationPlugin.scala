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

package com.intel.taproot.analytics.engine.plugin

import com.intel.taproot.event.{ EventContext, EventLogging }
import com.intel.taproot.analytics.NotNothing
import com.intel.taproot.analytics.component._
import com.intel.taproot.analytics.domain._
import com.intel.taproot.analytics.domain.command.{ CommandDocLoader }
import spray.json.JsObject
import spray.json._

import scala.reflect.runtime.{ universe => ru }
import ru._
import scala.util.control.NonFatal
import com.intel.taproot.analytics.security.UserPrincipal
import com.intel.taproot.analytics.domain.command.CommandDoc
import com.intel.taproot.analytics.engine.plugin.ApiMaturityTag.ApiMaturityTag

/**
 * Base trait for all operation-based plugins (query and command, for example).
 *
 * Plugin authors should implement the execute() method
 *
 * @tparam Arguments the type of the arguments that the plugin expects to receive from
 *                   the user
 * @tparam Return the type of the data that this plugin will return when invoked.
 */
abstract class OperationPlugin[Arguments <: Product: JsonFormat: ClassManifest, Return]
    extends ((Invocation, Arguments) => Return)
    with Plugin
    with EventLogging
    with ClassLoaderAware {

  def withPluginContext[T](context: String)(expr: => T)(implicit invocation: Invocation): T = {
    withContext(context) {
      EventContext.getCurrent.put("plugin_name", name)
      try {
        val caller = user.user
        EventContext.getCurrent.put("user", caller.username.getOrElse(caller.id.toString))
      }
      catch {
        case NonFatal(e) => EventContext.getCurrent.put("user-name-error", e.toString)
      }
      expr
    }(invocation.eventContext)
  }

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
   *
   * The key word 'new' has special meaning.  'model:lda/new' represents the constructor to LdaModel
   */
  def name: String

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  def doc: Option[CommandDoc] = None

  /**
   * Optional Tag for the plugin API
   */
  def apiMaturityTag: Option[ApiMaturityTag] = None

  /**
   * Convert the given JsObject to an instance of the Argument type
   */
  def parseArguments(arguments: JsObject)(implicit invocation: Invocation): Arguments = withPluginContext("parseArguments") {
    arguments.convertTo[Arguments]
  }

  /**
   * Convert the given argument to a JsObject
   */
  def serializeArguments(arguments: Arguments)(implicit invocation: Invocation): JsObject = withPluginContext("serializeArguments") {
    arguments.toJson.asJsObject()
  }

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
  final override def apply(simpleInvocation: Invocation, arguments: Arguments): Return = withPluginContext("apply")({
    require(simpleInvocation != null, "Invocation required")
    require(arguments != null, "Arguments required")

    //We call execute rather than letting plugin authors directly implement
    //apply so that if we ever need to put additional actions before or
    //after the plugin code, we can.
    withMyClassLoader {
      implicit val invocation = customizeInvocation(simpleInvocation, arguments)
      val result = try {
        debug("Invoking execute method with arguments:\n" + arguments)
        execute(arguments)(invocation)
      }
      finally {
        cleanup(invocation)
      }
      if (result == null) {
        throw new Exception(s"Plugin ${this.getClass.getName} returned null")
      }
      debug("Result was:\n" + result)
      result
    }
  })(simpleInvocation)

  /**
   * Can be overridden by subclasses to provide a more specialized Invocation. Called before
   * calling the execute method.
   */
  protected def customizeInvocation(invocation: Invocation, arguments: Arguments) = {
    invocation
  }

  protected def cleanup(invocation: Invocation) = {}

  implicit def user(implicit invocation: Invocation): UserPrincipal = invocation.user

}

/**
 * Base trait for command plugins
 */
abstract class CommandPlugin[Arguments <: Product: JsonFormat: ClassManifest: TypeTag, Return <: Product: JsonFormat: ClassManifest: TypeTag]
    extends OperationPlugin[Arguments, Return] with EventLogging {

  def engine(implicit invocation: Invocation) = invocation.asInstanceOf[CommandInvocation].engine

  val argumentManifest = implicitly[ClassManifest[Arguments]]
  val returnManifest = implicitly[ClassManifest[Return]]
  val argumentTag = implicitly[TypeTag[Arguments]]
  val returnTag = implicitly[TypeTag[Return]]
  val thisManifest = implicitly[ClassManifest[this.type]]
  val thisTag = implicitly[TypeTag[this.type]]
  val argSeparator = ","

  /**
   * Convert the given object to a JsObject
   */
  def serializeReturn(returnValue: Return)(implicit invocation: Invocation): JsObject = withPluginContext("serializeReturn") {
    returnValue.toJson.asJsObject
  }

  /**
   * Convert the given JsObject to an instance of the Return type
   */
  def parseReturn(js: JsObject)(implicit invocation: Invocation) = withPluginContext("parseReturn") {
    js.convertTo[Return]
  }

  /**
   * Number of jobs needs to be known to give a single progress bar
   * @param arguments command arguments: used if a command can produce variable number of jobs
   * @return number of jobs in this command
   */
  def numberOfJobs(arguments: Arguments)(implicit invocation: Invocation): Int = 1

  //  /**
  //   * Resolves a reference down to the requested type
  //   */
  //  def resolve[T <: UriReference: TypeTag](reference: UriReference)(implicit invocation: Invocation): T =
  //    invocation.resolver.resolve(reference).get

  /**
   * Creates an object of the requested type.
   */
  def create[T <: UriReference: TypeTag](args: CreateEntityArgs)(implicit invocation: Invocation, ev: NotNothing[T]): T = withPluginContext("create") {
    invocation.resolver.create[T](args)
  }

  /**
   * Deletes an object of the requested type.
   */
  def delete[T <: UriReference: TypeTag](entity: T)(implicit invocation: Invocation, ev: NotNothing[T]): Unit = withPluginContext("create") {
    invocation.resolver.delete[T](entity)
  }
  /**
   * Save data, possibly creating a new object
   */
  def save[T <: UriReference with HasData: TypeTag](data: T)(implicit invocation: Invocation): T = withPluginContext("save") {
    invocation.resolver.saveData(data)
  }

  /**
   * Create a new object of the requested type, pass it to the block. If block throws an exception,
   * delete the newly created object and rethrow the exception.
   */
  def tryNew[T <: UriReference with HasMetaData: TypeTag](args: CreateEntityArgs = CreateEntityArgs())(block: T => T)(implicit invocation: Invocation) = {
    val thing = create[T](args)
    try {
      block(thing)
    }
    catch {
      case NonFatal(e) =>
        delete(thing)
        throw e
    }
  }

  /**
   * Implicit conversion that lets plugin authors simply add type annotations to UriReferences
   * to convert them to [[com.intel.taproot.analytics.domain.HasMetaData]] or
   * [[com.intel.taproot.analytics.domain.HasData]] instances
   */
  implicit def resolve[In <: UriReference, Out <: UriReference](ref: In)(implicit invocation: Invocation,
                                                                         ev: Out <:< In,
                                                                         tagIn: TypeTag[In],
                                                                         tagOut: TypeTag[Out]): Out =
    withPluginContext("resolve") {
      invocation.resolver.resolve[Out](ref).get
    }

}

/**
 * Base trait for query plugins
 */
abstract class QueryPlugin[Arguments <: Product: JsonFormat: ClassManifest]
  extends OperationPlugin[Arguments, Any] {}

