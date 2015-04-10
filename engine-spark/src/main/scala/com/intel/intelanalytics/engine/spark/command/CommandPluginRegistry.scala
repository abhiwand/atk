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

package com.intel.intelanalytics.engine.spark.command

import com.intel.intelanalytics.component.Archive
import com.intel.intelanalytics.domain.command.{ CommandDefinition, CommandDoc }
import com.intel.intelanalytics.engine.plugin.{ CommandPlugin, Invocation }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.shared.JsonSchemaExtractor
import spray.json.JsonFormat
import scala.reflect.runtime.{ universe => ru }
import ru._
/**
 * Register and store command plugin
 */
class CommandPluginRegistry(loader: CommandLoader) {

  private val registry: CommandPluginRegistryMaps = loader.loadFromConfig()
  private def commandPlugins = registry.commandPlugins
  private def pluginsToArchiveMap = registry.pluginsToArchiveMap

  /**
   * Get command plugin by name
   * @param name command name
   * @return optional value of command plugin
   */
  def getCommandPlugin(name: String): Option[CommandPlugin[_, _]] = {
    commandPlugins.get(name)
  }

  /**
   * Adds the given command to the registry.
   * @param command the command to add
   * @return the same command that was passed, for convenience
   */
  def registerCommand[A <: Product, R <: Product](command: CommandPlugin[A, R]): CommandPlugin[A, R] = {
    synchronized {
      registry.commandPlugins += (command.name -> command)
    }
    command
  }

  /**
   * Registers a function as a command using FunctionCommand. This is a convenience method,
   * it is also possible to construct a FunctionCommand explicitly and pass it to the
   * registerCommand method that takes a CommandPlugin.
   *
   * For where numberOfJobs is constant for a command.
   *
   * @param name the name of the command
   * @param function the function to be called when running the command
   * @param numberOfJobs the number of jobs that this command will create (constant)
   * @tparam A the argument type of the command
   * @tparam R the return type of the command
   * @return the CommandPlugin instance created during the registration process.
   */
  def registerCommand[A <: Product: JsonFormat: ClassManifest: TypeTag, R <: Product: JsonFormat: ClassManifest: TypeTag](name: String,
                                                                                                                          function: (A, UserPrincipal, SparkInvocation) => R,
                                                                                                                          numberOfJobs: Int = 1,
                                                                                                                          doc: Option[CommandDoc] = None): CommandPlugin[A, R] = {
    registerCommand(name, function, (A) => numberOfJobs, doc)
  }

  /**
   * Registers a function as a command using FunctionCommand. This is a convenience method,
   * it is also possible to construct a FunctionCommand explicitly and pass it to the
   * registerCommand method that takes a CommandPlugin.
   *
   * For where numberOfJobs can change based on the arguments to a command.
   *
   * @param name the name of the command
   * @param function the function to be called when running the command
   * @param numberOfJobsFunc function for determining the number of jobs that this command will create
   * @tparam A the argument type of the command
   * @tparam R the return type of the command
   * @return the CommandPlugin instance created during the registration process.
   */
  def registerCommand[A <: Product: JsonFormat: ClassManifest: TypeTag, R <: Product: JsonFormat: ClassManifest: TypeTag](name: String,
                                                                                                                          function: (A, UserPrincipal, SparkInvocation) => R,
                                                                                                                          numberOfJobsFunc: (A) => Int,
                                                                                                                          doc: Option[CommandDoc]): CommandPlugin[A, R] = {
    // Note: providing a default =None to the doc parameter causes a strange compiler error where it can't
    // distinguish this method from the one which takes a plain Int for the numberOfJobs. Since the
    // numberOfJobsFunc variation is much less frequently used, we'll make doc a required parameter
    registerCommand(new SparkFunctionCommand(name, function.asInstanceOf[(A, UserPrincipal, Invocation) => R], numberOfJobsFunc, doc))
  }

  /**
   * Returns all the command definitions registered with this command executor.
   *
   * NOTE: this is a val because the underlying operations are not thread-safe -- Todd 3/10/2015
   */
  lazy val commandDefinitions: Iterable[CommandDefinition] =
    commandPlugins.values.map(p => {
      val (argSchema, resSchema) = getArgumentAndResultSchemas(p)
      CommandDefinition(p.name, argSchema, resSchema, p.doc, p.apiMaturityTag)
    })

  private def getArgumentAndResultSchemas(plugin: CommandPlugin[_, _]) = {
    val arg = plugin.argumentManifest
    val ret = plugin.returnManifest
    // It seems that the underlying operations are not thread-safe -- Todd 3/10/2015
    (JsonSchemaExtractor.getProductSchema(arg), JsonSchemaExtractor.getProductSchema(ret))
  }

  //  private var commandPlugins: Map[String, CommandPlugin[_, _]] = pluginRegistry.commandPlugins

  def getCommandDefinition(name: String): Option[CommandPlugin[_, _]] = {
    commandPlugins.get(name)
  }

  // Get archive name for the plugin. If it does not exist in the map, check if it is in commandPlugins as a valid plugin
  def getArchiveNameFromPlugin(name: String): Option[String] = {
    pluginsToArchiveMap.get(name)
  }
}

case class CommandPluginRegistryMaps(var commandPlugins: Map[String, CommandPlugin[_, _]], var pluginsToArchiveMap: Map[String, String])