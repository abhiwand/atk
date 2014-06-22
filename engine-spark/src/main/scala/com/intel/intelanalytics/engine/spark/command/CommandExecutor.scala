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

package com.intel.intelanalytics.engine.spark.command

import com.intel.intelanalytics.component.{ArchiveName, Boot}
import com.intel.intelanalytics.domain.command.{Command, CommandTemplate, Execution}
import com.intel.intelanalytics.engine.plugin.{FunctionCommand, CommandPlugin}
import com.intel.intelanalytics.engine.spark.context.SparkContextManager
import com.intel.intelanalytics.engine.spark.plugin.SparkInvocation
import com.intel.intelanalytics.engine.spark.{SparkEngine, SparkEngineConfig}
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.shared.EventLogging
import com.intel.intelanalytics.{ClassLoaderAware, NotFoundException}
import spray.json._

import scala.concurrent._
import scala.util.Try

/**
 * CommandExecutor manages a registry of CommandPlugins and executes them on request.
 *
 * The plugin registry is based on configuration - all Archives listed in the configuration
 * file under intel.analytics.engine.archives will be queried for the "CommandPlugin" key, and
 * any plugins they provide will be added to the plugin registry.
 *
 * Plugins can also be added programmatically using the registerCommand method.
 *
 * Plugins can be executed in three ways:
 *
 * 1. A CommandPlugin can be passed directly to the execute method. The command need not be in
 *    the registry
 * 2. A command can be called by name. This requires that the command be in the registry.
 * 3. A command can be called with a CommandTemplate. This requires that the command named by
 *    the command template be in the registry, and that the arguments provided in the CommandTemplate
 *    can be parsed by the command.
 *
 * @param engine an Engine instance that will be passed to command plugins during execution
 * @param commands a command storage that the executor can use for audit logging command execution
 * @param contextManager a SparkContext factory that can be passed to SparkCommandPlugins during execution
 */
class CommandExecutor(engine: => SparkEngine, commands: SparkCommandStorage, contextManager: SparkContextManager)
      extends EventLogging
        with ClassLoaderAware {

  private var commandPlugins: Map[String, CommandPlugin[_, _]] = SparkEngineConfig.archives.flatMap {
    case (archive, className) => Boot.getArchive(ArchiveName(archive, className))
      .getAll[CommandPlugin[_, _]]("CommandPlugin")
      .map(p => (p.name, p))
  }.toMap

  /**
   * Adds the given command to the registry.
   * @param command the command to add
   * @tparam A the argument type for the command
   * @tparam R the return type for the command
   * @return the same command that was passed, for convenience
   */
  def registerCommand[A,R](command: CommandPlugin[A,R]): CommandPlugin[A,R] = {
    synchronized {
      commandPlugins += (command.name -> command)
    }
    command
  }

  /**
   * Registers a function as a command using FunctionCommand. This is a convenience method,
   * it is also possible to construct a FunctionCommand explicitly and pass it to the
   * registerCommand method that takes a CommandPlugin.
   *
   * @param name the name of the command
   * @param function the function to be called when running the command
   * @tparam A the argument type of the command
   * @tparam R the return type of the command
   * @return the CommandPlugin instance created during the registration process.
   */
  def registerCommand[A : JsonFormat, R : JsonFormat](name: String,
                                                      function: (A, UserPrincipal) => R) : CommandPlugin[A, R] =
    registerCommand(FunctionCommand(name, function))

  private def getCommandDefinition(name: String): Option[CommandPlugin[_, _]] = {
    commandPlugins.get(name)
  }

  /**
   * Executes the given command template, managing all necessary auditing, contexts, class loaders, etc.
   *
   * Stores the results of the command execution back in the persistent command object.
   *
   * @param command the command to run, including name and arguments
   * @param user the user running the command
   * @return an Execution object that can be used to track the command's execution
   */
  def execute[A, R](command: CommandPlugin[A,R],
                                          arguments: A,
                                          user: UserPrincipal,
                                          executionContext: ExecutionContext): Execution = {
    implicit val ec = executionContext
    val cmd = commands.create(CommandTemplate(command.name, Some(command.serializeArguments(arguments))))
    withMyClassLoader {
      withContext("ce.execute") {
        withContext(command.name) {
          val cmdFuture = future {
            withCommand(cmd) {
              val invocation: SparkInvocation = SparkInvocation(engine, commandId = cmd.id, arguments = cmd.arguments,
                user = user, executionContext = implicitly[ExecutionContext],
                sparkContextFactory = () => contextManager.context(user).sparkContext)

              val funcResult = command(invocation, arguments)
              command.serializeReturn(funcResult)
            }
            commands.lookup(cmd.id).get
          }
          Execution(cmd, cmdFuture)
        }
      }
    }
  }

  /**
   * Executes the given command template, managing all necessary auditing, contexts, class loaders, etc.
   *
   * Stores the results of the command execution back in the persistent command object.
   *
   * This overload requires that the command already is registered in the plugin registry using registerCommand.
   *
   * @param name the name of the command to run
   * @param arguments the arguments to pass to the command
   * @param user the user running the command
   * @return an Execution object that can be used to track the command's execution
   */
  def execute[A, R](name: String,
                                          arguments: A,
                                          user: UserPrincipal,
                                          executionContext: ExecutionContext) : Execution= {
    val function = getCommandDefinition(name)
      .getOrElse(throw new NotFoundException("command definition", name))
      .asInstanceOf[CommandPlugin[A,R]]
    execute(function, arguments, user, executionContext)
  }

  /**
   * Executes the given command template, managing all necessary auditing, contexts, class loaders, etc.
   *
   * Stores the results of the command execution back in the persistent command object.
   *
   * This overload requires that the command already is registered in the plugin registry using registerCommand.
   *
   * @param command the CommandTemplate from which to extract the command name and the arguments
   * @param user the user running the command
   * @return an Execution object that can be used to track the command's execution
   */
  def execute[A, R](command: CommandTemplate,
                    user: UserPrincipal,
                    executionContext: ExecutionContext) : Execution = {
    val function = getCommandDefinition(command.name)
      .getOrElse(throw new NotFoundException("command definition", command.name))
      .asInstanceOf[CommandPlugin[A, R]]
    val convertedArgs = function.parseArguments(command.arguments.get)
    execute(function, convertedArgs, user, executionContext)
  }

  private def withCommand[T](command: Command)(block: => JsObject): Unit = {
    commands.complete(command.id, Try {
      block
    })
  }
}
