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

import com.intel.intelanalytics.component.ClassLoaderAware
import com.intel.intelanalytics.domain._
import com.intel.intelanalytics.domain.frame.{DataFrame, DataFrameTemplate, FrameReference}
import com.intel.intelanalytics.domain.graph.{GraphReference, Graph}
import com.intel.intelanalytics.engine.{Reflection, Engine, CommandStorage}
import com.intel.intelanalytics.engine.plugin.{Action, Invocation, CommandPlugin}
import com.intel.intelanalytics.engine.spark.context.SparkContextManager
import com.intel.intelanalytics.engine.spark.SparkEngine
import com.intel.intelanalytics.NotFoundException
import org.apache.hadoop.hdfs.web.resources.UriFsPathParam
import org.apache.spark.SparkContext
import spray.json._

import scala.concurrent._
import scala.reflect.api.JavaUniverse
import scala.reflect.api._
import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.{universe => ru}
import ru._
import scala.util.Try
import org.apache.spark.engine.{ProgressPrinter, SparkProgressListener}
import com.intel.intelanalytics.domain.command.CommandTemplate
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.command.Execution
import com.intel.intelanalytics.engine.spark.plugin.{SparkCommandPlugin, SparkInvocation}
import com.intel.intelanalytics.domain.command.Command
import scala.collection.mutable
import com.intel.event.EventLogging
import scala.concurrent.duration._

case class CommandContext(
                           command: Command,
                           action: Boolean,
                           executionContext: ExecutionContext,
                           user: UserPrincipal,
                           plugins: CommandPluginRegistry,
                           resolver: ReferenceResolver,
                           dynamic: mutable.Map[String, Any] = mutable.Map.empty,
                           cleaners: mutable.Map[String, () => Unit] = mutable.Map.empty) {

  def apply[T](name: String): Option[T] = {
    dynamic.get(name).map(o => o.asInstanceOf[T])
  }

  def put[T](name: String, value: T, cleaner: () => Unit = null) = {
    dynamic.put(name, value)
    if (cleaner != null) {
      cleaners.put(name, cleaner)
    }
  }

  def clean(): Unit = {
    cleaners.keys.foreach { key =>
      val func = cleaners(key)
      func()
    }
    cleaners.clear()
    dynamic.clear()
  }

}

/**
 * CommandExecutor manages a registry of CommandPlugins and executes them on request.
 *
 * The plugin registry is based on configuration - all Archives listed in the configuration
 * file under intel.analytics.engine.archives will be queried for the "command" key, and
 * any plugins they provide will be added to the plugin registry.
 *
 * Plugins can also be added programmatically using the registerCommand method.
 *
 * Plugins can be executed in three ways:
 *
 * 1. A CommandPlugin can be passed directly to the execute method. The command need not be in
 * the registry
 * 2. A command can be called by name. This requires that the command be in the registry.
 * 3. A command can be called with a CommandTemplate. This requires that the command named by
 * the command template be in the registry, and that the arguments provided in the CommandTemplate
 * can be parsed by the command.
 *
 * @param engine an Engine instance that will be passed to command plugins during execution
 * @param commands a command storage that the executor can use for audit logging command execution
 * @param contextManager a SparkContext factory that can be passed to SparkCommandPlugins during execution
 */
class CommandExecutor(engine: => SparkEngine, commands: SparkCommandStorage, contextManager: SparkContextManager)
  extends EventLogging
  with ClassLoaderAware {

  case class SimpleInvocation(engine: Engine,
                              commandStorage: CommandStorage,
                              executionContext: ExecutionContext,
                              arguments: Option[JsObject],
                              commandId: Long,
                              user: UserPrincipal) extends Invocation

  val commandIdContextMapping = new mutable.HashMap[Long, SparkContext]()

  /**
   * Create all the necessary empty metadata objects needed to generate a valid Return object without actually
   * running a Command.
   *
   * Note that this will fail if called on a return type that has members other than UriReference subtypes, since
   * those cannot be lazily suspended.
   *
   * @param command the command we're simulating
   * @param plugin the actual command plugin implementation
   * @param arguments an instance of Arguments that was passed with the actual argument values
   * @tparam Arguments the type of arguments to the command
   * @tparam Return the type of the return value of the command
   * @return an instance of the Return type with all the generated references.
   */
  def createSuspendedReferences[Arguments <: Product : TypeTag, Return <: Product : TypeTag](command: Command, plugin: CommandPlugin[Arguments, Return], arguments: Arguments): Return = {
    val types = Reflection.getUriReferenceTypes[Return]()
    val references = types.map {
      case (name, signature) =>
        val management: EntityManagement = EntityRegistry.entityForType(signature.typeSymbol.asType).get
        (name, management.create())
    }.toMap
    val ctorMap = Reflection.getConstructorMap[Return]()
    ctorMap(references)
  }

  /**
   * Executes the given command template, managing all necessary auditing, contexts, class loaders, etc.
   *
   * Stores the results of the command execution back in the persistent command object.
   *
   * @return an Execution object that can be used to track the command's execution
   */
  private def executeContext[A <: Product : TypeTag, R <: Product : TypeTag](commandContext: CommandContext, firstExecution: Boolean = true): Execution = {
    implicit val ec = commandContext.executionContext
    withMyClassLoader {
      withContext(commandContext.command.name) {
        val plugin = expectFunction[A, R](commandContext.plugins, commandContext.command)
        val arguments = plugin.parseArguments(commandContext.command.arguments.get)
        val cmdFuture = future {
          withContext("cmdFuture") {
            withCommand(commandContext.command) {
              if (commandContext.action) {
                val res = if (firstExecution) {
                  this.runDependencies(arguments, commandContext)(plugin.argumentTag)
                }
                else commandContext.resolver
                val result = invokeCommandFunction(plugin, arguments, commandContext.copy(resolver = res))
                if (firstExecution) {
                  commandContext.clean
                }
                result
              }
              else {
                val result = createSuspendedReferences(commandContext.command, plugin, arguments)
                plugin.serializeReturn(result)
              }
            }
          }
          commands.lookup(commandContext.command.id).get
        }
        Execution(commandContext.command, cmdFuture)
      }
    }
  }

  def runDependencies[A <: Product : TypeTag](arguments: A,
                                              commandContext: CommandContext): ReferenceResolver = {
    val dependencies = Dependencies.getComputables(arguments)
    val newResolver = dependencies match {
      case x if x.isEmpty => ReferenceResolver
      case _ =>
        var dependencySets = Dependencies.build(dependencies)
        var resolver = new AugmentedResolver(commandContext.resolver, Seq.empty)
        while (dependencySets.nonEmpty) {
          info(s"Resolving dependencies for ${commandContext.command.name}, ${dependencySets.length} rounds remain")
          val commands = dependencySets.head
          val results = commands.map(c => {
            executeContext(commandContext.copy(command = c, action = true, resolver = resolver), firstExecution = false)
          })
          //TODO: next, examine the results and add uri references to the resolver,
          //if they are HasData instances.
          //TODO: Realistic, non-hard-coded timeout
          val refs = results.map(exc => Await.result(exc.end, 100 hours))
            .flatMap(obj => Dependencies.getUriReferencesFromJsObject(obj.result.get))
          val data = refs.flatMap {
            case r: UriReference with HasData => Seq(r)
            case _ => Seq.empty
          }.toSeq
          resolver = resolver ++ data
          dependencySets = dependencySets.tail
        }
        resolver
    }
    newResolver
  }

  private def invokeCommandFunction[A <: Product : TypeTag, R <: Product : TypeTag](command: CommandPlugin[A, R],
                                                                                    arguments: A,
                                                                                    commandContext: CommandContext): JsObject = {
    val cmd = commandContext.command
    try {
      val invocation = command match {
        case c: SparkCommandPlugin[A, R] =>
          val context: SparkContext = commandContext("sparkContext") match {
            case None =>
              val sc: SparkContext = createSparkContextForCommand(command, arguments, commandContext.user, commandContext.command)
              commandContext.put("sparkContext", sc, () => sc.stop())
              sc
            case Some(sc) => sc
          }

          SparkInvocation(engine,
            commandId = cmd.id,
            arguments = cmd.arguments,
            user = commandContext.user,
            executionContext = commandContext.executionContext,
            sparkContext = context,
            commandStorage = commands)

        case c: CommandPlugin[A, R] =>
          SimpleInvocation(engine,
            commandStorage = commands,
            commandId = cmd.id,
            arguments = cmd.arguments,
            user = commandContext.user,
            executionContext = commandContext.executionContext)
      }
      info(s"Invoking command ${cmd.name}")
      val funcResult = command(invocation, arguments)
      command.serializeReturn(funcResult)
    }
    finally {
      stopCommand(cmd.id)
    }
  }

  /**
   * Is this an action? A command is an action if it declares so by implementing the Action
   * trait, or else by having a return type that has members that are not references.
   *
   * The second case makes this an action because there is no way to return a pending value
   * of an arbitrary type. UriReferences are a special case, we can create one ahead of time
   * and intercept calls to it, but we can't return a lazy Int (especially across process and
   * protocol boundaries) where a regular Int is requested.
   *
   * @tparam R the return type of the plugin
   * @return true if the plugin is an action, false otherwise.
   */
  def isAction[R <: Product](plugin: CommandPlugin[_, R]) = {
    plugin.isInstanceOf[Action] || {
      implicit val returnTag = plugin.returnTag
      val dataMembers: Seq[(String, Type)] = Reflection.getVals[R]()
      val referenceTypes: Seq[(String, Type)] = Reflection.getUriReferenceTypes[R]()
      dataMembers.length != referenceTypes.length
    }
  }

  def createSparkContextForCommand[R <: Product, A <: Product](command: CommandPlugin[A, R],
                                                               arguments: A,
                                                               user: UserPrincipal,
                                                               cmd: Command): SparkContext = withMyClassLoader {
    val commandId = cmd.id
    val commandName = cmd.name
    val context: SparkContext = contextManager.context(user, s"(id:$commandId,name:$commandName)")
    try {
      val listener = new SparkProgressListener(SparkProgressListener.progressUpdater, cmd, command.numberOfJobs(arguments))
      val progressPrinter = new ProgressPrinter(listener)
      context.addSparkListener(listener)
      context.addSparkListener(progressPrinter)
    }
    catch {
      // exception only shows up here due to dev error, but it is hard to debug without this logging
      case e: Exception => error("could not create progress listeners", exception = e)
    }
    commandIdContextMapping += (commandId -> context)
    context
  }

  /**
   * Executes the given command template, managing all necessary auditing, contexts, class loaders, etc.
   *
   * Stores the results of the command execution back in the persistent command object.
   *
   * This overload requires that the command already is registered in the plugin registry using registerCommand.
   *
   * @param commandTemplate the CommandTemplate from which to extract the command name and the arguments
   * @param user the user running the command
   * @return an Execution object that can be used to track the command's execution
   */
  def execute[A <: Product, R <: Product](commandTemplate: CommandTemplate,
                                          user: UserPrincipal,
                                          executionContext: ExecutionContext,
                                          commandPluginRegistry: CommandPluginRegistry,
                                          referenceResolver: ReferenceResolver = ReferenceResolver): Execution =
    withContext("ce.execute(ct)") {
      val cmd = commands.create(commandTemplate)
        val plugin = expectFunction[A, R](commandPluginRegistry, cmd)
        val context = CommandContext(cmd,
          action = isAction(plugin),
          executionContext,
          user,
          commandPluginRegistry,
          referenceResolver,
          mutable.Map.empty,
          mutable.Map.empty)

        implicit val argTag = plugin.argumentTag
        implicit val retTag = plugin.returnTag
        executeContext(context)(argTag, retTag)
    }

  private def expectFunction[A <: Product, R <: Product](plugins: CommandPluginRegistry, command: Command): CommandPlugin[A, R] = {
    plugins.getCommandDefinition(command.name)
      .getOrElse(throw new NotFoundException("command definition", command.name))
      .asInstanceOf[CommandPlugin[A, R]]
  }

  private def withCommand[T](command: Command)(block: => JsObject): Unit = {
    commands.complete(command.id, Try {
      block
    })
  }

  /**
   * Stop a command
   * @param commandId command id
   */
  def stopCommand(commandId: Long): Unit = {
    commandIdContextMapping.get(commandId).foreach { case (context) => context.stop()}
    commandIdContextMapping -= commandId
  }
}
