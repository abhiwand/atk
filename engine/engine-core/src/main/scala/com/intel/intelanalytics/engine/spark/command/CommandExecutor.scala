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

import java.io.File
import java.nio.file.{ FileSystems, Files }

import com.intel.intelanalytics.engine.spark.hadoop.HadoopSupport
import com.intel.intelanalytics.engine.spark.threading.EngineExecutionContext

import sys.process._

import com.intel.intelanalytics.component.ClassLoaderAware
import com.intel.intelanalytics.domain._
import com.intel.intelanalytics.engine._
import com.intel.intelanalytics.engine.plugin.{ Invocation, CommandPlugin }
import com.intel.intelanalytics.engine.spark.util.{ JvmMemory, KerberosAuthenticator }
import com.intel.intelanalytics.engine.spark.{SparkContextFactory, SparkEngineConfig, SparkEngine}
import com.intel.intelanalytics.{ EventLoggingImplicits, NotFoundException }
import spray.json._

import scala.concurrent._
import scala.reflect.runtime.{ universe => ru }
import ru._
import scala.util.Try
import com.intel.intelanalytics.domain.command.CommandTemplate
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.domain.command.Execution
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin }
import com.intel.intelanalytics.domain.command.Command
import scala.collection.mutable
import com.intel.event.{ EventContext, EventLogging }
import scala.concurrent.duration._
import com.intel.intelanalytics.engine.spark.threading.EngineExecutionContext.global

case class CommandContext(
    command: Command,
    executionContext: ExecutionContext,
    user: UserPrincipal,
    plugins: CommandPluginRegistry,
    resolver: ReferenceResolver,
    eventContext: EventContext,
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
 */
//class CommandExecutor(engine: => SparkEngine, commands: CommandStorage, contextFactory: SparkContextFactory)
class CommandExecutor(engine: => SparkEngine, commands: CommandStorage)
    extends EventLogging
    with EventLoggingImplicits
    with ClassLoaderAware {

  /**
   * Executes the given command template, managing all necessary auditing, contexts, class loaders, etc.
   *
   * Stores the results of the command execution back in the persistent command object.
   *
   * This overload requires that the command already is registered in the plugin registry using registerCommand.
   *
   * @param commandTemplate the CommandTemplate from which to extract the command name and the arguments
   * @return an Execution object that can be used to track the command's execution
   */
  def execute[A <: Product, R <: Product](commandTemplate: CommandTemplate,
                                          commandPluginRegistry: CommandPluginRegistry,
                                          referenceResolver: ReferenceResolver = ReferenceResolver)(implicit invocation: Invocation): Execution =
    withContext("ce.execute(ct)") {
      val cmd = commands.create(commandTemplate.copy(createdBy = if (invocation.user != null) Some(invocation.user.user.id) else None))
      val plugin = expectCommandPlugin[A, R](commandPluginRegistry, cmd)
      val context = CommandContext(cmd,
        EngineExecutionContext.global,
        user,
        commandPluginRegistry,
        referenceResolver,
        eventContext,
        mutable.Map.empty,
        mutable.Map.empty)

      createExecution(context)(plugin.argumentTag, plugin.returnTag, invocation)
    }

  def executeCommand[A <: Product, R <: Product](cmd: Command,
                                                 commandPluginRegistry: CommandPluginRegistry,
                                                 referenceResolver: ReferenceResolver = ReferenceResolver)(implicit invocation: Invocation): Unit =
    withContext(s"ce.executeCommand.${cmd.name}") {
      val plugin = expectCommandPlugin[A, R](commandPluginRegistry, cmd)
      val context = CommandContext(cmd,
        EngineExecutionContext.global,
        user,
        commandPluginRegistry,
        referenceResolver,
        eventContext,
        mutable.Map.empty,
        mutable.Map.empty)

      /* Stores the (intermediate) results, don't mark the command complete yet as it will be marked complete by rest server */
      commands.storeResult(context.command.id, Try { executeCommandContext(context, true) })
    }

  /**
   * Executes the given command template, managing all necessary auditing, contexts, class loaders, etc.
   *
   * Stores the results of the command execution back in the persistent command object.
   *
   * @return an Execution object that can be used to track the command's execution
   */
  private def createExecution[A <: Product: TypeTag, R <: Product: TypeTag](commandContext: CommandContext,
                                                                            firstExecution: Boolean = true)(implicit invocation: Invocation): Execution = {
    Execution(commandContext.command, executeCommandContextInFuture(commandContext, firstExecution))
  }

  /**
   * Execute the command in the future with correct classloader, context, etc.,
   * On complete - mark progress as 100% or failed
   */
  private def executeCommandContextInFuture[T](commandContext: CommandContext, firstExecution: Boolean)(implicit invocation: Invocation): Future[Command] = {
    withMyClassLoader {
      withContext(commandContext.command.name) {
        // run the command in a future so that we don't make the client wait for initial response
        val cmdFuture = future {
          commands.complete(commandContext.command.id, Try {
            executeCommandContext(commandContext, firstExecution)
          })
          // get the latest command progress from DB when command is done executing
          commands.lookup(commandContext.command.id).get
        }
        cmdFuture
      }
    }
  }

  /**
   * Execute commandContext in current thread (assumes correct classloader, context, etc. is already setup)
   * @tparam R plugin return type
   * @tparam A plugin arguments
   * @return plugin return value as JSON
   */
  private def executeCommandContext[R <: Product: TypeTag, A <: Product: TypeTag](commandContext: CommandContext, firstExecution: Boolean)(implicit invocation: Invocation): JsObject = withContext("cmdExcector") {
    info(s"command id:${commandContext.command.id}, name:${commandContext.command.name}, args:${commandContext.command.compactArgs}, ${JvmMemory.memory}")
    val plugin = expectCommandPlugin[A, R](commandContext.plugins, commandContext.command)
    val arguments = plugin.parseArguments(commandContext.command.arguments.get)
    implicit val commandInvocation = getInvocation(plugin, arguments, commandContext)
    debug(s"System Properties are: ${sys.props.keys.mkString(",")}")

    if (plugin.isInstanceOf[SparkCommandPlugin[A, R]] && !sys.props.contains("SPARK_SUBMIT") && SparkEngineConfig.isSparkOnYarn) {
      val archiveName = commandContext.plugins.getArchiveNameFromPlugin(plugin.name)
      val sparkSubmitExitCode = executeCommandOnYarn(commandContext.command, plugin, archiveName)
      // Reload the command as the error/result etc fields should have been updated in metastore upon yarn execution
      val updatedCommand = commands.lookup(commandContext.command.id).get
      if (updatedCommand.error.isDefined) {
        throw new Exception(s"Error executing ${plugin.name}: ${updatedCommand.error.get.message}")
      }
      if (updatedCommand.result.isDefined) {
        updatedCommand.result.get
      }
      else {
        error(s"Command didn't have any results, this is probably do to an error submitting command to yarn-cluster: $updatedCommand")
        throw new Exception(s"Error submitting command to yarn-cluster.")
      }
    }
    else {
      val res = commandContext.resolver
      val returnValue = invokeCommandFunction(plugin, arguments, commandContext.copy(resolver = res))
      plugin.serializeReturn(returnValue)
    }
  }

  private def getPluginJarPath(pluginJarsList: List[String], delimiter: String = ","): String = {
    pluginJarsList.filter(_ != "engine-core")
      .map(j => SparkContextFactory.jarPath(j)).mkString(delimiter)
  }

  private def executeCommandOnYarn[A <: Product: TypeTag, R <: Product: TypeTag](command: Command, plugin: CommandPlugin[A, R], archiveName: Option[String])(implicit invocation: Invocation): Int = {
    withContext("executeCommandOnYarn") {

      val tempConfFileName = s"/tmp/application_${command.id}.conf"
      val sparkCommandPlugin = plugin.asInstanceOf[SparkCommandPlugin[A, R]]
      val pluginArchiveName = archiveName.getOrElse(sparkCommandPlugin.getArchiveName())

      /* Serialize current config for the plugin so as to pass to Spark Submit */
      val (pluginJarsList, pluginExtraClasspath) = sparkCommandPlugin.serializePluginConfiguration(pluginArchiveName, tempConfFileName)

      try {

        withMyClassLoader {
          //Requires a TGT in the cache before executing SparkSubmit if CDH has Kerberos Support
          KerberosAuthenticator.loginWithKeyTabCLI()
          val (kerbFile, kerbOptions) = SparkEngineConfig.kerberosKeyTabPath match {
            case Some(path) => (s",${path}",
              s"-Dintel.analytics.engine.hadoop.kerberos.keytab-file=${new File(path).getName}")
            case None => ("", "")
          }

          val sparkMaster = Array(s"--master", s"${SparkEngineConfig.sparkMaster}")
          val jobName = Array(s"--name", s"${command.getJobName}")
          val pluginExecutionDriverClass = Array("--class", "com.intel.intelanalytics.engine.spark.command.CommandDriver")

          val pluginDependencyJars = SparkEngineConfig.sparkAppJarsLocal match {
            case true => Array[String]() /* Expect jars to installed locally and available */
            case false => Array("--jars",
              s"${SparkContextFactory.jarPath("interfaces")}," +
                s"${SparkContextFactory.jarPath("launcher")}," +
                s"${getPluginJarPath(pluginJarsList)}")
          }

          val pluginDependencyFiles = Array("--files", s"$tempConfFileName#application.conf$kerbFile",
            "--conf", s"config.resource=application.conf")
          val executionParams = Array(
            "--num-executors", s"${SparkEngineConfig.sparkOnYarnNumExecutors}",
            "--driver-java-options", s"-XX:MaxPermSize=${SparkEngineConfig.sparkDriverMaxPermSize} $kerbOptions")

          val executorClassPathString = "spark.executor.extraClassPath"
          val executorClassPathTuple = SparkEngineConfig.sparkAppJarsLocal match {
            case true => (executorClassPathString,
              s"${SparkContextFactory.jarPath("interfaces")}:${SparkContextFactory.jarPath("launcher")}:" +
              s"${SparkEngineConfig.hiveLib}:${getPluginJarPath(pluginJarsList, ":")}" +
              s"${SparkEngineConfig.sparkConfProperties.get(executorClassPathString).getOrElse("")}")
            case false => (executorClassPathString,
              s"${SparkEngineConfig.hiveLib}:${SparkEngineConfig.sparkConfProperties.get(executorClassPathString).getOrElse("")}")
          }

          val driverClassPathString = "spark.driver.extraClassPath"
          val driverClassPathTuple = (driverClassPathString,
            s"${SparkContextFactory.jarPath("interfaces")}:${SparkContextFactory.jarPath("launcher")}:" +
            s"${pluginExtraClasspath.mkString(":")}:${SparkEngineConfig.hiveLib}:${SparkEngineConfig.hiveConf}:" +
            s"${SparkEngineConfig.sparkConfProperties.get(driverClassPathString).getOrElse("")}")

          val executionConfigs = {
            for {
              (config, value) <- SparkEngineConfig.sparkConfProperties + (executorClassPathTuple, driverClassPathTuple)
            } yield List("--conf", s"$config=$value")
          }.flatMap(identity).toArray

          val verbose = Array("--verbose")
          /* Using engine-core.jar (or deploy.jar) here causes issue due to duplicate copying of the resource.
          So we hack to submit the job as if we are spark-submit shell script */
          val sparkInternalDriverClass = Array("spark-internal")
          val pluginArguments = Array(s"${command.id}")

          /* Prepare input arguments for Spark Submit; Do not change the order */
          val inputArgs = sparkMaster ++
            jobName ++
            pluginExecutionDriverClass ++
            pluginDependencyJars ++
            pluginDependencyFiles ++
            executionParams ++
            executionConfigs ++
            verbose ++
            sparkInternalDriverClass ++
            pluginArguments

          /* Launch Spark Submit */
          info(s"Launching Spark Submit with InputArgs: ${inputArgs.mkString(" ")}")
          val pluginDependencyJarsStr = s"${SparkContextFactory.jarPath("engine-core")}:${pluginExtraClasspath.mkString(":")}"
          val javaArgs = Array("java", "-cp", s"$pluginDependencyJarsStr", "org.apache.spark.deploy.SparkSubmit") ++ inputArgs

          // We were initially invoking SparkSubmit main method directly (i.e. inside our JVM). However, only one
          // ApplicationMaster can exist at a time inside a single JVM. All further calls to SparkSubmit fail to
          // create an instance of ApplicationMaster due to current spark design. We took the approach of invoking
          // SparkSubmit as a standalone process (using engine.jar) for every command to get the parallel
          // execution in yarn-cluster mode.

          val pb = new java.lang.ProcessBuilder(javaArgs: _*)
          val result = pb.inheritIO().start().waitFor()
          info(s"Command ${command.id} completed with exitCode:$result, ${JvmMemory.memory}")
          result
        }
      }
      finally {
        Files.deleteIfExists(FileSystems.getDefault().getPath(s"$tempConfFileName"))
        sys.props -= "SPARK_SUBMIT" /* Removing so that next command executes in a clean environment to begin with */
      }
    }
  }

  private def invokeCommandFunction[A <: Product: TypeTag, R <: Product: TypeTag](command: CommandPlugin[A, R],
                                                                                  arguments: A,
                                                                                  commandContext: CommandContext)(implicit invocation: Invocation): R = {
    val cmd = commandContext.command
    info(s"Invoking command ${cmd.name}")
    command(invocation, arguments)
  }

  private def getInvocation[R <: Product: TypeTag, A <: Product: TypeTag](command: CommandPlugin[A, R],
                                                                          arguments: A,
                                                                          commandContext: CommandContext)(implicit invocation: Invocation): Invocation with Product with Serializable = {

    SimpleInvocation(engine,
      commandStorage = commands,
      commandId = commandContext.command.id,
      arguments = commandContext.command.arguments,
      user = commandContext.user,
      executionContext = commandContext.executionContext,
      resolver = commandContext.resolver,
      eventContext = commandContext.eventContext
    )
  }

  /**
   * Lookup the plugin from the registry
   * @param plugins the registry of plugins
   * @param command the command to lookup a plugin for
   * @tparam A plugin arguments
   * @tparam R plugin return type
   * @return the plugin
   */
  private def expectCommandPlugin[A <: Product, R <: Product](plugins: CommandPluginRegistry, command: Command): CommandPlugin[A, R] = {
    plugins.getCommandDefinition(command.name)
      .getOrElse(throw new NotFoundException("command definition", command.name))
      .asInstanceOf[CommandPlugin[A, R]]
  }

  /**
   * Cancel a command
   * @param commandId command id
   */
  def cancelCommand(commandId: Long, commandPluginRegistry: CommandPluginRegistry): Unit = withMyClassLoader {
    /* This should be killing any yarn jobs running */
    val command = commands.lookup(commandId).getOrElse(throw new Exception(s"Command $commandId does not exist"))
    val commandPlugin = commandPluginRegistry.getCommandDefinition(command.name).get
    if (commandPlugin.isInstanceOf[SparkCommandPlugin[_, _]]) {
      if (!SparkEngineConfig.isSparkOnYarn) {
        /* SparkCommandPlugins which run on Standalone cluster mode */
        SparkCommandPlugin.stop(commandId)
      }
      else {
        HadoopSupport.killYarnJob(command.getJobName)
      }
    }
    else {
      // TODO: Other plugins like Giraph etc which inherit from CommandPlugin, See TRIB-4661
      // TODO: We need to rename all giraph jobs to use command.getJobName as yarn job name instead of hardcoded values like "iat_giraph_als"
      error(s"Cancel is NOT implemented for anything other than SparkCommandPlugins, ${command.name} is NOT a SparkCommandPlugin")
    }
  }
}
