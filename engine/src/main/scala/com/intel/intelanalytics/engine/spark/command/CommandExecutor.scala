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

import java.io.File
import java.nio.file.{ FileSystems, Files }

import com.intel.intelanalytics.engine.spark.hadoop.HadoopSupport

import sys.process._
import scala.collection.JavaConversions._

import com.intel.intelanalytics.component.ClassLoaderAware
import com.intel.intelanalytics.domain._
import com.intel.intelanalytics.engine._
import com.intel.intelanalytics.engine.plugin.{ Transformation, Invocation, CommandPlugin }
import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
import com.intel.intelanalytics.engine.spark.util.KerberosAuthenticator
import com.intel.intelanalytics.engine.spark.{ SparkEngineConfig, SparkEngine }
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

case class CommandContext(
    command: Command,
    action: Boolean,
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
  private def createSuspendedReferences[Arguments <: Product: TypeTag, Return <: Product: TypeTag](command: Command,
                                                                                                   plugin: CommandPlugin[Arguments, Return],
                                                                                                   arguments: Arguments)(implicit invocation: Invocation): Return = {

    val types = Reflection.getUriReferenceTypes[Return]()
    val references = types.map {
      case (name, signature) =>
        val management: EntityManager[_] = EntityTypeRegistry.entityManagerForType(signature.typeSymbol.asType.toType).get
        (name, management.create(CreateEntityArgs()))
    }.toMap
    val ctorMap = Reflection.getConstructorMap[Return]()
    ctorMap(references)
  }

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
        action = isAction(plugin),
        executionContext,
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
        action = isAction(plugin),
        executionContext,
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
    info(s"command id:${commandContext.command.id}, name:${commandContext.command.name}, args:${commandContext.command.compactArgs}")
    val plugin = expectCommandPlugin[A, R](commandContext.plugins, commandContext.command)
    val arguments = plugin.parseArguments(commandContext.command.arguments.get)
    implicit val commandInvocation = getInvocation(plugin, arguments, commandContext)
    debug(s"System Properties are: ${sys.props.keys.mkString(",")}")

    if (plugin.isInstanceOf[SparkCommandPlugin[A, R]] && !sys.props.contains("SPARK_SUBMIT") && SparkEngineConfig.isSparkOnYarnClusterMode) {
      val archiveName = commandContext.plugins.getArchiveNameFromPlugin(plugin.name)
      executeCommandOnYarn(commandContext.command, plugin, archiveName)
      // Reload the command as the error/result etc fields should have been updated in metastore upon yarn execution
      val updatedCommand = commands.lookup(commandContext.command.id).get
      if (updatedCommand.error.isDefined)
        throw new Exception(s"Error executing ${plugin.name}: ${updatedCommand.error.get.message}")
      updatedCommand.result.getOrElse(throw new Exception("Error submitting command to yarn-cluster"))
    }
    else {
      val returnValue = if (commandContext.action) {
        val res = if (firstExecution) {
          this.runDependencies(arguments, commandContext)(plugin.argumentTag, commandInvocation)
        }
        else commandContext.resolver
        val result = invokeCommandFunction(plugin, arguments, commandContext.copy(resolver = res))
        result
      }
      else {
        createSuspendedReferences(commandContext.command, plugin, arguments)
      }
      if (firstExecution) {
        commandContext.clean()
      }
      plugin.serializeReturn(returnValue)
    }
  }

  private def executeCommandOnYarn[A <: Product: TypeTag, R <: Product: TypeTag](command: Command, plugin: CommandPlugin[A, R], archiveName: Option[String])(implicit invocation: Invocation): Unit = {
    withContext("executeCommandOnYarn") {

      val tempConfFileName = s"/tmp/application_${command.id}.conf"
      val sparkCommandPlugin = plugin.asInstanceOf[SparkCommandPlugin[A, R]]
      val pluginArchiveName = archiveName.getOrElse(sparkCommandPlugin.getArchiveName())

      /* Serialize current config for the plugin so as to pass to Spark Submit */
      val (pluginJarsList, pluginExtraClasspath) = sparkCommandPlugin.serializePluginConfiguration(pluginArchiveName, tempConfFileName)

      val pluginJarPath = pluginJarsList.filter(_ != "engine")
        .map(j => SparkContextFactory.jarPath(j)).mkString(",")

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
          val pluginDependencyJars = Array("--jars", s"${SparkContextFactory.jarPath("engine-interfaces")},${SparkContextFactory.jarPath("launcher")},$pluginJarPath")
          val pluginDependencyFiles = Array("--files", s"$tempConfFileName#application.conf$kerbFile", "--conf", s"config.resource=application.conf")
          val executionParams = Array(
            "--num-executors", s"${SparkEngineConfig.sparkOnYarnNumExecutors}",
            "--driver-java-options", s"-XX:MaxPermSize=${SparkEngineConfig.sparkDriverMaxPermSize} $kerbOptions")

          val executionConfigs = {
            for {
              (config, value) <- SparkEngineConfig.sparkConfProperties
            } yield List("--conf", s"$config=$value")
          }.flatMap(identity).toArray

          // TODO: Once we get rid of setting SPARK_CLASSPATH in cdh, we should be setting only the driver-class-path
          val driver_classpath = SparkEngineConfig.sparkMaster match {
            case "yarn-cluster" | "yarn-client" => Array(s"--driver-class-path", s"${pluginExtraClasspath.mkString(":")}")
            case _ => Array[String]()
          }
          val verbose = Array("--verbose")
          /* Using engine.jar here causes issue due to duplicate copying of the resource. So we hack to submit the job as if we are spark-submit shell script */
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
            driver_classpath ++
            verbose ++
            sparkInternalDriverClass ++
            pluginArguments

          /* Launch Spark Submit */
          info(s"Launching Spark Submit with InputArgs: ${inputArgs.mkString(" ")}")
          val pluginDependencyJarsStr = s"${SparkContextFactory.jarPath("engine")}:${pluginExtraClasspath.mkString(":")}"
          val javaArgs = Array("java", "-cp", s"$pluginDependencyJarsStr", "org.apache.spark.deploy.SparkSubmit") ++ inputArgs

          // We were initially invoking SparkSubmit main method directly (i.e. inside our JVM). However, only one
          // ApplicationMaster can exist at a time inside a single JVM. All further calls to SparkSubmit fail to
          // create an instance of ApplicationMaster due to current spark design. We took the approach of invoking
          // SparkSubmit as a standalone process (using engine.jar) for every command to get the parallel
          // execution in yarn-cluster mode.

          val pb = new java.lang.ProcessBuilder(javaArgs: _*)
          val result = (pb.inheritIO() !)
          info(s"Command ${command.id} complete with result: $result")
        }
      }
      finally {
        Files.deleteIfExists(FileSystems.getDefault().getPath(s"$tempConfFileName"))
        sys.props -= "SPARK_SUBMIT" /* Removing so that next command executes in a clean environment to begin with */
      }
    }
  }

  /**
   * For the arguments and command given, find all the dependencies (UriReferences) in the arguments,
   * and if they aren't already materialized, run the commands that generate data for them. If they
   * in turn have dependencies, process those first, recursively.
   *
   * @param arguments the arguments to the command
   * @param commandContext the command that's being run
   * @param invocation the invocation in which we're running
   * @tparam A the arguments type
   * @return a new ReferenceResolver that will resolve references by returning the in-memory data
   *         when available, rather than going back to the EntityManager.
   */
  private def runDependencies[A <: Product: TypeTag](arguments: A,
                                                     commandContext: CommandContext)(implicit invocation: Invocation): ReferenceResolver = {
    implicit val eventContext = invocation.eventContext
    implicit val user = invocation.user
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
            createExecution(commandContext.copy(command = c, action = true, resolver = resolver), firstExecution = false)
          })
          //Next, examine the results and add uri references to the resolver,
          //if they are HasData instances. This is what allows RDDs or other data
          //structures to chain from one operation to the next without serializing
          //at each step
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
    !plugin.isInstanceOf[Transformation[_, _]] || {
      implicit val returnTag = plugin.returnTag
      val dataMembers: Seq[(String, Type)] = Reflection.getVals[R]()
      val referenceTypes: Seq[(String, Type)] = Reflection.getUriReferenceTypes[R]()
      dataMembers.length != referenceTypes.length
    }
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
      if (!SparkEngineConfig.isSparkOnYarnClusterMode) {
        /* SparkCommandPlugins which run on Standalone cluster mode */
        SparkCommandPlugin.stop(commandId)
      }
      else {
        HadoopSupport.killYarnJob(command.getJobName)
      }
    }
    else {
      /* Other plugins like Giraph etc which inherit from CommandPlugin, See TRIB-4661 */
      // We need to rename all giraph jobs to use command.getJobName as yarn job name instead of hardcoded values like "iat_giraph_als"
    }
  }
}
