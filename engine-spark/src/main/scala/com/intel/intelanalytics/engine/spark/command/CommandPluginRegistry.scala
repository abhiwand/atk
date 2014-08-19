package com.intel.intelanalytics.engine.spark.command

import com.intel.intelanalytics.engine.plugin.{ Invocation, FunctionCommand, CommandPlugin }
import spray.json.JsonFormat
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.engine.spark.plugin.SparkInvocation
import com.intel.intelanalytics.domain.command.CommandDefinition
import com.intel.intelanalytics.shared.JsonSchemaExtractor

/**
 * Register and store command plugin
 */
class CommandPluginRegistry(loader: CommandLoader) {

  private var commandPlugins: Map[String, CommandPlugin[_, _]] = loader.loadFromConfig()

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
   * @tparam A the argument type for the command
   * @tparam R the return type for the command
   * @return the same command that was passed, for convenience
   */
  def registerCommand[A <: Product, R <: Product](command: CommandPlugin[A, R]): CommandPlugin[A, R] = {
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
   * For where numberOfJobs is constant for a command.
   *
   * @param name the name of the command
   * @param function the function to be called when running the command
   * @param numberOfJobs the number of jobs that this command will create (constant)
   * @tparam A the argument type of the command
   * @tparam R the return type of the command
   * @return the CommandPlugin instance created during the registration process.
   */
  def registerCommand[A <: Product: JsonFormat: ClassManifest, R <: Product: JsonFormat: ClassManifest](name: String,
                                                                                                        function: (A, UserPrincipal, SparkInvocation) => R,
                                                                                                        numberOfJobs: Int = 1): CommandPlugin[A, R] = {
    registerCommand(name, function, (A) => numberOfJobs)
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
  def registerCommand[A <: Product: JsonFormat: ClassManifest, R <: Product: JsonFormat: ClassManifest](name: String,
                                                                                                        function: (A, UserPrincipal, SparkInvocation) => R,
                                                                                                        numberOfJobsFunc: (A) => Int): CommandPlugin[A, R] = {
    registerCommand(FunctionCommand(name, function.asInstanceOf[(A, UserPrincipal, Invocation) => R], numberOfJobsFunc))
  }

  /**
   * Returns all the command definitions registered with this command executor.
   */
  def getCommandDefinitions(): Iterable[CommandDefinition] =
    commandPlugins.values.map(p => {
      val (argSchema, resSchema) = getArgumentAndResultSchemas(p)
      CommandDefinition(p.name, argSchema, resSchema)
    })

  private def getArgumentAndResultSchemas(plugin: CommandPlugin[_, _]) = {
    val arg = plugin.argumentManifest
    val ret = plugin.returnManifest
    (JsonSchemaExtractor.getProductSchema(arg), JsonSchemaExtractor.getProductSchema(ret))
  }

  //  private var commandPlugins: Map[String, CommandPlugin[_, _]] = pluginRegistry.commandPlugins

  def getCommandDefinition(name: String): Option[CommandPlugin[_, _]] = {
    commandPlugins.get(name)
  }
}
