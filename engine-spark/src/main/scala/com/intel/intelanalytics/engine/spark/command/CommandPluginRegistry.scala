package com.intel.intelanalytics.engine.spark.command

import com.intel.intelanalytics.domain.command.{ CommandDefinition, CommandDoc }
import com.intel.intelanalytics.engine.plugin.{ CommandPlugin, Invocation }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.shared.JsonSchemaExtractor
import spray.json.JsonFormat

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
   * @return the same command that was passed, for convenience
   */
  def registerCommand[A <: Product, R <: Product](command: SparkCommandPlugin[A, R]): SparkCommandPlugin[A, R] = {
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
                                                                                                        numberOfJobs: Int = 1,
                                                                                                        doc: Option[CommandDoc] = None): SparkCommandPlugin[A, R] = {
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
  def registerCommand[A <: Product: JsonFormat: ClassManifest, R <: Product: JsonFormat: ClassManifest](name: String,
                                                                                                        function: (A, UserPrincipal, SparkInvocation) => R,
                                                                                                        numberOfJobsFunc: (A) => Int,
                                                                                                        doc: Option[CommandDoc]): SparkCommandPlugin[A, R] = {
    // Note: providing a default =None to the doc parameter causes a strange compiler error where it can't
    // distinguish this method from the one which takes a plain Int for the numberOfJobs. Since the
    // numberOfJobsFunc variation is much less frequently used, we'll make doc a required parameter
    registerCommand(new SparkFunctionCommand(name, function.asInstanceOf[(A, UserPrincipal, Invocation) => R], numberOfJobsFunc, doc))
  }

  /**
   * Returns all the command definitions registered with this command executor.
   */
  def getCommandDefinitions(): Iterable[CommandDefinition] =
    commandPlugins.values.map(p => {
      val (argSchema, resSchema) = getArgumentAndResultSchemas(p)
      CommandDefinition(p.name, argSchema, resSchema, p.doc)
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
