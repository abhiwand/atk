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

import com.intel.intelanalytics.component.Archive
import com.intel.intelanalytics.domain.command.{ CommandDefinition, CommandDoc }
import com.intel.intelanalytics.engine.PluginDocAnnotation
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, CommandPlugin, Invocation }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.schema.ObjectSchema
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
   * Returns all the command definitions registered with this command executor.
   *
   * NOTE: this is a val because the underlying operations are not thread-safe -- Todd 3/10/2015
   */
  lazy val commandDefinitions: Iterable[CommandDefinition] =
    commandPlugins.values.map(p => {
      // It seems that the underlying operations are not thread-safe -- Todd 3/10/2015
      val argSchema = getArgumentsSchema(p)
      val retSchema = getReturnSchema(p)
      val doc = getCommandDoc(p)
      CommandDefinition(p.name, argSchema, retSchema, doc, p.apiMaturityTag)
    })

  //  private var commandPlugins: Map[String, CommandPlugin[_, _]] = pluginRegistry.commandPlugins

  def getCommandDefinition(name: String): Option[CommandPlugin[_, _]] = {
    commandPlugins.get(name)
  }

  // Get archive name for the plugin. If it does not exist in the map, check if it is in commandPlugins as a valid plugin
  def getArchiveNameFromPlugin(name: String): Option[String] = {
    pluginsToArchiveMap.get(name)
  }

  private def getArgumentsSchema(p: CommandPlugin[_, _]): ObjectSchema = {
    JsonSchemaExtractor.getProductSchema(p.argumentManifest, includeDefaultValues = true)
  }

  private def getReturnSchema(p: CommandPlugin[_, _]): ObjectSchema = {
    val schema = JsonSchemaExtractor.getProductSchema(p.returnManifest, includeDefaultValues = false)
    // if plugin annotation has a returns description, add it to the schema
    JsonSchemaExtractor.getPluginDocAnnotation(p.thisManifest) match {
      case None => schema
      case Some(pluginDoc) => schema.copy(description = pluginDoc.getReturnsDescription)
    }
  }

  private def getCommandDoc(p: CommandPlugin[_, _]): Option[CommandDoc] = {
    JsonSchemaExtractor.getPluginDocAnnotation(p.thisManifest) match {
      case Some(pluginDoc) => Some(CommandDoc(pluginDoc.oneLine, Some(pluginDoc.extended)))
      case None => p.doc
    }
  }
}

case class CommandPluginRegistryMaps(var commandPlugins: Map[String, CommandPlugin[_, _]], var pluginsToArchiveMap: Map[String, String])
