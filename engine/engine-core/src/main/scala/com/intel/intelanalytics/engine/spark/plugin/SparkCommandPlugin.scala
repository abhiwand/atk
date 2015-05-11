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

package com.intel.intelanalytics.engine.spark.plugin

import java.nio.file.{ Paths, Files }
import java.nio.charset.StandardCharsets
import scala.collection.JavaConversions._

import com.intel.intelanalytics.component.Archive
import com.intel.intelanalytics.engine.plugin.{ CommandInvocation, CommandPlugin, Invocation }
import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
import com.typesafe.config.{ ConfigList, ConfigValue }
import org.apache.spark.SparkContext
import org.apache.spark.engine.{ ProgressPrinter, SparkProgressListener }
import com.intel.event.EventLogging
import com.intel.intelanalytics.engine.spark.{ SparkEngineConfig, SparkEngine }

/**
 * Base trait for command plugins that need direct access to a SparkContext
 *
 * @tparam Argument the argument type for the command
 * @tparam Return the return type for the command
 */
trait SparkCommandPlugin[Argument <: Product, Return <: Product]
    extends CommandPlugin[Argument, Return] {

  override def engine(implicit invocation: Invocation): SparkEngine = invocation.asInstanceOf[SparkInvocation].engine

  /**
   * Name of the custom kryoclass this plugin needs.
   * kryoRegistrator = None means use JavaSerializer
   */
  def kryoRegistrator: Option[String] = Some("com.intel.intelanalytics.engine.spark.EngineKryoRegistrator")

  def sc(implicit invocation: Invocation): SparkContext = invocation.asInstanceOf[SparkInvocation].sparkContext

  /**
   * Can be overridden by subclasses to provide a more specialized Invocation. Called before
   * calling the execute method.
   */
  override protected def customizeInvocation(invocation: Invocation, arguments: Argument): Invocation = {
    require(invocation.isInstanceOf[CommandInvocation], "Cannot invoke a CommandPlugin without a CommandInvocation")
    val commandInvocation = invocation.asInstanceOf[CommandInvocation]
    val sparkEngine: SparkEngine = commandInvocation.engine.asInstanceOf[SparkEngine]
    val sparkInvocation = new SparkInvocation(sparkEngine,
      invocation.user,
      commandInvocation.commandId,
      invocation.executionContext,
      commandInvocation.arguments,
      //TODO: Hide context factory behind a property on SparkEngine?
      null,
      commandInvocation.commandStorage,
      invocation.resolver,
      invocation.eventContext)
    sparkInvocation.copy(sparkContext = createSparkContextForCommand(arguments, sparkEngine.sparkContextFactory)(sparkInvocation))
  }

  def createSparkContextForCommand(arguments: Argument, sparkContextFactory: SparkContextFactory)(implicit invocation: SparkInvocation): SparkContext = {
    val cmd = invocation.commandStorage.lookup(invocation.commandId)
      .getOrElse(throw new IllegalArgumentException(s"Command ${invocation.commandId} does not exist"))
    val commandId = cmd.id
    val commandName = cmd.name
    val context: SparkContext = sparkContextFactory.context(s"(id:$commandId,name:$commandName)", kryoRegistrator)
    if (!SparkEngineConfig.reuseSparkContext) {
      try {
        val listener = new SparkProgressListener(SparkProgressListener.progressUpdater, cmd, numberOfJobs(arguments)) // Pass number of Jobs here
        val progressPrinter = new ProgressPrinter(listener)
        context.addSparkListener(listener)
        context.addSparkListener(progressPrinter)
      }
      catch {
        // exception only shows up here due to dev error, but it is hard to debug without this logging
        case e: Exception => error("could not create progress listeners", exception = e)
      }
    }
    SparkCommandPlugin.commandIdContextMapping += (commandId -> context)
    context
  }

  override def cleanup(invocation: Invocation) = {
    val sparkInvocation = invocation.asInstanceOf[SparkInvocation]
    SparkCommandPlugin.stop(sparkInvocation.commandId)
  }

  /**
   * Serializes the plugin configuration to a path given the archive name the plugin belongs to.
   * Returns the jars and extra classpath needed to run this plugin by looking into its configuration and
   * and archive's parent's configuration
   * @param archiveName name of archive to serialize
   * @param path path to serialize
   * @return tuple of 2 lists corresponding to jars and extraClassPath needed to run this archive externally
   */
  def serializePluginConfiguration(archiveName: String, path: String): (List[String], List[String]) = withMyClassLoader {

    info(s"Serializing Plugin Configuration for archive $archiveName to path $path")
    val currentConfig = try {
      Archive.getAugmentedConfig(archiveName, Thread.currentThread().getContextClassLoader).entrySet()
    }
    catch {
      case _: Throwable => SparkEngineConfig.config.entrySet()
    }

    val allEntries = {
      for {
        i <- currentConfig
      } yield i.getKey -> i.getValue
    }.toMap

    def getParentForArchive(archive: String, configMap: Map[String, ConfigValue]): Option[String] = {
      val parent = for {
        (k, v) <- configMap
        if (k.contains(s"$archive.parent"))
      } yield v
      if (parent.iterator.hasNext)
        Some(parent.head.render.replace("\"", ""))
      else None
    }

    /* Get the plugin hirearchy by adding the plugin and its parents */
    val jars = scala.collection.mutable.MutableList[String](archiveName)
    var nextArchiveName = Option[String](archiveName)
    while (nextArchiveName.isDefined) {
      nextArchiveName = getParentForArchive(nextArchiveName.get, allEntries)
      if (nextArchiveName.isDefined)
        jars += nextArchiveName.get
    }

    /* Get extraClassPath for plugin including its parents' extra classpath */
    val extraClassPath = scala.collection.mutable.MutableList[String]()
    for {
      i <- jars
      (configKey, configValue) <- allEntries
      if (configKey.contains(s"$i.extra-classpath"))
    } extraClassPath ++= configValue.asInstanceOf[ConfigList].unwrapped().map(_.toString)

    /* Convert all configs to strings; override the archives entry with current plugin's archive name */
    /* We always need engine as in Engine.scala we add plugins via
    registerCommand api which need engine to be there in system archives list always */
    val configEntriesInString = for {
      (configKey, configValue) <- allEntries
    } yield {
      if (configKey == "intel.analytics.engine.plugin.command.archives")
        s"""$configKey=[\"$archiveName\"]"""
      else
        s"$configKey=${configValue.render}"
    }

    Files.write(Paths.get(path), configEntriesInString.mkString("\n").getBytes(StandardCharsets.UTF_8))
    (jars.toList, extraClassPath.distinct.toList)
  }

  def getArchiveName() = withMyClassLoader {
    Archive.system.lookupArchiveNameByLoader(Thread.currentThread().getContextClassLoader)
  }
}

object SparkCommandPlugin extends EventLogging {

  private var commandIdContextMapping = Map.empty[Long, SparkContext]

  def stop(commandId: Long) = {
    commandIdContextMapping.get(commandId).foreach { case (context) => stopContextIfNeeded(context) }
    commandIdContextMapping -= commandId
  }

  private def stopContextIfNeeded(sc: SparkContext): Unit = {
    if (SparkEngineConfig.reuseSparkContext) {
      info("not stopping local SparkContext so that it can be re-used")
    }
    else {
      sc.stop()
    }
  }
}