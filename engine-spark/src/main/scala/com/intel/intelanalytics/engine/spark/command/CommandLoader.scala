package com.intel.intelanalytics.engine.spark.command

import com.intel.intelanalytics.engine.plugin.CommandPlugin
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.component.Boot

/**
 * Load command plugin
 */
class CommandLoader {
  /**
   * Load plugins from the config
   * @return mapping between name and plugin
   */
  def loadFromConfig(): Map[String, CommandPlugin[_, _]] = SparkEngineConfig.archives.flatMap {
    archive =>
      Boot.getArchive(archive)
        .getAll[CommandPlugin[_ <: Product, _ <: Product]]("command")
        .map(p => (p.name, p))
  }.toMap
}
