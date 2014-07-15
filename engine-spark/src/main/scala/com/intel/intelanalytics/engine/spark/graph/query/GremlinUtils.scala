package com.intel.intelanalytics.engine.spark.graph.query

import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.typesafe.config.Config
import scala.collection.JavaConverters._

object GremlinUtils {

  /**
   * Default settings for Gremlin queries.
   *
   * @param config Default configuration
   * @param path Paths are dot-separated expressions such as foo.bar.baz
   * @return Titan configuration with default settings specified in the path expression.
   */
  def getTitanConfiguration(config: Config, path: String): SerializableBaseConfiguration = {
    val titanConfiguration = new SerializableBaseConfiguration
    val titanLoadConfig = config.getConfig(path)
    for (entry <- titanLoadConfig.entrySet().asScala) {
      titanConfiguration.addProperty(entry.getKey, titanLoadConfig.getString(entry.getKey))
    }
    titanConfiguration
  }


}
