package com.intel.intelanalytics.algorithm.util

import com.intel.intelanalytics.domain.graph.Graph
import com.intel.intelanalytics.engine.spark.graph.{ GraphBuilderConfigFactory, GraphName }
import com.typesafe.config.{ ConfigValue, ConfigObject, Config }
import org.apache.hadoop.conf.Configuration
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

object GiraphConfigurationUtil {

  /**
   * Set a value in the hadoop configuration, if the argument is not None.
   * @param hadoopConfiguration the configuration to update
   * @param hadoopKey the key name to set
   * @param arg the value to use, if it is defined
   */
  def set(hadoopConfiguration: Configuration, hadoopKey: String, arg: Option[Any]) = arg.foreach { value =>
    hadoopConfiguration.set(hadoopKey, value.toString)
  }

  /**
   * Create new Hadoop Configuration object, preloaded with the properties
   * specified in the given Config object under the provided key.
   * @param config the Config object from which to copy properties to the Hadoop Configuration
   * @param key the starting point in the Config object. Defaults to "hadoop".
   * @return a populated Hadoop Configuration object.
   */
  def newHadoopConfigurationFrom(config: Config, key: String = "hadoop"): org.apache.hadoop.conf.Configuration = {
    require(config != null, "Config cannot be null")
    require(key != null, "Key cannot be null")
    val hConf = new Configuration()
    val properties = flattenConfig(config.getConfig(key))
    properties.foreach { kv =>
      println(s"Setting ${kv._1} to ${kv._2}")
      hConf.set(kv._1, kv._2)
    }
    hConf
  }

  /**
   * Update the Hadoop configuration object with the Titan configuration
   *
   * @param hConf the Hadoop configuration object to update
   * @param config the Config object from which to copy Titan properties to the Hadoop Configuration
   * @param graph the graph object containing the Titan graph name
   */
  def initializeTitanConfig(hConf: Configuration, config: Config, graph: Graph): Unit = {

    val titanConfig = GraphBuilderConfigFactory.getTitanConfiguration(graph.name)

    titanConfig.getKeys.foreach {
      case (titanKey: String) =>
        val titanGiraphKey = "giraph.titan.input." + titanKey
        set(hConf, titanGiraphKey, Option[Any](titanConfig.getProperty(titanKey)))
    }
  }

  /**
   * Flatten a nested Config structure down to a simple dictionary that maps complex keys to
   * a string value, similar to java.util.Properties.
   *
   * @param config the config to flatten
   * @return a map of property names to values
   */
  private def flattenConfig(config: Config, prefix: String = ""): Map[String, String] = {
    val result = config.root.asScala.foldLeft(Map.empty[String, String]) {
      (map, kv) =>
        kv._2 match {
          case co: ConfigObject =>
            val nested = flattenConfig(co.toConfig, prefix = prefix + kv._1 + ".")
            map ++ nested
          case value: ConfigValue =>
            map + (prefix + kv._1 -> value.unwrapped().toString)
        }
    }
    result
  }

}